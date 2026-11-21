//! Shopping Sessions: clean the click-stream CSVs in DATA_DIR, join each
//! person's IDs, build sessions, credit and assist campaigns, and write
//! user_daily and campaign_daily as Parquet to OUTPUT_DIR. rules.md is the spec.

mod csv;
mod out;
mod ts;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};

use out::{CampaignRow, UserRow};
use ts::DAY;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const WEEK: i64 = 7 * DAY;
const SESSION_GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const MAX_PRICE: u64 = 10_000_000;
const CHUNK: usize = 4 << 20;
const NONE: u32 = u32::MAX;

fn log(start: Instant, what: &str) {
    eprintln!("[{:>7.3}s] {what}", start.elapsed().as_secs_f64());
}

fn main() {
    let start = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .ok();
    run(&data, &output, start);
    log(start, "done");
}

// ---------------------------------------------------------------- inputs

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file()
                    && path.file_name().and_then(|n| n.to_str()).is_some_and(|name| {
                        name.ends_with(".csv") && !name.starts_with('.')
                    })
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

fn map(path: &Path) -> Option<Mmap> {
    let file = File::open(path).ok()?;
    if file.metadata().ok()?.len() == 0 {
        return None;
    }
    let map = unsafe { Mmap::map(&file) }.ok()?;
    #[cfg(unix)]
    {
        let _ = map.advise(memmap2::Advice::Sequential);
    }
    Some(map)
}

fn map_all(files: &[PathBuf]) -> Vec<Mmap> {
    files.par_iter().filter_map(|path| map(path)).collect()
}

/// Splits every file, minus its header line, into chunks that start and end
/// at line boundaries.
fn chunks(maps: &[Mmap]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    for map in maps {
        let data: &[u8] = map;
        let Some(header_end) = memchr::memchr(b'\n', data) else {
            continue;
        };
        let mut start = header_end + 1;
        while start < data.len() {
            let mut end = (start + CHUNK).min(data.len());
            if end < data.len() {
                end = match memchr::memchr(b'\n', &data[end..]) {
                    Some(at) => end + at + 1,
                    None => data.len(),
                };
            }
            out.push(&data[start..end]);
            start = end;
        }
    }
    out
}

fn read_lookup(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_default()
}

/// The rows of a clean two-column lookup file, without its header.
fn lookup_rows(data: &[u8]) -> Vec<[&[u8]; 2]> {
    let mut rows = Vec::new();
    let Some(header_end) = memchr::memchr(b'\n', data) else {
        return rows;
    };
    csv::for_lines(&data[header_end + 1..], |line| {
        if let Some(fields) = csv::split::<2>(line) {
            rows.push(fields);
        }
    });
    rows
}

fn manifest_time(text: &str, key: &str) -> i64 {
    let at = text.find(key).expect("manifest key") + key.len();
    let rest = &text[at..];
    let open = rest.find('"').expect("manifest value") + 1;
    let rest = &rest[open..];
    let close = rest.find('"').expect("manifest value");
    ts::parse(rest[..close].as_bytes()).expect("manifest timestamp")
}

// ---------------------------------------------------------------- rows

#[derive(Clone, Copy)]
struct Event<'a> {
    event_id: &'a [u8],
    user_id: &'a [u8],
    ts: i64,
    ingest: i64,
    /// Price for a valid purchase, 0 for view and cart.
    price: u32,
    category: u16,
    valid: bool,
}

#[derive(Clone, Copy)]
struct Click<'a> {
    user_id: &'a [u8],
    click_id: &'a [u8],
    ts: i64,
    campaign: u16,
}

#[inline]
fn parse_price(field: &[u8]) -> Option<u32> {
    if field.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    for &b in field {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u64::from(b - b'0');
        if value > MAX_PRICE {
            return None;
        }
    }
    (value >= 1).then_some(value as u32)
}

struct Context<'a> {
    window_start: i64,
    window_end: i64,
    products: FxHashMap<&'a [u8], u16>,
    unknown_category: u16,
    campaigns: FxHashMap<&'a [u8], u16>,
}

fn parse_event<'a>(line: &'a [u8], context: &Context) -> Option<Event<'a>> {
    let [event_id, user_id, ts_field, ingest_field, kind, product, price] = csv::split::<7>(line)?;
    if event_id.is_empty() {
        return None;
    }
    let ingest = ts::parse(ingest_field);
    let ts = ts::parse(ts_field);
    let mut valid = !user_id.is_empty() && !product.is_empty() && ingest.is_some();
    let ts = match ts {
        Some(t) if t >= context.window_start && t < context.window_end => t,
        _ => {
            valid = false;
            0
        }
    };
    let ingest = ingest.unwrap_or(i64::MAX);
    let mut price_cents = 0;
    match kind {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(price) {
            Some(p) => price_cents = p,
            None => valid = false,
        },
        _ => valid = false,
    }
    if valid && ingest - ts > LATE {
        valid = false;
    }
    let category = if valid {
        context
            .products
            .get(product)
            .copied()
            .unwrap_or(context.unknown_category)
    } else {
        0
    };
    Some(Event {
        event_id,
        user_id,
        ts,
        ingest,
        price: price_cents,
        category,
        valid,
    })
}

fn parse_click<'a>(line: &'a [u8], context: &Context) -> Option<Click<'a>> {
    let [click_id, user_id, campaign, ts_field] = csv::split::<4>(line)?;
    if click_id.is_empty() || user_id.is_empty() {
        return None;
    }
    let campaign = *context.campaigns.get(campaign)?;
    let ts = ts::parse(ts_field)?;
    if ts < context.window_start - WEEK || ts >= context.window_end {
        return None;
    }
    Some(Click {
        user_id,
        click_id,
        ts,
        campaign,
    })
}

// ---------------------------------------------------------------- IDs

struct Ids<'a> {
    index: FxHashMap<&'a [u8], u32>,
    names: Vec<&'a [u8]>,
}

impl<'a> Ids<'a> {
    fn new() -> Self {
        Self {
            index: FxHashMap::with_capacity_and_hasher(1 << 20, FxBuildHasher),
            names: Vec::with_capacity(1 << 20),
        }
    }

    #[inline]
    fn intern(&mut self, id: &'a [u8]) -> u32 {
        let next = self.names.len() as u32;
        *self.index.entry(id).or_insert_with(|| {
            self.names.push(id);
            next
        })
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let up = parent[parent[x as usize] as usize];
        parent[x as usize] = up;
        x = up;
    }
    x
}

// ---------------------------------------------------------------- pipeline

#[derive(Clone, Copy)]
struct Live {
    person: u32,
    ts: i64,
    price: u32,
    category: u16,
}

#[derive(Clone, Copy)]
struct PersonClick<'a> {
    person: u32,
    ts: i64,
    click_id: &'a [u8],
    campaign: u16,
}

fn run(data: &Path, output: &Path, start: Instant) {
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let window_start = manifest_time(&manifest, "\"window_start\"");
    let window_end = manifest_time(&manifest, "\"window_end\"");

    let products_raw = read_lookup(&data.join("products.csv"));
    let campaigns_raw = read_lookup(&data.join("campaigns.csv"));
    let mut category_names: Vec<&[u8]> = Vec::new();
    let mut category_index: FxHashMap<&[u8], u16> = FxHashMap::default();
    let mut products: FxHashMap<&[u8], u16> = FxHashMap::default();
    for [product, category] in lookup_rows(&products_raw) {
        let next = category_names.len() as u16;
        let index = *category_index.entry(category).or_insert_with(|| {
            category_names.push(category);
            next
        });
        products.insert(product, index);
    }
    let unknown_category = category_names.len() as u16;
    category_names.push(b"unknown");
    let mut campaign_ids: Vec<&[u8]> = Vec::new();
    let mut campaign_channels: Vec<&[u8]> = Vec::new();
    let mut campaigns: FxHashMap<&[u8], u16> = FxHashMap::default();
    for [campaign, channel] in lookup_rows(&campaigns_raw) {
        campaigns.insert(campaign, campaign_ids.len() as u16);
        campaign_ids.push(campaign);
        campaign_channels.push(channel);
    }
    let organic = campaign_ids.len() as u16;
    let context = Context {
        window_start,
        window_end,
        products,
        unknown_category,
        campaigns,
    };

    let event_maps = map_all(&csv_files(&data.join("events")));
    let click_maps = map_all(&csv_files(&data.join("clicks")));
    let link_maps = map_all(&csv_files(&data.join("links")));
    log(start, "mapped");

    // Parse.
    let events: Vec<Event> = chunks(&event_maps)
        .par_iter()
        .map(|chunk| {
            let mut rows = Vec::with_capacity(chunk.len() / 80);
            csv::for_lines(chunk, |line| {
                if let Some(event) = parse_event(line, &context) {
                    rows.push(event);
                }
            });
            rows
        })
        .flatten()
        .collect();
    let clicks: Vec<Click> = chunks(&click_maps)
        .par_iter()
        .map(|chunk| {
            let mut rows = Vec::with_capacity(chunk.len() / 50);
            csv::for_lines(chunk, |line| {
                if let Some(click) = parse_click(line, &context) {
                    rows.push(click);
                }
            });
            rows
        })
        .flatten()
        .collect();
    let links: Vec<[&[u8]; 2]> = chunks(&link_maps)
        .par_iter()
        .map(|chunk| {
            let mut rows = Vec::new();
            csv::for_lines(chunk, |line| {
                if let Some([a, b]) = csv::split::<2>(line)
                    && !a.is_empty()
                    && !b.is_empty()
                {
                    rows.push([a, b]);
                }
            });
            rows
        })
        .flatten()
        .collect();
    log(
        start,
        &format!(
            "parsed {} events, {} clicks, {} links",
            events.len(),
            clicks.len(),
            links.len()
        ),
    );

    // Dedup events: keep the copy with the smallest ingest_ts, then drop it
    // if that copy is invalid.
    let mut best: FxHashMap<&[u8], u32> =
        FxHashMap::with_capacity_and_hasher(events.len(), FxBuildHasher);
    for (at, event) in events.iter().enumerate() {
        best.entry(event.event_id)
            .and_modify(|kept| {
                if event.ingest < events[*kept as usize].ingest {
                    *kept = at as u32;
                }
            })
            .or_insert(at as u32);
    }
    let kept: Vec<Event> = best
        .values()
        .map(|&at| events[at as usize])
        .filter(|event| event.valid)
        .collect();
    drop(best);
    log(start, &format!("dedup: {} live events", kept.len()));

    // People.
    let mut ids = Ids::new();
    let link_pairs: Vec<(u32, u32)> = links
        .iter()
        .map(|[a, b]| (ids.intern(a), ids.intern(b)))
        .collect();
    let event_people: Vec<u32> = kept.iter().map(|e| ids.intern(e.user_id)).collect();
    let click_people: Vec<u32> = clicks.iter().map(|c| ids.intern(c.user_id)).collect();
    let count = ids.names.len();
    let mut parent: Vec<u32> = (0..count as u32).collect();
    let mut smallest: Vec<u32> = (0..count as u32).collect();
    for &(a, b) in &link_pairs {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[rb as usize] = ra;
            let (sa, sb) = (smallest[ra as usize], smallest[rb as usize]);
            if ids.names[sb as usize] < ids.names[sa as usize] {
                smallest[ra as usize] = sb;
            }
        }
    }
    let person: Vec<u32> = (0..count as u32)
        .map(|id| smallest[find(&mut parent, id) as usize])
        .collect();
    log(start, &format!("people: {count} IDs"));

    let mut live: Vec<Live> = kept
        .iter()
        .zip(&event_people)
        .map(|(event, &id)| Live {
            person: person[id as usize],
            ts: event.ts,
            price: event.price,
            category: event.category,
        })
        .collect();
    live.par_sort_unstable_by_key(|e| (e.person, e.ts));
    let mut person_clicks: Vec<PersonClick> = clicks
        .iter()
        .zip(&click_people)
        .map(|(click, &id)| PersonClick {
            person: person[id as usize],
            ts: click.ts,
            click_id: click.click_id,
            campaign: click.campaign,
        })
        .collect();
    person_clicks.par_sort_unstable_by(|a, b| {
        (a.person, a.ts, a.click_id).cmp(&(b.person, b.ts, b.click_id))
    });
    // Offsets of each person's clicks.
    let mut click_start = vec![0u32; count + 1];
    for click in &person_clicks {
        click_start[click.person as usize + 1] += 1;
    }
    for at in 0..count {
        click_start[at + 1] += click_start[at];
    }
    log(start, "sorted");

    // Sessions, credit, assists, and both tables.
    let first_day = window_start.div_euclid(DAY);
    let days = ((window_end - 1).div_euclid(DAY) - first_day + 1) as usize;
    let campaign_slots = campaign_ids.len() + 1;
    // sessions, purchasing, revenue, assisted per (campaign, day)
    let mut campaign_table = vec![[0i64; 4]; campaign_slots * days];
    let mut user_rows: Vec<UserRow> = Vec::new();
    let mut stamp = vec![NONE; campaign_slots];
    let mut session_number: u32 = 0;
    let mut category_sums: Vec<(u16, u64)> = Vec::new();

    let mut at = 0;
    while at < live.len() {
        let who = live[at].person;
        let mut end = at;
        while end < live.len() && live[end].person == who {
            end += 1;
        }
        let person_events = &live[at..end];
        let clicks_of =
            &person_clicks[click_start[who as usize] as usize..click_start[who as usize + 1] as usize];

        // One user_daily row per day.
        let mut row: Option<(i64, i64, i64, i64)> = None; // day, sessions, events, revenue
        category_sums.clear();
        let flush = |row: &mut Option<(i64, i64, i64, i64)>,
                         sums: &mut Vec<(u16, u64)>,
                         rows: &mut Vec<UserRow>| {
            if let Some((day, sessions, events, revenue)) = row.take() {
                let mut top: Option<(u16, u64)> = None;
                for &(category, sum) in sums.iter() {
                    top = match top {
                        None => Some((category, sum)),
                        Some((tc, ts_)) => {
                            if sum > ts_
                                || (sum == ts_
                                    && category_names[category as usize]
                                        < category_names[tc as usize])
                            {
                                Some((category, sum))
                            } else {
                                Some((tc, ts_))
                            }
                        }
                    };
                }
                rows.push(UserRow {
                    user_id: ids.names[who as usize].to_vec(),
                    day: day as i32,
                    sessions,
                    events,
                    revenue,
                    top_category: top.map(|(c, _)| category_names[c as usize].to_vec()),
                });
                sums.clear();
            }
        };

        let mut s = 0;
        while s < person_events.len() {
            let session_start = person_events[s].ts;
            let mut e = s + 1;
            let mut previous = session_start;
            while e < person_events.len() && person_events[e].ts - previous <= SESSION_GAP {
                previous = person_events[e].ts;
                e += 1;
            }
            let session = &person_events[s..e];
            s = e;
            let day = session_start.div_euclid(DAY);
            if row.is_some_and(|r| r.0 != day) {
                flush(&mut row, &mut category_sums, &mut user_rows);
            }
            let mut revenue: u64 = 0;
            for event in session {
                if event.price > 0 {
                    let price = u64::from(event.price);
                    revenue += price;
                    match category_sums.iter_mut().find(|(c, _)| *c == event.category) {
                        Some(entry) => entry.1 += price,
                        None => category_sums.push((event.category, price)),
                    }
                }
            }
            let r = row.get_or_insert((day, 0, 0, 0));
            r.1 += 1;
            r.2 += session.len() as i64;
            r.3 += revenue as i64;

            // Credit and assists.
            let day_slot = (day - first_day) as usize;
            let hi = clicks_of.partition_point(|c| c.ts <= session_start);
            let lo = clicks_of.partition_point(|c| c.ts < session_start - WEEK);
            let credited = if hi > lo {
                let latest = clicks_of[hi - 1].ts;
                let first = clicks_of[lo..hi].partition_point(|c| c.ts < latest) + lo;
                clicks_of[first].campaign
            } else {
                organic
            };
            let cell = &mut campaign_table[credited as usize * days + day_slot];
            cell[0] += 1;
            cell[1] += i64::from(revenue > 0);
            cell[2] += revenue as i64;
            for click in &clicks_of[lo..hi] {
                let campaign = click.campaign as usize;
                if stamp[campaign] != session_number {
                    stamp[campaign] = session_number;
                    campaign_table[campaign * days + day_slot][3] += 1;
                }
            }
            session_number += 1;
        }
        flush(&mut row, &mut category_sums, &mut user_rows);
        at = end;
    }
    log(start, &format!("sessions: {session_number}"));

    let mut campaign_rows = Vec::new();
    for campaign in 0..campaign_slots {
        for day_slot in 0..days {
            let [sessions, purchasing, revenue, assisted] =
                campaign_table[campaign * days + day_slot];
            if sessions == 0 && assisted == 0 {
                continue;
            }
            let (id, channel): (&[u8], &[u8]) = if campaign == organic as usize {
                (b"organic", b"organic")
            } else {
                (campaign_ids[campaign], campaign_channels[campaign])
            };
            campaign_rows.push(CampaignRow {
                campaign_id: id.to_vec(),
                channel: channel.to_vec(),
                day: (first_day + day_slot as i64) as i32,
                sessions,
                purchasing_sessions: purchasing,
                revenue,
                assisted_sessions: assisted,
            });
        }
    }
    out::write_users(&output.join("user_daily"), &user_rows);
    out::write_campaigns(&output.join("campaign_daily"), &campaign_rows);
    log(
        start,
        &format!(
            "wrote {} user rows, {} campaign rows",
            user_rows.len(),
            campaign_rows.len()
        ),
    );
}
