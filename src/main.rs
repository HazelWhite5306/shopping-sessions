//! Shopping Sessions: clean the click-stream CSVs in DATA_DIR, join each
//! person's IDs, build sessions, credit and assist campaigns, and write
//! user_daily and campaign_daily as Parquet to OUTPUT_DIR. rules.md is the spec.
//!
//! Every phase runs in parallel: chunks are parsed straight into hash shards,
//! events are deduplicated per shard, user IDs are interned per shard, and
//! people are regrouped into buckets that are sessionized independently.

mod csv;
mod out;
mod ts;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use out::{CampaignRow, UserRow};
use ts::DAY;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const WEEK: i64 = 7 * DAY;
const SESSION_GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const MAX_PRICE: u64 = 10_000_000;
const CHUNK: usize = 2 << 20;
/// Hash shards for event IDs and user IDs, and buckets for people.
const SHARDS: usize = 64;
const BUCKETS: usize = 256;
const NO_CATEGORY: u16 = u16::MAX;

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

// ---------------------------------------------------------------- hashing

#[inline]
fn fold(a: u64, b: u64) -> u64 {
    let product = u128::from(a) * u128::from(b);
    (product as u64) ^ ((product >> 64) as u64)
}

/// A fast, well-mixed hash of a byte string (wyhash-style folded multiply).
#[inline]
fn hash_bytes(s: &[u8]) -> u64 {
    const K0: u64 = 0xa076_1d64_78bd_642f;
    const K1: u64 = 0xe703_7ed1_a0b4_28db;
    const K2: u64 = 0x8ebc_6af0_9c88_c6e3;
    let n = s.len();
    let mut seed = K0 ^ (n as u64).wrapping_mul(K2);
    let (a, b) = if n >= 8 {
        let mut at = 0;
        while n - at > 16 {
            let x = u64::from_le_bytes(s[at..at + 8].try_into().unwrap());
            let y = u64::from_le_bytes(s[at + 8..at + 16].try_into().unwrap());
            seed = fold(x ^ K1, y ^ seed);
            at += 16;
        }
        (
            u64::from_le_bytes(s[at..at + 8].try_into().unwrap()),
            u64::from_le_bytes(s[n - 8..].try_into().unwrap()),
        )
    } else if n >= 4 {
        (
            u64::from(u32::from_le_bytes(s[..4].try_into().unwrap())),
            u64::from(u32::from_le_bytes(s[n - 4..].try_into().unwrap())),
        )
    } else if n > 0 {
        (
            u64::from(s[0]) | u64::from(s[n / 2]) << 8 | u64::from(s[n - 1]) << 16,
            0,
        )
    } else {
        (0, 0)
    };
    fold(fold(a ^ K1, b ^ seed) ^ K2, (n as u64) ^ K1)
}

#[inline]
fn shard(hash: u64) -> usize {
    (hash >> 40) as usize & (SHARDS - 1)
}

#[inline]
fn bucket(person: u32) -> usize {
    (fold(u64::from(person), 0x9E37_79B9_7F4A_7C15) as usize) & (BUCKETS - 1)
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
    let _ = map.advise(memmap2::Advice::Sequential);
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

/// A lookup from IDs to small indexes, with an array fast path for IDs in
/// the canonical form `<prefix><n>` (no leading zero), like `p123` or `c_45`.
struct Lookup<'a> {
    prefix: &'static [u8],
    by_number: Vec<u16>,
    by_bytes: FxHashMap<&'a [u8], u16>,
}

impl<'a> Lookup<'a> {
    fn new(prefix: &'static [u8]) -> Self {
        Self {
            prefix,
            by_number: Vec::new(),
            by_bytes: FxHashMap::default(),
        }
    }

    #[inline]
    fn number(&self, id: &[u8]) -> Option<usize> {
        let digits = id.strip_prefix(self.prefix)?;
        if digits.is_empty() || digits.len() > 7 || digits[0] == b'0' {
            return None;
        }
        let mut n = 0usize;
        for &b in digits {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n * 10 + usize::from(b - b'0');
        }
        Some(n)
    }

    fn insert(&mut self, id: &'a [u8], value: u16) {
        if let Some(n) = self.number(id) {
            if self.by_number.len() <= n {
                self.by_number.resize(n + 1, NO_CATEGORY);
            }
            self.by_number[n] = value;
        }
        self.by_bytes.insert(id, value);
    }

    #[inline]
    fn get(&self, id: &[u8]) -> Option<u16> {
        match self.number(id) {
            Some(n) => self
                .by_number
                .get(n)
                .copied()
                .filter(|&value| value != NO_CATEGORY),
            None => self.by_bytes.get(id).copied(),
        }
    }
}

// ---------------------------------------------------------------- rows

/// One parsed copy of an event, before dedup.
#[derive(Clone, Copy)]
struct EventCopy<'a> {
    event_id: &'a [u8],
    user_id: &'a [u8],
    event_hash: u64,
    user_hash: u64,
    ts: i64,
    ingest: i64,
    /// Price for a valid purchase, 0 for view and cart.
    price: u32,
    category: u16,
    valid: bool,
}

/// A kept, valid event, before its user ID is interned.
#[derive(Clone, Copy)]
struct LiveEvent<'a> {
    user_id: &'a [u8],
    user_hash: u64,
    ts: i64,
    price: u32,
    category: u16,
}

#[derive(Clone, Copy)]
struct ValidClick<'a> {
    user_id: &'a [u8],
    user_hash: u64,
    click_id: &'a [u8],
    ts: i64,
    campaign: u16,
}

/// An event or click whose user ID is now a number (an ID, then a person).
#[derive(Clone, Copy)]
struct PersonEvent {
    person: u32,
    price: u32,
    ts: i64,
    category: u16,
}

#[derive(Clone, Copy)]
struct PersonClick<'a> {
    person: u32,
    campaign: u16,
    ts: i64,
    click_id: &'a [u8],
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
    products: Lookup<'a>,
    unknown_category: u16,
    campaigns: Lookup<'a>,
}

#[inline]
fn parse_event<'a>(line: &'a [u8], context: &Context) -> Option<EventCopy<'a>> {
    let [event_id, user_id, ts_field, ingest_field, kind, product, price] = csv::split::<7>(line)?;
    if event_id.is_empty() {
        return None;
    }
    let ingest = ts::parse(ingest_field);
    let mut valid = !user_id.is_empty() && !product.is_empty() && ingest.is_some();
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
    let mut ts = 0;
    if valid {
        match ts::parse(ts_field) {
            Some(t) if t >= context.window_start && t < context.window_end => ts = t,
            _ => valid = false,
        }
    }
    if valid && ingest - ts > LATE {
        valid = false;
    }
    let (category, user_hash) = if valid {
        (
            context
                .products
                .get(product)
                .unwrap_or(context.unknown_category),
            hash_bytes(user_id),
        )
    } else {
        (0, 0)
    };
    Some(EventCopy {
        event_id,
        user_id,
        event_hash: hash_bytes(event_id),
        user_hash,
        ts,
        ingest,
        price: price_cents,
        category,
        valid,
    })
}

#[inline]
fn parse_click<'a>(line: &'a [u8], context: &Context) -> Option<ValidClick<'a>> {
    let [click_id, user_id, campaign, ts_field] = csv::split::<4>(line)?;
    if click_id.is_empty() || user_id.is_empty() {
        return None;
    }
    let campaign = context.campaigns.get(campaign)?;
    let ts = ts::parse(ts_field)?;
    if ts < context.window_start - WEEK || ts >= context.window_end {
        return None;
    }
    Some(ValidClick {
        user_id,
        user_hash: hash_bytes(user_id),
        click_id,
        ts,
        campaign,
    })
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let up = parent[parent[x as usize] as usize];
        parent[x as usize] = up;
        x = up;
    }
    x
}

/// Moves `items` (one Vec of shards per producer) into one Vec per shard.
fn transpose<T: Send>(producers: Vec<Vec<Vec<T>>>, shards: usize) -> Vec<Vec<T>> {
    let mut by_shard: Vec<Vec<Vec<T>>> = (0..shards).map(|_| Vec::new()).collect();
    for producer in producers {
        for (at, part) in producer.into_iter().enumerate() {
            by_shard[at].push(part);
        }
    }
    by_shard
        .into_par_iter()
        .map(|parts| {
            let total = parts.iter().map(Vec::len).sum();
            let mut joined = Vec::with_capacity(total);
            for mut part in parts {
                joined.append(&mut part);
            }
            joined
        })
        .collect()
}

struct Interner<'a> {
    names: Vec<&'a [u8]>,
    hashes: Vec<u64>,
    table: HashTable<u32>,
}

impl<'a> Interner<'a> {
    #[inline]
    fn intern(&mut self, id: &'a [u8], hash: u64) -> u32 {
        let Self {
            names,
            hashes,
            table,
        } = self;
        match table.entry(
            hash,
            |&at| names[at as usize] == id,
            |&at| hashes[at as usize],
        ) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let at = names.len() as u32;
                entry.insert(at);
                names.push(id);
                hashes.push(hash);
                at
            }
        }
    }
}

/// One user shard's interned IDs and rows.
struct UserShard<'a> {
    names: Vec<&'a [u8]>,
    /// (link side as link * 2 + side, local ID)
    link_sides: Vec<(u32, u32)>,
    events: Vec<PersonEvent>,
    clicks: Vec<PersonClick<'a>>,
}

/// Per-thread results of the session pass.
struct Tally {
    users: Vec<(u32, i32, u32, u32, u64, u16)>,
    /// (sessions, purchasing, revenue, assisted) per campaign and day.
    campaigns: Vec<[i64; 4]>,
    stamp: Vec<u64>,
    session: u64,
}

// ---------------------------------------------------------------- pipeline

fn run(data: &Path, output: &Path, start: Instant) {
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let window_start = manifest_time(&manifest, "\"window_start\"");
    let window_end = manifest_time(&manifest, "\"window_end\"");

    let products_raw = fs::read(data.join("products.csv")).unwrap_or_default();
    let campaigns_raw = fs::read(data.join("campaigns.csv")).unwrap_or_default();
    let mut category_names: Vec<&[u8]> = Vec::new();
    let mut category_index: FxHashMap<&[u8], u16> = FxHashMap::default();
    let mut products = Lookup::new(b"p");
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
    // Ranks in byte order, so a tie in top_category goes to the smaller rank.
    let mut order: Vec<usize> = (0..category_names.len()).collect();
    order.sort_by_key(|&at| category_names[at]);
    let mut category_rank = vec![0u16; category_names.len()];
    for (rank, &at) in order.iter().enumerate() {
        category_rank[at] = rank as u16;
    }
    let mut campaign_ids: Vec<&[u8]> = Vec::new();
    let mut campaign_channels: Vec<&[u8]> = Vec::new();
    let mut campaigns = Lookup::new(b"c_");
    for [campaign, channel] in lookup_rows(&campaigns_raw) {
        campaigns.insert(campaign, campaign_ids.len() as u16);
        campaign_ids.push(campaign);
        campaign_channels.push(channel);
    }
    let organic = campaign_ids.len();
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
    let event_chunks = chunks(&event_maps);
    let click_chunks = chunks(&click_maps);
    let link_chunks = chunks(&link_maps);
    log(start, "mapped");

    // 1. Parse each chunk straight into shards.
    let (events, (clicks, links)) = rayon::join(
        || {
            event_chunks
                .par_iter()
                .map(|chunk| {
                    let mut shards: Vec<Vec<EventCopy>> = (0..SHARDS)
                        .map(|_| Vec::with_capacity(chunk.len() / 80 / SHARDS + 16))
                        .collect();
                    csv::for_lines(chunk, |line| {
                        if let Some(event) = parse_event(line, &context) {
                            shards[shard(event.event_hash)].push(event);
                        }
                    });
                    shards
                })
                .collect::<Vec<_>>()
        },
        || {
            rayon::join(
                || {
                    click_chunks
                        .par_iter()
                        .map(|chunk| {
                            let mut shards: Vec<Vec<ValidClick>> = (0..SHARDS)
                                .map(|_| Vec::with_capacity(chunk.len() / 50 / SHARDS + 16))
                                .collect();
                            csv::for_lines(chunk, |line| {
                                if let Some(click) = parse_click(line, &context) {
                                    shards[shard(click.user_hash)].push(click);
                                }
                            });
                            shards
                        })
                        .collect::<Vec<_>>()
                },
                || {
                    link_chunks
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
                        .collect::<Vec<[&[u8]; 2]>>()
                },
            )
        },
    );
    log(start, "parsed");

    // 2. Dedup events per shard; keep valid survivors, sharded by user.
    let events = transpose(events, SHARDS);
    let clicks = transpose(clicks, SHARDS);
    let live: Vec<Vec<Vec<LiveEvent>>> = events
        .par_iter()
        .map(|copies| {
            let mut table: HashTable<u32> = HashTable::with_capacity(copies.len());
            for (at, copy) in copies.iter().enumerate() {
                match table.entry(
                    copy.event_hash,
                    |&kept| copies[kept as usize].event_id == copy.event_id,
                    |&kept| copies[kept as usize].event_hash,
                ) {
                    Entry::Occupied(mut entry) => {
                        let kept = entry.get_mut();
                        if copy.ingest < copies[*kept as usize].ingest {
                            *kept = at as u32;
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(at as u32);
                    }
                }
            }
            let mut out: Vec<Vec<LiveEvent>> = (0..SHARDS)
                .map(|_| Vec::with_capacity(copies.len() / SHARDS + 16))
                .collect();
            for &kept in table.iter() {
                let copy = &copies[kept as usize];
                if copy.valid {
                    out[shard(copy.user_hash)].push(LiveEvent {
                        user_id: copy.user_id,
                        user_hash: copy.user_hash,
                        ts: copy.ts,
                        price: copy.price,
                        category: copy.category,
                    });
                }
            }
            out
        })
        .collect();
    drop(events);
    let live = transpose(live, SHARDS);
    log(start, "deduplicated");

    // 3. Intern user IDs per shard.
    let mut link_sides: Vec<Vec<(u32, &[u8], u64)>> = (0..SHARDS).map(|_| Vec::new()).collect();
    for (at, [a, b]) in links.iter().enumerate() {
        for (side, id) in [a, b].into_iter().enumerate() {
            let hash = hash_bytes(id);
            link_sides[shard(hash)].push(((at * 2 + side) as u32, id, hash));
        }
    }
    let mut users: Vec<UserShard> = live
        .into_par_iter()
        .zip(clicks.into_par_iter())
        .zip(link_sides.into_par_iter())
        .map(|((live, clicks), sides)| {
            let mut ids = Interner {
                names: Vec::new(),
                hashes: Vec::new(),
                table: HashTable::with_capacity((live.len() + clicks.len()) / 8 + sides.len() + 64),
            };
            let link_sides: Vec<(u32, u32)> = sides
                .iter()
                .map(|&(code, id, hash)| (code, ids.intern(id, hash)))
                .collect();
            let events: Vec<PersonEvent> = live
                .iter()
                .map(|event| PersonEvent {
                    person: ids.intern(event.user_id, event.user_hash),
                    price: event.price,
                    ts: event.ts,
                    category: event.category,
                })
                .collect();
            let clicks: Vec<PersonClick> = clicks
                .iter()
                .map(|click| PersonClick {
                    person: ids.intern(click.user_id, click.user_hash),
                    campaign: click.campaign,
                    ts: click.ts,
                    click_id: click.click_id,
                })
                .collect();
            let names = ids.names;
            UserShard {
                names,
                link_sides,
                events,
                clicks,
            }
        })
        .collect();
    let mut base = Vec::with_capacity(SHARDS);
    let mut count = 0usize;
    for user_shard in &users {
        base.push(count as u32);
        count += user_shard.names.len();
    }
    let names: Vec<&[u8]> = users
        .iter()
        .flat_map(|user_shard| user_shard.names.iter().copied())
        .collect();
    log(start, &format!("interned {count} IDs"));

    // 4. Join linked IDs; each person is named by their smallest ID.
    let mut link_ids = vec![0u32; links.len() * 2];
    for (user_shard, &offset) in users.iter().zip(&base) {
        for &(code, local) in &user_shard.link_sides {
            link_ids[code as usize] = offset + local;
        }
    }
    let mut parent: Vec<u32> = (0..count as u32).collect();
    let mut smallest: Vec<u32> = (0..count as u32).collect();
    for pair in link_ids.chunks_exact(2) {
        let (ra, rb) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
        if ra != rb {
            parent[rb as usize] = ra;
            let (sa, sb) = (smallest[ra as usize], smallest[rb as usize]);
            if names[sb as usize] < names[sa as usize] {
                smallest[ra as usize] = sb;
            }
        }
    }
    let mut person = vec![0u32; count];
    for id in 0..count as u32 {
        person[id as usize] = smallest[find(&mut parent, id) as usize];
    }
    drop(parent);
    log(start, &format!("linked {} pairs", links.len()));

    // 5. Regroup rows by person into buckets.
    let regrouped: Vec<(Vec<Vec<PersonEvent>>, Vec<Vec<PersonClick>>)> = users
        .par_iter_mut()
        .zip(base.par_iter())
        .map(|(user_shard, &offset)| {
            let mut events: Vec<Vec<PersonEvent>> = (0..BUCKETS)
                .map(|_| Vec::with_capacity(user_shard.events.len() / BUCKETS + 8))
                .collect();
            for mut event in user_shard.events.drain(..) {
                event.person = person[(offset + event.person) as usize];
                events[bucket(event.person)].push(event);
            }
            let mut clicks: Vec<Vec<PersonClick>> = (0..BUCKETS)
                .map(|_| Vec::with_capacity(user_shard.clicks.len() / BUCKETS + 8))
                .collect();
            for mut click in user_shard.clicks.drain(..) {
                click.person = person[(offset + click.person) as usize];
                clicks[bucket(click.person)].push(click);
            }
            (events, clicks)
        })
        .collect();
    let (event_parts, click_parts): (Vec<_>, Vec<_>) = regrouped.into_iter().unzip();
    let event_buckets = transpose(event_parts, BUCKETS);
    let click_buckets = transpose(click_parts, BUCKETS);
    log(start, "regrouped");

    // 6. Sessions, credit and assists, per bucket.
    let first_day = window_start.div_euclid(DAY);
    let days = ((window_end - 1).div_euclid(DAY) - first_day + 1).max(1) as usize;
    let campaign_slots = organic + 1;
    let empty_tally = || Tally {
        users: Vec::new(),
        campaigns: vec![[0i64; 4]; campaign_slots * days],
        stamp: vec![u64::MAX; campaign_slots],
        session: 0,
    };
    let tally = event_buckets
        .into_par_iter()
        .zip(click_buckets.into_par_iter())
        .fold(empty_tally, |mut tally, (mut events, mut clicks)| {
            events.sort_unstable_by_key(|e| (e.person, e.ts));
            clicks.sort_unstable_by(|a, b| {
                (a.person, a.ts, a.click_id).cmp(&(b.person, b.ts, b.click_id))
            });
            sessionize(
                &events,
                &clicks,
                &mut tally,
                first_day,
                days,
                organic,
                &category_rank,
            );
            tally
        })
        .reduce(empty_tally, |mut a, mut b| {
            a.users.append(&mut b.users);
            for (x, y) in a.campaigns.iter_mut().zip(&b.campaigns) {
                for k in 0..4 {
                    x[k] += y[k];
                }
            }
            a.session += b.session;
            a
        });
    log(start, &format!("sessions: {}", tally.session));

    // 7. Write both tables.
    let user_rows: Vec<UserRow> = tally
        .users
        .par_iter()
        .map(|&(who, day, sessions, events, revenue, top)| UserRow {
            user_id: names[who as usize].to_vec(),
            day,
            sessions: i64::from(sessions),
            events: i64::from(events),
            revenue: revenue as i64,
            top_category: (top != NO_CATEGORY).then(|| category_names[top as usize].to_vec()),
        })
        .collect();
    let mut campaign_rows = Vec::new();
    for campaign in 0..campaign_slots {
        for day_slot in 0..days {
            let [sessions, purchasing, revenue, assisted] =
                tally.campaigns[campaign * days + day_slot];
            if sessions == 0 && assisted == 0 {
                continue;
            }
            let (id, channel): (&[u8], &[u8]) = if campaign == organic {
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
    rayon::join(
        || out::write_users(&output.join("user_daily"), &user_rows),
        || out::write_campaigns(&output.join("campaign_daily"), &campaign_rows),
    );
    log(
        start,
        &format!(
            "wrote {} user rows, {} campaign rows",
            user_rows.len(),
            campaign_rows.len()
        ),
    );
}

/// Walks one bucket's events (sorted by person, ts) and clicks (sorted by
/// person, ts, click_id) into sessions, user-day rows and campaign tallies.
fn sessionize(
    events: &[PersonEvent],
    clicks: &[PersonClick],
    tally: &mut Tally,
    first_day: i64,
    days: usize,
    organic: usize,
    category_rank: &[u16],
) {
    let mut sums: Vec<(u16, u64)> = Vec::new();
    let mut click_at = 0;
    let mut at = 0;
    while at < events.len() {
        let who = events[at].person;
        let mut end = at + 1;
        while end < events.len() && events[end].person == who {
            end += 1;
        }
        while click_at < clicks.len() && clicks[click_at].person < who {
            click_at += 1;
        }
        let mut click_end = click_at;
        while click_end < clicks.len() && clicks[click_end].person == who {
            click_end += 1;
        }
        let mine = &clicks[click_at..click_end];
        click_at = click_end;

        // The current user-day row: day, sessions, events, revenue.
        let mut row: Option<(i64, u32, u32, u64)> = None;
        sums.clear();
        let mut s = at;
        while s < end {
            let session_start = events[s].ts;
            let mut e = s + 1;
            let mut previous = session_start;
            while e < end && events[e].ts - previous <= SESSION_GAP {
                previous = events[e].ts;
                e += 1;
            }
            let day = session_start.div_euclid(DAY);
            if let Some(r) = row
                && r.0 != day
            {
                emit(tally, who, r, &mut sums, category_rank);
                row = None;
            }
            let mut revenue: u64 = 0;
            for event in &events[s..e] {
                if event.price > 0 {
                    let price = u64::from(event.price);
                    revenue += price;
                    match sums.iter_mut().find(|(c, _)| *c == event.category) {
                        Some(entry) => entry.1 += price,
                        None => sums.push((event.category, price)),
                    }
                }
            }
            let r = row.get_or_insert((day, 0, 0, 0));
            r.1 += 1;
            r.2 += (e - s) as u32;
            r.3 += revenue;

            // Credit and assists.
            let day_slot = (day - first_day) as usize;
            let hi = mine.partition_point(|c| c.ts <= session_start);
            let lo = mine[..hi].partition_point(|c| c.ts < session_start - WEEK);
            let credited = if hi > lo {
                let latest = mine[hi - 1].ts;
                let first = lo + mine[lo..hi].partition_point(|c| c.ts < latest);
                usize::from(mine[first].campaign)
            } else {
                organic
            };
            let cell = &mut tally.campaigns[credited * days + day_slot];
            cell[0] += 1;
            cell[1] += i64::from(revenue > 0);
            cell[2] += revenue as i64;
            let session = tally.session;
            for click in &mine[lo..hi] {
                let campaign = usize::from(click.campaign);
                if tally.stamp[campaign] != session {
                    tally.stamp[campaign] = session;
                    tally.campaigns[campaign * days + day_slot][3] += 1;
                }
            }
            tally.session += 1;
            s = e;
        }
        if let Some(r) = row {
            emit(tally, who, r, &mut sums, category_rank);
        }
        at = end;
    }
}

#[inline]
fn emit(
    tally: &mut Tally,
    who: u32,
    (day, sessions, events, revenue): (i64, u32, u32, u64),
    sums: &mut Vec<(u16, u64)>,
    category_rank: &[u16],
) {
    let mut top = NO_CATEGORY;
    let mut best = 0u64;
    for &(category, sum) in sums.iter() {
        if top == NO_CATEGORY
            || sum > best
            || (sum == best && category_rank[category as usize] < category_rank[top as usize])
        {
            top = category;
            best = sum;
        }
    }
    tally
        .users
        .push((who, day as i32, sessions, events, revenue, top));
    sums.clear();
}
