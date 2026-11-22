//! The main pipeline, for inputs whose IDs all pack into a u64 (1 to 9 ASCII
//! bytes; the generator's IDs always do). It moves each row far less than the
//! general pipeline in main.rs, which stays as the exact fallback:
//!
//! 1. Links first: a concurrent union-find over packed link IDs, so every
//!    linked ID's person name (its smallest ID) is known before events.
//! 2. One parse pass: each valid event copy goes straight to its person's
//!    partition (16 bytes), each valid click too, and every event copy leaves
//!    a stub (event_id, ingest_ts, where its row went) in an event_id
//!    partition.
//! 3. Dedup per event_id partition: every copy but the earliest-ingested is
//!    marked dead in place (and the earliest too if it was invalid).
//! 4. Sessions per person partition.
//!
//! Returns false, having written nothing, when the input does not fit; the
//! caller then runs the general pipeline.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

/// Person partitions and event_id partitions.
const PARTS: usize = 64;
/// A stub whose copy was invalid, so it has no row to mark.
const NO_ROW: u64 = u64::MAX;

/// An ID of 1 to 9 bytes, each 1..=127, packed 7 bits a byte, big-endian and
/// left-aligned: integer order is byte-wise string order, and no ID packs
/// to 0.
#[inline]
pub fn pack_id(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 9 {
        return None;
    }
    let mut value = 0u64;
    let mut bad = 0u8;
    for &b in s {
        bad |= u8::from(b == 0) | (b >> 7);
        value = (value << 7) | u64::from(b);
    }
    if bad != 0 {
        return None;
    }
    Some(value << (7 * (9 - n)))
}

/// The bytes of a packed ID.
pub fn unpack_id(value: u64, out: &mut Vec<u8>) {
    for at in (0..9).rev() {
        let byte = ((value >> (7 * at)) & 0x7F) as u8;
        if byte == 0 {
            break;
        }
        out.push(byte);
    }
}

#[inline]
fn mix(key: u64) -> u64 {
    fold(key ^ 0x2545_F491_4F6C_DD1D, 0x9E37_79B9_7F4A_7C15)
}

#[inline]
fn part_of(key: u64) -> usize {
    (mix(key) >> 58) as usize
}

#[inline]
fn stub_part_of(key: u64) -> usize {
    ((mix(key) >> 52) & 63) as usize
}

/// A valid event copy in its person's partition. `price_category` is
/// INVALID once dedup marks the copy dead.
#[derive(Clone, Copy)]
struct Row {
    person: u64,
    ts: u32,
    price_category: u32,
}

/// One event copy, valid or not, for dedup.
#[derive(Clone, Copy)]
struct Stub {
    id: u64,
    ingest: i64,
    /// worker << 48 | partition << 32 | index of its Row, or NO_ROW.
    row: u64,
}

#[derive(Clone, Copy)]
struct Click {
    person: u64,
    click: u64,
    ts: i64,
    campaign: u16,
}

/// A click numbered by person within its partition, for the session pass.
#[derive(Clone, Copy)]
struct LocalClick {
    person: u32,
    campaign: u16,
    ts: i64,
    click: u64,
}

impl SessionClick for LocalClick {
    #[inline]
    fn person(&self) -> u32 {
        self.person
    }
    #[inline]
    fn ts(&self) -> i64 {
        self.ts
    }
    #[inline]
    fn campaign(&self) -> u16 {
        self.campaign
    }
}

struct Worker {
    rows: Vec<Vec<Row>>,
    clicks: Vec<Vec<Click>>,
    stubs: Vec<Vec<Stub>>,
}

/// Linked IDs and their person names: open addressing, key 0 = empty.
struct People {
    entries: Vec<[u64; 2]>,
    mask: usize,
}

impl People {
    /// Starts loading `key`'s slot, so `name` later finds it in cache.
    #[inline]
    fn prefetch(&self, key: u64) {
        let at = mix(key) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.entries.as_ptr().add(at) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline]
    fn name(&self, key: u64) -> u64 {
        let mut at = mix(key) as usize & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked(at) };
            if entry[0] == key {
                return entry[1];
            }
            if entry[0] == 0 {
                return key;
            }
            at = (at + 1) & self.mask;
        }
    }
}

/// The concurrent table of link IDs: the slot of `key`, inserting it.
#[inline]
fn insert(keys: &[AtomicU64], mask: usize, key: u64) -> u32 {
    let mut at = mix(key) as usize & mask;
    loop {
        let current = keys[at].load(Ordering::Acquire);
        if current == key {
            return at as u32;
        }
        if current == 0 {
            match keys[at].compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return at as u32,
                Err(seen) if seen == key => return at as u32,
                Err(_) => {}
            }
            continue;
        }
        at = (at + 1) & mask;
    }
}

pub struct Inputs<'a> {
    pub work: &'a [Piece],
    /// The mapped event, click and link files.
    pub maps: [&'a [Mmap]; 3],
    pub context: &'a Context<'a>,
    pub category_rank: &'a [u16],
    pub tables: Tables<'a>,
    pub window_start: i64,
    pub window_end: i64,
}

pub fn run(inputs: &Inputs, start: Instant) -> bool {
    let Inputs {
        work,
        maps,
        context,
        category_rank,
        window_start,
        window_end,
        ..
    } = *inputs;
    let bytes_of = |piece: &Piece| -> &[u8] {
        match piece.kind {
            Kind::Event => &maps[0][piece.map],
            Kind::Click => &maps[1][piece.map],
            Kind::Link => &maps[2][piece.map],
        }
    };
    let workers = rayon::current_num_threads();
    if workers > 0xFFFF {
        return false;
    }
    let unfit = AtomicBool::new(false);

    // 1. Links, then the union-find and each linked ID's person name.
    let pairs: Vec<(u64, u64)> = work
        .par_iter()
        .filter(|piece| piece.kind == Kind::Link)
        .map(|piece| {
            let mut out = Vec::new();
            csv::for_records::<2>(lines_of(bytes_of(piece), piece.from, piece.to), |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    match (pack_id(a), pack_id(b)) {
                        (Some(a), Some(b)) => out.push((a, b)),
                        _ => unfit.store(true, Ordering::Relaxed),
                    }
                }
            });
            out
        })
        .flatten()
        .collect();
    if unfit.load(Ordering::Relaxed) {
        return false;
    }
    let capacity = (pairs.len() * 5 / 2).next_power_of_two().max(1024);
    let mask = capacity - 1;
    let keys: Vec<AtomicU64> = (0..capacity).map(|_| AtomicU64::new(0)).collect();
    let parent: Vec<AtomicU32> = (0..capacity as u32).map(AtomicU32::new).collect();
    pairs.par_chunks(4096).for_each(|chunk| {
        for &(a, b) in chunk {
            let (a, b) = (insert(&keys, mask, a), insert(&keys, mask, b));
            union_atomic(&parent, a, b);
        }
    });
    let smallest: Vec<AtomicU64> = (0..capacity).map(|_| AtomicU64::new(u64::MAX)).collect();
    (0..capacity).into_par_iter().for_each(|at| {
        let key = keys[at].load(Ordering::Relaxed);
        if key != 0 {
            smallest[find_atomic(&parent, at as u32) as usize].fetch_min(key, Ordering::Relaxed);
        }
    });
    let people = People {
        entries: (0..capacity)
            .into_par_iter()
            .map(|at| {
                let key = keys[at].load(Ordering::Relaxed);
                if key == 0 {
                    [0, 0]
                } else {
                    [key, smallest[find_atomic(&parent, at as u32) as usize].load(Ordering::Relaxed)]
                }
            })
            .collect(),
        mask,
    };
    log(start, &format!("linked {} pairs", pairs.len()));

    // 2. Events and clicks, from a shared queue in file order.
    let next = AtomicUsize::new(0);
    let mut parsed: Vec<Worker> = (0..workers)
        .into_par_iter()
        .map(|worker| {
            let mut out = Worker {
                rows: (0..PARTS).map(|_| Vec::new()).collect(),
                clicks: (0..PARTS).map(|_| Vec::new()).collect(),
                stubs: (0..PARTS).map(|_| Vec::new()).collect(),
            };
            let mut clock = ts::Parser::new();
            loop {
                let at = next.fetch_add(1, Ordering::Relaxed);
                let Some(piece) = work.get(at) else {
                    break;
                };
                let chunk = lines_of(bytes_of(piece), piece.from, piece.to);
                match piece.kind {
                    Kind::Event => csv::for_records::<7>(chunk, |fields| {
                        parse_event_into(fields, context, &mut clock, &people, worker, &mut out, &unfit)
                    }),
                    Kind::Click => csv::for_records::<4>(chunk, |fields| {
                        parse_click_into(fields, context, &mut clock, &people, &mut out, &unfit)
                    }),
                    Kind::Link => {}
                }
            }
            out
        })
        .collect();
    if unfit.load(Ordering::Relaxed) {
        return false;
    }
    log(start, "parsed");

    // 3. Dedup: per event_id partition, mark every row but the kept copy's dead.
    struct RowBase(*mut Row);
    unsafe impl Sync for RowBase {}
    unsafe impl Send for RowBase {}
    let bases: Vec<Vec<RowBase>> = parsed
        .iter_mut()
        .map(|worker| worker.rows.iter_mut().map(|rows| RowBase(rows.as_mut_ptr())).collect())
        .collect();
    let parsed = &parsed;
    let bases = &bases;
    let kill = |row: u64| {
        if row != NO_ROW {
            let worker = (row >> 48) as usize;
            let part = ((row >> 32) & 0xFFFF) as usize;
            let index = (row & 0xFFFF_FFFF) as usize;
            // Each row belongs to one event_id, so one thread writes it.
            unsafe { (*bases[worker][part].0.add(index)).price_category = INVALID };
        }
    };
    (0..PARTS).into_par_iter().for_each(|part| {
        let total: usize = parsed.iter().map(|worker| worker.stubs[part].len()).sum();
        let mut kept: HashTable<Stub> = HashTable::with_capacity(total);
        for worker in parsed {
            for &stub in &worker.stubs[part] {
                match kept.entry(mix(stub.id), |k| k.id == stub.id, |k| mix(k.id)) {
                    Entry::Occupied(mut entry) => {
                        let best = entry.get_mut();
                        if stub.ingest < best.ingest {
                            kill(best.row);
                            *best = stub;
                        } else {
                            // Equal ingest_ts means identical copies.
                            kill(stub.row);
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(stub);
                    }
                }
            }
        }
    });
    log(start, "deduplicated");

    // 4. Sessions, credit and assists, per person partition.
    let Tables {
        organic, first_day, days, ..
    } = inputs.tables;
    let campaign_slots = organic + 1;
    let empty_tally = || Tally {
        users: Vec::new(),
        sums: Vec::new(),
        campaigns: vec![[0i64; 4]; campaign_slots * days],
        stamp: vec![u64::MAX; campaign_slots],
        session: 0,
    };
    let next_part = AtomicUsize::new(0);
    let tally = (0..workers)
        .into_par_iter()
        .map(|_| {
            let mut tally = empty_tally();
            let mut local: FxHashMap<u64, u32> = FxHashMap::default();
            let mut names: Vec<u64> = Vec::new();
            let mut row_people: Vec<u32> = Vec::new();
            loop {
                let part = next_part.fetch_add(1, Ordering::Relaxed);
                if part >= PARTS {
                    break;
                }
                local.clear();
                names.clear();
                row_people.clear();
                // Number this partition's people, counting their rows.
                let mut starts: Vec<usize> = vec![0];
                for worker in parsed {
                    for row in &worker.rows[part] {
                        if row.price_category == INVALID {
                            continue;
                        }
                        let index = *local.entry(row.person).or_insert_with(|| {
                            names.push(row.person);
                            starts.push(0);
                            (names.len() - 1) as u32
                        });
                        starts[index as usize + 1] += 1;
                        row_people.push(index);
                    }
                }
                let people = names.len();
                for index in 0..people {
                    starts[index + 1] += starts[index];
                }
                let mut events: Vec<PersonEvent> = Vec::with_capacity(starts[people]);
                let mut cursor = starts.clone();
                unsafe {
                    let target = events.as_mut_ptr();
                    let mut at = 0;
                    for worker in parsed {
                        for row in &worker.rows[part] {
                            if row.price_category == INVALID {
                                continue;
                            }
                            let index = row_people[at] as usize;
                            at += 1;
                            target.add(cursor[index]).write(PersonEvent {
                                person: index as u32,
                                ts: row.ts,
                                price_category: row.price_category,
                            });
                            cursor[index] += 1;
                        }
                    }
                    events.set_len(starts[people]);
                }
                // Clicks of people with events; the rest never count.
                let mut click_starts = vec![0usize; people + 1];
                let mut click_people: Vec<u32> = Vec::new();
                for worker in parsed {
                    for click in &worker.clicks[part] {
                        let index = local.get(&click.person).copied().unwrap_or(u32::MAX);
                        click_people.push(index);
                        if index != u32::MAX {
                            click_starts[index as usize + 1] += 1;
                        }
                    }
                }
                for index in 0..people {
                    click_starts[index + 1] += click_starts[index];
                }
                let mut clicks: Vec<LocalClick> = Vec::with_capacity(click_starts[people]);
                let mut cursor = click_starts.clone();
                unsafe {
                    let target = clicks.as_mut_ptr();
                    let mut at = 0;
                    for worker in parsed {
                        for click in &worker.clicks[part] {
                            let index = click_people[at];
                            at += 1;
                            if index == u32::MAX {
                                continue;
                            }
                            let index = index as usize;
                            target.add(cursor[index]).write(LocalClick {
                                person: index as u32,
                                campaign: click.campaign,
                                ts: click.ts,
                                click: click.click,
                            });
                            cursor[index] += 1;
                        }
                    }
                    clicks.set_len(click_starts[people]);
                }
                for index in 0..people {
                    let mine = &mut events[starts[index]..starts[index + 1]];
                    mine.sort_unstable_by_key(|e| e.ts);
                    let theirs = &mut clicks[click_starts[index]..click_starts[index + 1]];
                    theirs.sort_unstable_by_key(|c| (c.ts, c.click));
                    sessionize(
                        mine,
                        theirs,
                        &mut tally,
                        first_day,
                        days,
                        organic,
                        category_rank,
                        window_start,
                        names[index],
                    );
                }
            }
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

    let _ = window_end;
    write_tables(&inputs.tables, &tally, |name, out| unpack_id(name, out), start);
    std::mem::forget((tally, people, keys, parent, smallest, pairs));
    true
}

#[inline]
fn parse_event_into(
    fields: [&[u8]; 7],
    context: &Context,
    clock: &mut ts::Parser,
    people: &People,
    worker: usize,
    out: &mut Worker,
    unfit: &AtomicBool,
) {
    let [event_id, user_id, ts_field, ingest_field, kind, product, price] = fields;
    if event_id.is_empty() {
        return;
    }
    let Some(id) = pack_id(event_id) else {
        unfit.store(true, Ordering::Relaxed);
        return;
    };
    // The user's person slot is likely a cache miss: start loading it now,
    // and parse the timestamps while it arrives.
    let user = pack_id(user_id);
    if let Some(user) = user {
        people.prefetch(user);
    }
    let ingest = clock.parse(ingest_field);
    let mut valid = !user_id.is_empty() && !product.is_empty() && ingest.is_some();
    let ingest = ingest.unwrap_or(i64::MAX);
    let mut price_cents: u32 = 0;
    match kind {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(price) {
            Some(p) => price_cents = p,
            None => valid = false,
        },
        _ => valid = false,
    }
    let mut ts = context.window_start;
    if valid {
        match clock.parse(ts_field) {
            Some(t) if t >= context.window_start && t < context.window_end => ts = t,
            _ => valid = false,
        }
    }
    if valid && ingest - ts > LATE {
        valid = false;
    }
    let mut row = NO_ROW;
    if valid {
        let Some(user) = user else {
            unfit.store(true, Ordering::Relaxed);
            return;
        };
        // Only a purchase's category is ever used.
        let price_category = if price_cents > 0 {
            pack(
                price_cents,
                context.products.get(product).unwrap_or(context.unknown_category),
            )
        } else {
            pack(0, 0)
        };
        let person = people.name(user);
        let part = part_of(person);
        let rows = &mut out.rows[part];
        row = (worker as u64) << 48 | (part as u64) << 32 | rows.len() as u64;
        rows.push(Row {
            person,
            ts: (ts - context.window_start) as u32,
            price_category,
        });
    }
    out.stubs[stub_part_of(id)].push(Stub { id, ingest, row });
}

#[inline]
fn parse_click_into(
    fields: [&[u8]; 4],
    context: &Context,
    clock: &mut ts::Parser,
    people: &People,
    out: &mut Worker,
    unfit: &AtomicBool,
) {
    let [click_id, user_id, campaign, ts_field] = fields;
    if click_id.is_empty() || user_id.is_empty() {
        return;
    }
    let user = pack_id(user_id);
    if let Some(user) = user {
        people.prefetch(user);
    }
    let Some(campaign) = context.campaigns.get(campaign) else {
        return;
    };
    let Some(ts) = clock.parse(ts_field) else {
        return;
    };
    if ts < context.window_start - WEEK || ts >= context.window_end {
        return;
    }
    let (Some(click), Some(user)) = (pack_id(click_id), user) else {
        unfit.store(true, Ordering::Relaxed);
        return;
    };
    let person = people.name(user);
    out.clicks[part_of(person)].push(Click {
        person,
        click,
        ts,
        campaign,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packing_keeps_order_and_bytes() {
        let ids: Vec<&[u8]> = vec![
            b"a", b"a ", b"ab", b"abc", b"U1", b"u1", b" u1", b"u1 ", b"u1\r", b"zzzzzzzzz",
            b"\x01", b"\x7f", b"123456789", b"e0", b"e00",
        ];
        for a in &ids {
            let pa = pack_id(a).unwrap();
            assert_ne!(pa, 0);
            let mut back = Vec::new();
            unpack_id(pa, &mut back);
            assert_eq!(&back[..], *a);
            for b in &ids {
                let pb = pack_id(b).unwrap();
                assert_eq!(pa.cmp(&pb), a.cmp(b), "{a:?} {b:?}");
            }
        }
        for unfit in [&b""[..], b"0123456789", b"\x00", b"\x80", b"\xc3\xa9"] {
            assert_eq!(pack_id(unfit), None);
        }
    }
}
