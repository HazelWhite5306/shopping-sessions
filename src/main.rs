//! Shopping Sessions: clean the click-stream CSVs in DATA_DIR, join each
//! person's IDs, build sessions, credit and assist campaigns, and write
//! user_daily and campaign_daily as Parquet to OUTPUT_DIR. rules.md is the spec.
//!
//! Every phase runs in parallel: chunks are parsed straight into hash shards,
//! events are deduplicated per shard, user IDs are interned per shard, and
//! people are regrouped into buckets that are sessionized independently.
//! Rows hold text fields as packed 8-byte references into the memory-mapped
//! files, and shards are read in place, so memory stays small.

mod csv;
mod diag;
mod key;
mod out;
mod ts;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use key::Key;
use out::CampaignRow;
use ts::DAY;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const WEEK: i64 = 7 * DAY;
const SESSION_GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const MAX_PRICE: u64 = 10_000_000;
const CHUNK: usize = 2 << 20;
/// I/O threads for the loaders read mode; they mostly wait on the disk.
const LOADERS: usize = 16;
/// Hash shards for event IDs and user IDs, and buckets for people.
const SHARDS: usize = 64;
const BUCKETS: usize = 256;
const NO_CATEGORY: u16 = u16::MAX;

/// Every log line, kept so the diagnostic mode can repeat them in its summary.
static LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn log(start: Instant, what: &str) {
    let line = format!("{:.3} {what}", start.elapsed().as_secs_f64());
    eprintln!("[{line}]");
    if let Ok(mut lines) = LOG.lock() {
        lines.push(line);
    }
}

/// The phase log since the last call, as one compact line.
fn take_log() -> String {
    let mut lines = LOG.lock().unwrap();
    let text = lines
        .iter()
        .map(|line| {
            let mut parts = line.splitn(3, ' ');
            let time = parts.next().unwrap_or("");
            let word = parts.next().unwrap_or("");
            format!("{word}={time}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    lines.clear();
    text
}

/// mimalloc options, by their index in mimalloc.h's `mi_option_e`.
const MI_OPTION_ALLOW_LARGE_OS_PAGES: libmimalloc_sys::mi_option_t = 6;
const MI_OPTION_PURGE_DELAY: libmimalloc_sys::mi_option_t = 15;

fn main() {
    let start = Instant::now();
    // Never hand freed memory back to the OS mid-run: re-faulting it cost as
    // much kernel time as the pipeline's own work. Large pages cut the rest.
    let allocator = std::env::var("SESSIONS_MI").unwrap_or_else(|_| "default".into());
    unsafe {
        if allocator.contains("keep") {
            libmimalloc_sys::mi_option_set(MI_OPTION_PURGE_DELAY, -1);
        }
        if allocator.contains("large") {
            libmimalloc_sys::mi_option_set(MI_OPTION_ALLOW_LARGE_OS_PAGES, 1);
        }
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    // SESSIONS_THREADS overrides the scorer's NCPU, for experiments such as
    // one thread per physical core.
    let threads = std::env::var("SESSIONS_THREADS")
        .or_else(|_| std::env::var("NCPU"))
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .ok();
    if std::env::var("SESSIONS_EXPERIMENT").as_deref() == Ok("sizes") {
        eprintln!(
            "EventCopy {} LiveEvent {} ValidClick {} PersonEvent {} PersonClick {}",
            std::mem::size_of::<EventCopy>(),
            std::mem::size_of::<LiveEvent>(),
            std::mem::size_of::<ValidClick>(),
            std::mem::size_of::<PersonEvent>(),
            std::mem::size_of::<PersonClick>()
        );
        return;
    }
    let diagnose = std::env::var("SESSIONS_DIAG").as_deref() == Ok("1");
    let first = diagnose.then(|| diag::before(&data, start));
    run(&data, &output, start);
    log(start, "done");
    if let Some(first) = first {
        let cold_log = take_log();
        diag::after(&data, &output, first, cold_log, run, take_log);
    }
    // Every output file is closed; skip freeing memory and unmapping files.
    std::process::exit(0);
}

/// Frees a large value on a background thread, off the critical path.
fn discard<T: Send + 'static>(value: T) {
    std::thread::spawn(move || drop(value));
}

// ---------------------------------------------------------------- packed text

/// A byte string inside a memory-mapped input file, packed into 8 bytes:
/// the address in the high 48 bits and the length in the low 16. A field of
/// 65,535 bytes or more goes to a side table instead. The maps live until the
/// process exits, so the bytes stay valid.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Text(u64);

static LONG_TEXT: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());
const LONG: u64 = 0xFFFF;

impl Text {
    #[inline]
    fn new(s: &[u8]) -> Text {
        let address = s.as_ptr() as u64;
        if s.len() < LONG as usize && address < (1 << 48) {
            Text(address << 16 | s.len() as u64)
        } else {
            let mut long = LONG_TEXT.lock().unwrap();
            long.push((s.as_ptr() as usize, s.len()));
            Text(((long.len() - 1) as u64) << 16 | LONG)
        }
    }

    #[inline]
    fn get(self) -> &'static [u8] {
        let len = self.0 & 0xFFFF;
        if len != LONG {
            unsafe { std::slice::from_raw_parts((self.0 >> 16) as *const u8, len as usize) }
        } else {
            let (address, len) = LONG_TEXT.lock().unwrap()[(self.0 >> 16) as usize];
            unsafe { std::slice::from_raw_parts(address as *const u8, len) }
        }
    }
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
    let (a, b) = if n > 16 {
        let mut at = 0;
        while n - at > 16 {
            let x = u64::from_le_bytes(s[at..at + 8].try_into().unwrap());
            let y = u64::from_le_bytes(s[at + 8..at + 16].try_into().unwrap());
            seed = fold(x ^ K1, y ^ seed);
            at += 16;
        }
        // The last 16 bytes, overlapping what the loop already mixed.
        (
            u64::from_le_bytes(s[n - 16..n - 8].try_into().unwrap()),
            u64::from_le_bytes(s[n - 8..].try_into().unwrap()),
        )
    } else if n >= 8 {
        (
            u64::from_le_bytes(s[..8].try_into().unwrap()),
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
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|name| name.ends_with(".csv") && !name.starts_with('.'))
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

/// How input pages are brought in: "willneed" (ask the kernel to read every
/// file at once), "loaders" (I/O threads read each piece just ahead of the
/// parsers, many reads in flight), "both", or "none".
fn read_mode() -> &'static str {
    static MODE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    MODE.get_or_init(|| std::env::var("SESSIONS_READ").unwrap_or_else(|_| "willneed".into()))
}

fn map(path: &Path) -> Option<Mmap> {
    let file = File::open(path).ok()?;
    if file.metadata().ok()?.len() == 0 {
        return None;
    }
    let map = unsafe { Mmap::map(&file) }.ok()?;
    if matches!(read_mode(), "willneed" | "both") {
        let _ = map.advise(memmap2::Advice::WillNeed);
    } else {
        let _ = map.advise(memmap2::Advice::Sequential);
    }
    Some(map)
}

fn map_all(files: &[PathBuf]) -> Vec<Mmap> {
    files.par_iter().filter_map(|path| map(path)).collect()
}

/// Which table a piece of input belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Event,
    Click,
    Link,
}

/// A slice of one input file: the lines whose first byte lies in
/// `from..to`. Each parse task finds its own line boundaries, so nothing
/// touches the files before the parsers and loaders start.
#[derive(Clone, Copy)]
struct Piece {
    kind: Kind,
    map: usize,
    from: usize,
    to: usize,
}

fn pieces(maps: &[Mmap], kind: Kind, out: &mut Vec<Piece>) {
    for (at, map) in maps.iter().enumerate() {
        let mut from = 0;
        while from < map.len() {
            let to = (from + CHUNK).min(map.len());
            out.push(Piece {
                kind,
                map: at,
                from,
                to,
            });
            from = to;
        }
    }
}

/// The bytes of the lines whose first byte is in `from..to`, without the
/// file's header line. Neighbouring pieces split at the same line starts.
fn lines_of(data: &[u8], from: usize, to: usize) -> &[u8] {
    // The first line start at or after `at`.
    let line_start = |at: usize| -> usize {
        if at == 0 {
            0
        } else if at >= data.len() {
            data.len()
        } else {
            match memchr::memchr(b'\n', &data[at - 1..]) {
                Some(offset) => at + offset,
                None => data.len(),
            }
        }
    };
    let mut start = line_start(from);
    let end = line_start(to);
    if start == 0 {
        start = memchr::memchr(b'\n', data).map_or(data.len(), |at| at + 1);
    }
    if start >= end {
        &data[..0]
    } else {
        &data[start..end]
    }
}

/// Starts I/O threads that read the pieces into the page cache in order,
/// many at once, just ahead of the parsers that take them in the same order.
fn start_loaders(blocks: Vec<(usize, usize)>) {
    if !matches!(read_mode(), "loaders" | "both") || blocks.is_empty() {
        return;
    }
    let blocks = std::sync::Arc::new(blocks);
    let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..LOADERS {
        let blocks = blocks.clone();
        let next = next.clone();
        std::thread::spawn(move || {
            loop {
                let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(&(address, len)) = blocks.get(at) else {
                    break;
                };
                let start = address & !4095;
                let end = address + len;
                // MADV_POPULATE_READ (Linux 5.14+); on older kernels, touch
                // one byte per page instead.
                let done =
                    unsafe { libc::madvise(start as *mut libc::c_void, end - start, 22) == 0 };
                if !done {
                    let mut page = address;
                    while page < end {
                        unsafe { std::ptr::read_volatile(page as *const u8) };
                        page = (page & !4095) + 4096;
                    }
                }
            }
        });
    }
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
        assert!(prefix.len() == 1 || prefix.len() == 2);
        Self {
            prefix,
            by_number: Vec::new(),
            by_bytes: FxHashMap::default(),
        }
    }

    /// `n` for an ID that is exactly the prefix and 1 to 7 digits without a
    /// leading zero. Prefixes are 1 or 2 bytes.
    #[inline]
    fn number(&self, id: &[u8]) -> Option<usize> {
        let p = self.prefix.len();
        let n = id.len();
        if n <= p || n > p + 7 || id[0] != self.prefix[0] || (p == 2 && id[1] != self.prefix[1]) {
            return None;
        }
        let digits = &id[p..];
        let mut value = 0usize;
        let mut bad = digits[0] == b'0';
        for &b in digits {
            let digit = b.wrapping_sub(b'0');
            bad |= digit > 9;
            value = value * 10 + usize::from(digit);
        }
        if bad { None } else { Some(value) }
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

/// Price (low 24 bits; at most 10,000,000) and category (high 8 bits).
/// Category 255 marks an invalid event copy.
const INVALID: u32 = 0xFF << 24;

#[inline]
fn pack(price: u32, category: u16) -> u32 {
    price | u32::from(category) << 24
}

/// One parsed copy of an event, before dedup (48 bytes). `ts` is
/// milliseconds after the window start, which a 14-day window fits easily.
#[derive(Clone, Copy)]
struct EventCopy {
    event_id: Key,
    user_id: Key,
    ingest: i64,
    ts: u32,
    price_category: u32,
}

/// A kept, valid event, before its user ID is interned (24 bytes).
#[derive(Clone, Copy)]
struct LiveEvent {
    user_id: Key,
    ts: u32,
    price_category: u32,
}

#[derive(Clone, Copy)]
struct ValidClick {
    user_id: Key,
    click_id: Key,
    ts: i64,
    campaign: u16,
}

/// An event whose user ID is now a number (an ID, then a person).
#[derive(Clone, Copy)]
struct PersonEvent {
    person: u32,
    /// Milliseconds after the window start.
    ts: u32,
    price_category: u32,
}

impl PersonEvent {
    #[inline]
    fn price(&self) -> u32 {
        self.price_category & 0xFF_FFFF
    }

    #[inline]
    fn category(&self) -> u16 {
        (self.price_category >> 24) as u16
    }
}

#[derive(Clone, Copy)]
struct PersonClick {
    person: u32,
    campaign: u16,
    ts: i64,
    click_id: Key,
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

/// Parses an event line; returns the copy and its event-ID hash.
#[inline]
fn parse_event(fields: [&[u8]; 7], context: &Context, clock: &mut ts::Parser) -> Option<EventCopy> {
    let [
        event_id,
        user_id,
        ts_field,
        ingest_field,
        kind,
        product,
        price,
    ] = fields;
    if event_id.is_empty() {
        return None;
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
    // Only a purchase's category is ever used (top_category sums purchase
    // prices), so views and carts skip the product lookup.
    let price_category = if !valid {
        INVALID
    } else if price_cents > 0 {
        pack(
            price_cents,
            context
                .products
                .get(product)
                .unwrap_or(context.unknown_category),
        )
    } else {
        pack(0, 0)
    };
    Some(EventCopy {
        event_id: Key::new(event_id),
        user_id: Key::new(user_id),
        ingest,
        ts: (ts - context.window_start) as u32,
        price_category,
    })
}

#[inline]
fn parse_click(
    fields: [&[u8]; 4],
    context: &Context,
    clock: &mut ts::Parser,
) -> Option<ValidClick> {
    let [click_id, user_id, campaign, ts_field] = fields;
    if click_id.is_empty() || user_id.is_empty() {
        return None;
    }
    let campaign = context.campaigns.get(campaign)?;
    let ts = clock.parse(ts_field)?;
    if ts < context.window_start - WEEK || ts >= context.window_end {
        return None;
    }
    let user_id = Key::new(user_id);
    Some(ValidClick {
        user_id,
        click_id: Key::new(click_id),
        ts,
        campaign,
    })
}

/// The root of `x`, halving the path as it goes. Concurrent finds and
/// unions are safe: every write moves a node to one of its ancestors.
fn find_atomic(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let up = parent[x as usize].load(Ordering::Relaxed);
        if up == x {
            return x;
        }
        let grand = parent[up as usize].load(Ordering::Relaxed);
        if grand != up {
            let _ = parent[x as usize].compare_exchange_weak(
                up,
                grand,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        x = grand;
    }
}

/// Joins the sets of `a` and `b`, linking the larger root under the smaller.
fn union_atomic(parent: &[AtomicU32], a: u32, b: u32) {
    loop {
        let (ra, rb) = (find_atomic(parent, a), find_atomic(parent, b));
        if ra == rb {
            return;
        }
        let (high, low) = if ra > rb { (ra, rb) } else { (rb, ra) };
        if parent[high as usize]
            .compare_exchange(high, low, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}


/// `shards` empty Vecs, each with room for `each` items.
fn shard_vecs<T>(shards: usize, each: usize) -> Vec<Vec<T>> {
    (0..shards).map(|_| Vec::with_capacity(each)).collect()
}

/// Numbers the user IDs of one shard. Entries hold the key itself, so a
/// probe never reads `names`.
struct Interner {
    names: Vec<Key>,
    table: HashTable<(Key, u32)>,
}

impl Interner {
    #[inline]
    fn intern(&mut self, id: Key, hash: u64) -> u32 {
        let Self { names, table } = self;
        match table.entry(hash, |entry| entry.0 == id, |entry| entry.0.hash()) {
            Entry::Occupied(entry) => entry.get().1,
            Entry::Vacant(entry) => {
                let at = names.len() as u32;
                entry.insert((id, at));
                names.push(id);
                at
            }
        }
    }
}

/// One user shard's interned IDs and rows.
struct UserShard {
    names: Vec<Key>,
    /// (link side as link * 2 + side, local ID)
    link_sides: Vec<(u32, u32)>,
    events: Vec<PersonEvent>,
    clicks: Vec<PersonClick>,
}

/// Per-thread results of the session pass.
struct Tally {
    /// (name, day, sessions, events, revenue, top category) per person-day.
    users: Vec<(u64, i32, u32, u32, u64, u16)>,
    sums: Vec<(u16, u64)>,
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
    // Rows pack the category into 8 bits (255 marks an invalid copy) and the
    // event time into 32 bits after the window start.
    assert!(
        category_names.len() <= 255,
        "more than 254 categories in products.csv"
    );
    assert!(
        window_end - window_start <= i64::from(u32::MAX),
        "the event window is longer than 49 days"
    );
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
    let mut work: Vec<Piece> = Vec::new();
    pieces(&event_maps, Kind::Event, &mut work);
    pieces(&click_maps, Kind::Click, &mut work);
    pieces(&link_maps, Kind::Link, &mut work);
    let bytes_of = |piece: &Piece| -> &[u8] {
        match piece.kind {
            Kind::Event => &event_maps[piece.map],
            Kind::Click => &click_maps[piece.map],
            Kind::Link => &link_maps[piece.map],
        }
    };
    start_loaders(
        work.iter()
            .map(|piece| {
                (
                    bytes_of(piece).as_ptr() as usize + piece.from,
                    piece.to - piece.from,
                )
            })
            .collect(),
    );
    log(start, "mapped");

    // 1. Parse the pieces in order from a shared queue, straight into shards:
    //    events by event ID, clicks by user ID.
    type Parsed<'m> = (
        Vec<Vec<Vec<EventCopy>>>,
        Vec<Vec<Vec<ValidClick>>>,
        Vec<[&'m [u8]; 2]>,
    );
    let next = std::sync::atomic::AtomicUsize::new(0);
    let workers = rayon::current_num_threads();
    let parsed: Vec<Parsed> = (0..workers)
        .into_par_iter()
        .map(|_| {
            let (mut events, mut clicks, mut links): Parsed = (Vec::new(), Vec::new(), Vec::new());
            let mut clock = ts::Parser::new();
            loop {
                let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(piece) = work.get(at) else {
                    break;
                };
                let chunk = lines_of(bytes_of(piece), piece.from, piece.to);
                match piece.kind {
                    Kind::Event => {
                        let mut shards: Vec<Vec<EventCopy>> =
                            shard_vecs(SHARDS, chunk.len() / 70 / SHARDS * 5 / 4 + 64);
                        csv::for_records::<7>(chunk, |fields| {
                            if let Some(event) = parse_event(fields, &context, &mut clock) {
                                shards[shard(event.event_id.hash())].push(event);
                            }
                        });
                        events.push(shards);
                    }
                    Kind::Click => {
                        let mut shards: Vec<Vec<ValidClick>> =
                            shard_vecs(SHARDS, chunk.len() / 45 / SHARDS * 5 / 4 + 64);
                        csv::for_records::<4>(chunk, |fields| {
                            if let Some(click) = parse_click(fields, &context, &mut clock) {
                                shards[shard(click.user_id.hash())].push(click);
                            }
                        });
                        clicks.push(shards);
                    }
                    Kind::Link => {
                        csv::for_records::<2>(chunk, |[a, b]| {
                            if !a.is_empty() && !b.is_empty() {
                                links.push([a, b]);
                            }
                        });
                    }
                }
            }
            (events, clicks, links)
        })
        .collect();
    let mut events = Vec::new();
    let mut clicks = Vec::new();
    let mut links = Vec::new();
    for (mut e, mut c, mut l) in parsed {
        events.append(&mut e);
        clicks.append(&mut c);
        links.append(&mut l);
    }
    log(start, "parsed");

    // 2. Dedup events per shard, reading each chunk's part in place; keep
    //    valid survivors, sharded by user ID.
    // Losing copies are marked INVALID in place, then survivors are read in
    // file order, so reading each user ID back from the input streams forward.
    struct Rows(*mut Vec<Vec<EventCopy>>);
    unsafe impl Sync for Rows {}
    unsafe impl Send for Rows {}
    let mut events = events;
    let chunk_count = events.len();
    let rows = Rows(events.as_mut_ptr());
    let rows = &rows;
    let live: Vec<usize> = (0..SHARDS)
        .into_par_iter()
        .map(|at| {
            // Every chunk's part `at` belongs to this thread alone.
            let part = |chunk: usize| unsafe { &mut (&mut *rows.0.add(chunk))[at] };
            let total: usize = (0..chunk_count).map(|chunk| part(chunk).len()).sum();
            // Sized up front, so the table never grows and never rehashes.
            // Entries hold the key itself, so a probe never follows a pointer.
            let mut table: HashTable<(Key, *mut EventCopy)> = HashTable::with_capacity(total);
            for chunk in 0..chunk_count {
                let copies = part(chunk);
                let base = copies.as_mut_ptr();
                for index in 0..copies.len() {
                    let copy = unsafe { base.add(index) };
                    let id = unsafe { (*copy).event_id };
                    match table.entry(id.hash(), |kept| kept.0 == id, |kept| kept.0.hash()) {
                        Entry::Occupied(mut entry) => unsafe {
                            let kept = entry.get().1;
                            if (*copy).ingest < (*kept).ingest {
                                (*kept).price_category = INVALID;
                                entry.get_mut().1 = copy;
                            } else {
                                // Equal ingest_ts means identical copies.
                                (*copy).price_category = INVALID;
                            }
                        },
                        Entry::Vacant(entry) => {
                            entry.insert((id, copy));
                        }
                    }
                }
            }
            drop(table);
            total
        })
        .collect::<Vec<usize>>();
    log(start, "dedup tables");
    let live: Vec<Vec<Vec<LiveEvent>>> = live
        .into_par_iter()
        .enumerate()
        .map(|(at, total)| {
            let part = |chunk: usize| unsafe { &mut (&mut *rows.0.add(chunk))[at] };
            let mut out: Vec<Vec<LiveEvent>> = shard_vecs(SHARDS, total / SHARDS * 5 / 4 + 64);
            for chunk in 0..chunk_count {
                for kept in part(chunk).iter() {
                    if kept.price_category != INVALID {
                        let user_id = kept.user_id;
                        out[shard(user_id.hash())].push(LiveEvent {
                            user_id,
                            ts: kept.ts,
                            price_category: kept.price_category,
                        });
                    }
                }
            }
            out
        })
        .collect();
    discard(events);
    log(start, "deduplicated");

    // 3. Intern user IDs per shard, reading every producer's part in place.
    // Link sides, sharded by ID in parallel blocks: (link * 2 + side, ID, hash).
    const LINK_BLOCK: usize = 1 << 16;
    let link_parts: Vec<Vec<Vec<(u32, Key, u64)>>> = links
        .par_chunks(LINK_BLOCK)
        .enumerate()
        .map(|(block, rows)| {
            let mut out: Vec<Vec<(u32, Key, u64)>> =
                shard_vecs(SHARDS, rows.len() * 2 / SHARDS * 5 / 4 + 16);
            for (offset, [a, b]) in rows.iter().enumerate() {
                let at = block * LINK_BLOCK + offset;
                for (side, id) in [a, b].into_iter().enumerate() {
                    let id = Key::new(id);
                    let hash = id.hash();
                    out[shard(hash)].push(((at * 2 + side) as u32, id, hash));
                }
            }
            out
        })
        .collect();
    let mut users: Vec<UserShard> = (0..SHARDS)
        .into_par_iter()
        .map(|at| {
            let event_count: usize = live.iter().map(|part| part[at].len()).sum();
            let click_count: usize = clicks.iter().map(|part| part[at].len()).sum();
            let side_count: usize = link_parts.iter().map(|part| part[at].len()).sum();
            let mut ids = Interner {
                names: Vec::new(),
                table: HashTable::with_capacity((event_count + click_count) / 8 + side_count + 64),
            };
            let link_sides: Vec<(u32, u32)> = link_parts
                .iter()
                .flat_map(|part| part[at].iter())
                .map(|&(code, id, hash)| (code, ids.intern(id, hash)))
                .collect();
            let mut events = Vec::with_capacity(event_count);
            for part in &live {
                for event in &part[at] {
                    events.push(PersonEvent {
                        person: ids.intern(event.user_id, event.user_id.hash()),
                        ts: event.ts,
                        price_category: event.price_category,
                    });
                }
            }
            let mut person_clicks = Vec::with_capacity(click_count);
            for part in &clicks {
                for click in &part[at] {
                    person_clicks.push(PersonClick {
                        person: ids.intern(click.user_id, click.user_id.hash()),
                        campaign: click.campaign,
                        ts: click.ts,
                        click_id: click.click_id,
                    });
                }
            }
            UserShard {
                names: ids.names,
                link_sides,
                events,
                clicks: person_clicks,
            }
        })
        .collect();
    discard(live);
    discard(clicks);
    let mut base = Vec::with_capacity(SHARDS);
    let mut count = 0usize;
    for user_shard in &users {
        base.push(count as u32);
        count += user_shard.names.len();
    }
    let names: Vec<Key> = users
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
    // Concurrent union-find: a root's parent only ever moves to a smaller
    // index, so the forest stays acyclic whatever the interleaving.
    let parent: Vec<AtomicU32> = (0..count as u32).map(AtomicU32::new).collect();
    link_ids.par_chunks(2 * 4096).for_each(|pairs| {
        for pair in pairs.chunks_exact(2) {
            union_atomic(&parent, pair[0], pair[1]);
        }
    });
    // Each person's name: the byte-wise smallest ID among its members.
    let root: Vec<u32> = (0..count as u32)
        .into_par_iter()
        .map(|id| find_atomic(&parent, id))
        .collect();
    let smallest: Vec<AtomicU32> = (0..count as u32).map(AtomicU32::new).collect();
    (0..count as u32).into_par_iter().for_each(|id| {
        let top = root[id as usize];
        if top == id {
            return;
        }
        let slot = &smallest[top as usize];
        let mut current = slot.load(Ordering::Relaxed);
        while names[id as usize] < names[current as usize] {
            match slot.compare_exchange_weak(current, id, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(seen) => current = seen,
            }
        }
    });
    let name_of = |id: u32| smallest[root[id as usize] as usize].load(Ordering::Relaxed);
    // Number each person within their bucket, so the session phase indexes
    // small bucket-local arrays. person[id] = bucket << 24 | local index;
    // bucket_people[bucket][local] is the person's name ID. A person is an ID
    // that is its own name; ranges count them per bucket in parallel, then
    // each range fills its share of every bucket.
    let range = count.div_ceil(rayon::current_num_threads() * 4).max(1);
    let counts: Vec<Vec<u32>> = (0..count)
        .into_par_iter()
        .step_by(range)
        .map(|from| {
            let mut counts = vec![0u32; BUCKETS];
            for id in from as u32..(from + range).min(count) as u32 {
                if name_of(id) == id {
                    counts[bucket(id)] += 1;
                }
            }
            counts
        })
        .collect();
    let mut offsets = vec![vec![0u32; BUCKETS]; counts.len()];
    let mut bucket_sizes = vec![0u32; BUCKETS];
    for at in 0..BUCKETS {
        for (part, part_counts) in counts.iter().enumerate() {
            offsets[part][at] = bucket_sizes[at];
            bucket_sizes[at] += part_counts[at];
        }
    }
    let mut bucket_people: Vec<Vec<u32>> = bucket_sizes
        .iter()
        .map(|&size| vec![0u32; size as usize])
        .collect();
    let mut local = vec![0u32; count];
    {
        struct Shared(*mut u32);
        unsafe impl Sync for Shared {}
        unsafe impl Send for Shared {}
        let local_out = Shared(local.as_mut_ptr());
        let people_out: Vec<Shared> = bucket_people.iter_mut().map(|v| Shared(v.as_mut_ptr())).collect();
        let (local_out, people_out) = (&local_out, &people_out);
        offsets.into_par_iter().enumerate().for_each(|(part, mut next)| {
            let from = part * range;
            for id in from as u32..(from + range).min(count) as u32 {
                if name_of(id) == id {
                    let at = bucket(id);
                    // Each range writes its own slots of each bucket, and
                    // each person ID once.
                    unsafe {
                        *people_out[at].0.add(next[at] as usize) = id;
                        *local_out.0.add(id as usize) = next[at];
                    }
                    next[at] += 1;
                }
            }
        });
    }
    let person: Vec<u32> = (0..count as u32)
        .into_par_iter()
        .map(|id| {
            let name = name_of(id);
            (bucket(name) as u32) << 24 | local[name as usize]
        })
        .collect();
    assert!(
        bucket_people.iter().all(|people| people.len() < 1 << 24),
        "too many people in one bucket"
    );
    log(start, &format!("linked {} pairs", links.len()));

    // 5. Regroup rows by person into buckets.
    let regrouped: Vec<(Vec<Vec<PersonEvent>>, Vec<Vec<PersonClick>>)> = users
        .par_iter_mut()
        .zip(base.par_iter())
        .map(|(user_shard, &offset)| {
            let mut events: Vec<Vec<PersonEvent>> =
                shard_vecs(BUCKETS, user_shard.events.len() / BUCKETS * 3 / 2 + 32);
            for mut event in std::mem::take(&mut user_shard.events) {
                let code = person[(offset + event.person) as usize];
                event.person = code & 0xFF_FFFF;
                events[(code >> 24) as usize].push(event);
            }
            let mut clicks: Vec<Vec<PersonClick>> =
                shard_vecs(BUCKETS, user_shard.clicks.len() / BUCKETS * 3 / 2 + 32);
            for mut click in std::mem::take(&mut user_shard.clicks) {
                let code = person[(offset + click.person) as usize];
                click.person = code & 0xFF_FFFF;
                clicks[(code >> 24) as usize].push(click);
            }
            (events, clicks)
        })
        .collect();
    log(start, "regrouped");

    // 6. Sessions, credit and assists, per bucket.
    let first_day = window_start.div_euclid(DAY);
    let days = ((window_end - 1).div_euclid(DAY) - first_day + 1).max(1) as usize;
    let campaign_slots = organic + 1;
    let empty_tally = || Tally {
        users: Vec::new(),
        sums: Vec::new(),
        campaigns: vec![[0i64; 4]; campaign_slots * days],
        stamp: vec![u64::MAX; campaign_slots],
        session: 0,
    };
    // One tally per worker, each pulling buckets from a shared queue: far
    // fewer campaign tables to zero and merge than one per rayon split.
    let next_bucket = std::sync::atomic::AtomicUsize::new(0);
    let tally = (0..rayon::current_num_threads())
        .into_par_iter()
        .map(|_| {
            let mut tally = empty_tally();
            loop {
                let at = next_bucket.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if at >= BUCKETS {
                    break;
                }
                // Rows carry bucket-local person numbers: count, then place each
                // row in its person's range, with small arrays that stay in cache.
                let people = bucket_people[at].len();
                let mut starts = vec![0usize; people + 1];
                for (part, _) in &regrouped {
                    for event in &part[at] {
                        starts[event.person as usize + 1] += 1;
                    }
                }
                for index in 0..people {
                    starts[index + 1] += starts[index];
                }
                let total = starts[people];
                let mut events: Vec<PersonEvent> = Vec::with_capacity(total);
                let mut cursor = starts.clone();
                unsafe {
                    let target = events.as_mut_ptr();
                    for (part, _) in &regrouped {
                        for event in &part[at] {
                            let index = event.person as usize;
                            target.add(cursor[index]).write(*event);
                            cursor[index] += 1;
                        }
                    }
                    events.set_len(total);
                }
                let mut click_starts = vec![0usize; people + 1];
                for (_, part) in &regrouped {
                    for click in &part[at] {
                        click_starts[click.person as usize + 1] += 1;
                    }
                }
                for index in 0..people {
                    click_starts[index + 1] += click_starts[index];
                }
                let click_total = click_starts[people];
                let mut clicks: Vec<PersonClick> = Vec::with_capacity(click_total);
                let mut cursor = click_starts.clone();
                unsafe {
                    let target = clicks.as_mut_ptr();
                    for (_, part) in &regrouped {
                        for click in &part[at] {
                            let index = click.person as usize;
                            target.add(cursor[index]).write(*click);
                            cursor[index] += 1;
                        }
                    }
                    clicks.set_len(click_total);
                }
                // Each person's few events by ts, and clicks by (ts, click_id).
                for index in 0..people {
                    if starts[index] == starts[index + 1] {
                        // A person with only clicks or links has no sessions.
                        continue;
                    }
                    let mine = &mut events[starts[index]..starts[index + 1]];
                    mine.sort_unstable_by_key(|e| e.ts);
                    let their_clicks = &mut clicks[click_starts[index]..click_starts[index + 1]];
                    their_clicks
                        .sort_unstable_by(|a, b| (a.ts, a.click_id).cmp(&(b.ts, b.click_id)));
                    sessionize(
                        mine,
                        their_clicks,
                        &mut tally,
                        first_day,
                        days,
                        organic,
                        &category_rank,
                        window_start,
                        u64::from(bucket_people[at][index]),
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
    discard(regrouped);
    log(start, &format!("sessions: {}", tally.session));

    // 7. Write both tables.
    let write_name = |who: u64, buffer: &mut Vec<u8>| {
        names[who as usize].with_bytes(|bytes| buffer.extend_from_slice(bytes))
    };
    let tables = Tables {
        output,
        category_names: &category_names,
        campaign_ids: &campaign_ids,
        campaign_channels: &campaign_channels,
        organic,
        first_day,
        days,
    };
    write_tables(&tables, &tally, write_name, start);
    // The maps must outlive every Text; main exits without unmapping them.
    // The process exits right after: freeing millions of small buffers here
    // took one thread ~65 ms at 20M, so everything large is left in place.
    // The maps must outlive every Text anyway.
    std::mem::forget((tally, names, users));
    std::mem::forget((person, local, bucket_people, smallest, parent, link_ids, links));
    std::mem::forget((event_maps, click_maps, link_maps));
}

/// What both pipelines need to write the output tables.
struct Tables<'a> {
    output: &'a Path,
    category_names: &'a [&'a [u8]],
    campaign_ids: &'a [&'a [u8]],
    campaign_channels: &'a [&'a [u8]],
    organic: usize,
    first_day: i64,
    days: usize,
}

/// Writes user_daily (several files in parallel) and campaign_daily.
fn write_tables(
    tables: &Tables,
    tally: &Tally,
    write_name: impl Fn(u64, &mut Vec<u8>) + Sync + Copy,
    start: Instant,
) {
    let Tables {
        output,
        category_names,
        campaign_ids,
        campaign_channels,
        organic,
        first_day,
        days,
    } = *tables;
    let user_rows: &[out::UserRow] = &tally.users;
    let mut campaign_rows = Vec::new();
    for campaign in 0..=organic {
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
    // Any number of Parquet files is fine, so the user table is written as
    // several files in parallel.
    let user_dir = output.join("user_daily");
    let per_file = user_rows.len().div_ceil(8).max(1);
    rayon::join(
        || {
            user_rows
                .par_chunks(per_file)
                .enumerate()
                .for_each(|(part, rows)| {
                    out::write_users(&user_dir, part, rows, write_name, category_names, NO_CATEGORY)
                });
            if user_rows.is_empty() {
                out::write_users(&user_dir, 0, &[], write_name, category_names, NO_CATEGORY);
            }
        },
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
    std::mem::forget(campaign_rows);
}

/// What the session pass needs from a click: its person (as numbered in the
/// slice), time and campaign.
trait SessionClick {
    fn person(&self) -> u32;
    fn ts(&self) -> i64;
    fn campaign(&self) -> u16;
}

impl SessionClick for PersonClick {
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

/// Walks one bucket's events (sorted by person, ts) and clicks (sorted by
/// person, ts, click_id) into sessions, user-day rows and campaign tallies.
fn sessionize<C: SessionClick>(
    events: &[PersonEvent],
    clicks: &[C],
    tally: &mut Tally,
    first_day: i64,
    days: usize,
    organic: usize,
    category_rank: &[u16],
    window_start: i64,
    name: u64,
) {
    let mut sums = std::mem::take(&mut tally.sums);
    let mut click_at = 0;
    let mut at = 0;
    while at < events.len() {
        let who = events[at].person;
        let mut end = at + 1;
        while end < events.len() && events[end].person == who {
            end += 1;
        }
        while click_at < clicks.len() && clicks[click_at].person() < who {
            click_at += 1;
        }
        let mut click_end = click_at;
        while click_end < clicks.len() && clicks[click_end].person() == who {
            click_end += 1;
        }
        let mine = &clicks[click_at..click_end];
        click_at = click_end;

        // The current user-day row: day, sessions, events, revenue.
        let mut row: Option<(i64, u32, u32, u64)> = None;
        sums.clear();
        let mut s = at;
        while s < end {
            let first = events[s].ts;
            let session_start = window_start + i64::from(first);
            let mut e = s + 1;
            let mut previous = first;
            while e < end && i64::from(events[e].ts - previous) <= SESSION_GAP {
                previous = events[e].ts;
                e += 1;
            }
            let day = session_start.div_euclid(DAY);
            if let Some(r) = row
                && r.0 != day
            {
                emit(tally, name, r, &mut sums, category_rank);
                row = None;
            }
            let mut revenue: u64 = 0;
            for event in &events[s..e] {
                if event.price() > 0 {
                    let price = u64::from(event.price());
                    revenue += price;
                    match sums.iter_mut().find(|(c, _)| *c == event.category()) {
                        Some(entry) => entry.1 += price,
                        None => sums.push((event.category(), price)),
                    }
                }
            }
            let r = row.get_or_insert((day, 0, 0, 0));
            r.1 += 1;
            r.2 += (e - s) as u32;
            r.3 += revenue;

            // Credit and assists.
            let day_slot = (day - first_day) as usize;
            let hi = mine.partition_point(|c| c.ts() <= session_start);
            let lo = mine[..hi].partition_point(|c| c.ts() < session_start - WEEK);
            let credited = if hi > lo {
                let latest = mine[hi - 1].ts();
                let first = lo + mine[lo..hi].partition_point(|c| c.ts() < latest);
                usize::from(mine[first].campaign())
            } else {
                organic
            };
            let cell = &mut tally.campaigns[credited * days + day_slot];
            cell[0] += 1;
            cell[1] += i64::from(revenue > 0);
            cell[2] += revenue as i64;
            let session = tally.session;
            for click in &mine[lo..hi] {
                let campaign = usize::from(click.campaign());
                if tally.stamp[campaign] != session {
                    tally.stamp[campaign] = session;
                    tally.campaigns[campaign * days + day_slot][3] += 1;
                }
            }
            tally.session += 1;
            s = e;
        }
        if let Some(r) = row {
            emit(tally, name, r, &mut sums, category_rank);
        }
        at = end;
    }
    tally.sums = sums;
}

#[inline]
fn emit(
    tally: &mut Tally,
    who: u64,
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
