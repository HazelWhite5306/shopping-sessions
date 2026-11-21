//! A diagnostic mode (SESSIONS_DIAG=1) for learning about the scoring
//! machine. It runs the pipeline cold and warm, measures read speeds, then
//! prints a short summary as the very last lines of stderr (the scorer shows
//! only the end of the log) and exits with status 1 on purpose so the log is
//! shown. Never set it for a real submission.

use std::fs::{self, File};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;

fn files(data: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for folder in ["events", "clicks", "links"] {
        if let Ok(entries) = fs::read_dir(data.join(folder)) {
            for entry in entries.flatten() {
                out.push(entry.path());
            }
        }
    }
    out.push(data.join("products.csv"));
    out.push(data.join("campaigns.csv"));
    out
}

/// Bytes of `path` resident in the page cache, and its size.
fn resident(path: &Path) -> (u64, u64) {
    let Ok(file) = File::open(path) else {
        return (0, 0);
    };
    let size = file.metadata().map(|m| m.len()).unwrap_or(0);
    if size == 0 {
        return (0, 0);
    }
    unsafe {
        let address = libc::mmap(
            std::ptr::null_mut(),
            size as usize,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        if address == libc::MAP_FAILED {
            return (0, size);
        }
        let pages = (size as usize).div_ceil(4096);
        let mut vector = vec![0u8; pages];
        let ok = libc::mincore(address, size as usize, vector.as_mut_ptr()) == 0;
        libc::munmap(address, size as usize);
        if !ok {
            return (0, size);
        }
        let cached = vector.iter().filter(|&&page| page & 1 == 1).count() as u64;
        ((cached * 4096).min(size), size)
    }
}

fn cached_percent(data: &Path) -> (f64, f64) {
    let (cached, total) = files(data)
        .par_iter()
        .map(|path| resident(path))
        .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    (100.0 * cached as f64 / total.max(1) as f64, total as f64 / 1e6)
}

/// Reads every input file with parallel 1 MB reads (through the page cache).
fn read_all(data: &Path) -> f64 {
    let start = Instant::now();
    let bytes: u64 = files(data)
        .par_iter()
        .map(|path| {
            use std::io::Read;
            let Ok(mut file) = File::open(path) else {
                return 0;
            };
            let mut buffer = vec![0u8; 1 << 20];
            let mut total = 0u64;
            while let Ok(n) = file.read(&mut buffer) {
                if n == 0 {
                    break;
                }
                total += n as u64;
            }
            total
        })
        .sum();
    bytes as f64 / start.elapsed().as_secs_f64() / 1e9
}

/// Reads every input file with O_DIRECT, bypassing the page cache.
fn read_direct(data: &Path) -> f64 {
    use std::os::unix::fs::OpenOptionsExt;
    let start = Instant::now();
    let bytes: u64 = files(data)
        .par_iter()
        .map(|path| {
            let Ok(file) = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
            else {
                return 0;
            };
            let layout = std::alloc::Layout::from_size_align(4 << 20, 4096).unwrap();
            let buffer = unsafe { std::alloc::alloc(layout) };
            let mut total = 0u64;
            loop {
                let n = unsafe {
                    libc::read(file.as_raw_fd(), buffer as *mut libc::c_void, 4 << 20)
                };
                if n <= 0 {
                    break;
                }
                total += n as u64;
            }
            unsafe { std::alloc::dealloc(buffer, layout) };
            total
        })
        .sum();
    bytes as f64 / start.elapsed().as_secs_f64() / 1e9
}

fn first_line(path: &str, prefix: &str) -> String {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with(prefix))
        .unwrap_or("?")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn mount_of(point: &str) -> String {
    fs::read_to_string("/proc/mounts")
        .unwrap_or_default()
        .lines()
        .find(|line| line.split_whitespace().nth(1) == Some(point))
        .map(|line| line.split_whitespace().take(4).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|| "?".into())
}

pub struct Before {
    cached: f64,
    megabytes: f64,
    startup: f64,
}

/// Before the pipeline: how much input is already cached.
pub fn before(data: &Path, start: Instant) -> Before {
    let (cached, megabytes) = cached_percent(data);
    Before {
        cached,
        megabytes,
        startup: start.elapsed().as_secs_f64(),
    }
}

/// After the pipeline: a warm rerun, read speeds, the machine, then the
/// summary as the last lines. Exits 1.
pub fn after(
    data: &Path,
    output: &Path,
    first: Before,
    cold_log: String,
    run: impl Fn(&Path, &Path, Instant),
    take_log: impl Fn() -> String,
) -> ! {
    let (cached_after, _) = cached_percent(data);
    let warm = Instant::now();
    let _ = fs::remove_dir_all(output.join("user_daily"));
    let _ = fs::remove_dir_all(output.join("campaign_daily"));
    run(data, output, warm);
    let warm_log = take_log();
    let warm_read = read_all(data);
    let direct_read = read_direct(data);

    let cpus = std::thread::available_parallelism().map_or(0, usize::from);
    let summary = [
        format!(
            "input {:.0} MB; in page cache before first run {:.1}%, after {:.1}%",
            first.megabytes, first.cached, cached_after
        ),
        format!("diag setup took {:.3}s", first.startup),
        format!("FIRST RUN: {cold_log}"),
        format!("WARM RUN: {warm_log}"),
        format!("read speed: warm {warm_read:.2} GB/s, O_DIRECT disk {direct_read:.2} GB/s"),
        format!(
            "cpus available {cpus}; {}; {}",
            first_line("/proc/self/status", "Cpus_allowed_list"),
            first_line("/proc/cpuinfo", "model name")
        ),
        format!(
            "{}; {}; THP {}",
            first_line("/proc/meminfo", "MemTotal"),
            first_line("/proc/meminfo", "MemAvailable"),
            fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
                .unwrap_or_default()
                .trim()
        ),
        format!("mount /data: {}", mount_of("/data")),
        format!(
            "kernel {}",
            fs::read_to_string("/proc/version")
                .unwrap_or_default()
                .split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        ),
    ];
    eprintln!("==== DIAG SUMMARY ====");
    for line in summary {
        eprintln!("[diag] {line}");
    }
    eprintln!("[diag] exiting 1 on purpose so the log is shown");
    std::process::exit(1);
}
