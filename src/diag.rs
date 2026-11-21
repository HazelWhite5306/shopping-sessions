//! A diagnostic mode (SESSIONS_DIAG=1) for learning about the scoring
//! machine: whether the input is already in the page cache, how fast the disk
//! and memory read, and how long each phase takes there. It runs the
//! pipeline, prints everything to stderr, and exits with status 1 on purpose
//! so the scorer shows the log. Never set it for a real submission.

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

fn cache_report(data: &Path, label: &str) {
    let (cached, total) = files(data)
        .par_iter()
        .map(|path| resident(path))
        .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    eprintln!(
        "[diag] {label}: {:.1} MB of {:.1} MB input in page cache ({:.1}%)",
        cached as f64 / 1e6,
        total as f64 / 1e6,
        100.0 * cached as f64 / total.max(1) as f64
    );
}

/// Reads every input file with `threads`-way parallel 1 MB reads.
fn read_all(data: &Path) -> (u64, f64) {
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
    (bytes, start.elapsed().as_secs_f64())
}

/// Reads every input file with O_DIRECT, bypassing the page cache: the
/// disk's own speed at 8 parallel streams of 4 MB reads.
fn read_direct(data: &Path) -> (u64, f64) {
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
    (bytes, start.elapsed().as_secs_f64())
}

fn show(path: &str, grep: &[&str]) {
    if let Ok(text) = fs::read_to_string(path) {
        for line in text.lines() {
            if grep.is_empty() || grep.iter().any(|word| line.contains(word)) {
                eprintln!("[diag] {path}: {line}");
            }
        }
    }
}

/// Before the pipeline: what the machine is and what is already cached.
pub fn before(data: &Path, start: Instant) {
    eprintln!(
        "[diag] process start to diag: {:.3}s; threads available {:?}",
        start.elapsed().as_secs_f64(),
        std::thread::available_parallelism()
    );
    show("/proc/uptime", &[]);
    show("/proc/1/stat", &[]);
    cache_report(data, "before run");
}

/// After the pipeline: warm rerun, read speeds, and machine details. Exits 1.
pub fn after(data: &Path, output: &Path, run: impl Fn(&Path, &Path, Instant)) -> ! {
    cache_report(data, "after run");
    let warm = Instant::now();
    let _ = fs::remove_dir_all(output.join("user_daily"));
    let _ = fs::remove_dir_all(output.join("campaign_daily"));
    run(data, output, warm);
    eprintln!("[diag] warm rerun total {:.3}s", warm.elapsed().as_secs_f64());

    let (bytes, seconds) = read_all(data);
    eprintln!(
        "[diag] warm read(): {:.1} MB in {:.3}s = {:.2} GB/s",
        bytes as f64 / 1e6,
        seconds,
        bytes as f64 / seconds / 1e9
    );
    let (bytes, seconds) = read_direct(data);
    eprintln!(
        "[diag] O_DIRECT disk read: {:.1} MB in {:.3}s = {:.2} GB/s",
        bytes as f64 / 1e6,
        seconds,
        bytes as f64 / seconds / 1e9
    );
    show("/proc/cpuinfo", &["model name", "cpu MHz", "cache size", "siblings", "cpu cores"]);
    show("/proc/meminfo", &["MemTotal", "MemAvailable", "Cached", "Hugepages"]);
    show("/proc/mounts", &["/data", "/output", "/scratch", "/tmp", " / "]);
    show("/proc/version", &[]);
    show("/sys/kernel/mm/transparent_hugepage/enabled", &[]);
    show("/sys/fs/cgroup/cpu.max", &[]);
    show("/sys/fs/cgroup/memory.max", &[]);
    show("/proc/self/status", &["Cpus_allowed_list", "VmHWM"]);
    // Values only for the scorer's documented variables; names for the rest.
    let known = ["DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR", "HOME", "NCPU", "MEM_GB", "PATH"];
    for (key, value) in std::env::vars() {
        if known.contains(&key.as_str()) {
            eprintln!("[diag] env {key}={value}");
        } else {
            eprintln!("[diag] env {key} is set");
        }
    }
    eprintln!("[diag] exiting 1 on purpose so the log is shown");
    std::process::exit(1);
}
