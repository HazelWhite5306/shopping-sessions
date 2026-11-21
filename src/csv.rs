//! The CSV dialect in rules.md: `\n` ends a line, any field may be wrapped in
//! double quotes, a quote anywhere else makes the line malformed, and the field
//! count must match exactly.

/// Splits `line` into exactly `N` fields, or None if it is malformed.
#[inline]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let n = line.len();
    if n == 0 {
        return None;
    }
    let mut out: [&[u8]; N] = [&line[..0]; N];
    let mut field = 0;
    let mut i = 0;
    loop {
        if field >= N {
            return None;
        }
        if i < n && line[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < n && line[j] != b'"' && line[j] != b',' {
                j += 1;
            }
            if j >= n || line[j] != b'"' {
                return None;
            }
            out[field] = &line[start..j];
            field += 1;
            j += 1;
            if j == n {
                break;
            }
            if line[j] != b',' {
                return None;
            }
            i = j + 1;
        } else {
            let start = i;
            let mut j = i;
            while j < n {
                let b = line[j];
                if b == b',' {
                    break;
                }
                if b == b'"' {
                    return None;
                }
                j += 1;
            }
            out[field] = &line[start..j];
            field += 1;
            if j == n {
                break;
            }
            i = j + 1;
        }
    }
    if field == N { Some(out) } else { None }
}

/// Bits for every `\n`, `,` and `"` in the 64 bytes at `at` (fewer at the end).
#[inline(always)]
fn structural(data: &[u8], at: usize) -> u64 {
    if at + 64 <= data.len() {
        #[cfg(target_feature = "avx2")]
        unsafe {
            use std::arch::x86_64::*;
            let pointer = data.as_ptr().add(at);
            let a = _mm256_loadu_si256(pointer as *const __m256i);
            let b = _mm256_loadu_si256(pointer.add(32) as *const __m256i);
            let newline = _mm256_set1_epi8(b'\n' as i8);
            let comma = _mm256_set1_epi8(b',' as i8);
            let quote = _mm256_set1_epi8(b'"' as i8);
            let hits = |v: __m256i| {
                _mm256_or_si256(
                    _mm256_or_si256(_mm256_cmpeq_epi8(v, newline), _mm256_cmpeq_epi8(v, comma)),
                    _mm256_cmpeq_epi8(v, quote),
                )
            };
            let low = _mm256_movemask_epi8(hits(a)) as u32 as u64;
            let high = _mm256_movemask_epi8(hits(b)) as u32 as u64;
            return low | high << 32;
        }
    }
    let mut bits = 0u64;
    for (i, &byte) in data[at..data.len().min(at + 64)].iter().enumerate() {
        if byte == b'\n' || byte == b',' || byte == b'"' {
            bits |= 1 << i;
        }
    }
    bits
}

/// Calls `f` with the `N` fields of every well-formed line in a chunk that
/// starts at a line start, and skips malformed lines. Matches
/// `for_lines` + `split::<N>` exactly, but finds every separator and quote
/// 64 bytes at a time.
#[inline]
#[allow(unused_assignments)]
pub fn for_records<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let n = data.len();
    let mut fields: [&'a [u8]; N] = [&data[..0]; N];
    let mut line_start = 0;
    let mut field_start = 0;
    let mut count = 0; // fields closed in this line
    let mut bad = false;
    let mut quotes = 0u32; // quotes in the current field
    let mut first_quote = 0;
    let mut last_quote = 0;

    // Closes the field that ends at `end` (exclusive).
    macro_rules! close {
        ($end:expr) => {{
            let end = $end;
            if count < N {
                if quotes == 0 {
                    fields[count] = &data[field_start..end];
                } else if quotes == 2 && first_quote == field_start && last_quote + 1 == end {
                    fields[count] = &data[field_start + 1..end - 1];
                } else {
                    bad = true;
                }
            } else {
                bad = true;
            }
            count += 1;
            quotes = 0;
            field_start = end + 1;
        }};
    }
    macro_rules! end_line {
        ($end:expr) => {{
            let end = $end;
            close!(end);
            if !bad && count == N && end > line_start {
                f(fields);
            }
            count = 0;
            bad = false;
            line_start = end + 1;
        }};
    }

    let mut base = 0;
    while base < n {
        let mut bits = structural(data, base);
        while bits != 0 {
            let at = base + bits.trailing_zeros() as usize;
            bits &= bits - 1;
            match data[at] {
                b',' => close!(at),
                b'"' => {
                    if quotes == 0 {
                        first_quote = at;
                    }
                    last_quote = at;
                    quotes += 1;
                }
                _ => end_line!(at),
            }
        }
        base += 64;
    }
    if line_start < n {
        end_line!(n);
    }
}

/// Calls `f` on every line of a chunk that starts at a line start.
#[inline]
pub fn for_lines<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut start = 0;
    for end in memchr::memchr_iter(b'\n', data) {
        f(&data[start..end]);
        start = end + 1;
    }
    if start < data.len() {
        f(&data[start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for_lines(data, |line| {
            if let Some(fields) = split::<N>(line) {
                out.push(fields.iter().map(|f| f.to_vec()).collect());
            }
        });
        out
    }

    fn fast<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for_records::<N>(data, |fields| {
            out.push(fields.iter().map(|f| f.to_vec()).collect());
        });
        out
    }

    #[test]
    fn records_match_split_on_random_input() {
        let alphabet = b"ab,,\"\"\n\n \r";
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        for round in 0..3000 {
            let len = next() % 400;
            let mut data: Vec<u8> = (0..len).map(|_| alphabet[next() % alphabet.len()]).collect();
            // Some rounds use well-formed rows with quoted fields, like the generator.
            if round % 3 == 0 {
                data.clear();
                for _ in 0..next() % 20 {
                    let fields = 1 + next() % 8;
                    for at in 0..fields {
                        if at > 0 {
                            data.push(b',');
                        }
                        let quoted = next() % 5 == 0;
                        if quoted {
                            data.push(b'"');
                        }
                        for _ in 0..next() % 12 {
                            data.push(b"xyz019 "[next() % 7]);
                        }
                        if quoted {
                            data.push(b'"');
                        }
                    }
                    if next() % 10 != 0 {
                        data.push(b'\n');
                    }
                }
            }
            assert_eq!(fast::<2>(&data), reference::<2>(&data), "{:?}", String::from_utf8_lossy(&data));
            assert_eq!(fast::<4>(&data), reference::<4>(&data), "{:?}", String::from_utf8_lossy(&data));
            assert_eq!(fast::<7>(&data), reference::<7>(&data), "{:?}", String::from_utf8_lossy(&data));
        }
    }

    #[test]
    fn dialect() {
        assert_eq!(split::<2>(b"a,b"), Some([&b"a"[..], b"b"]));
        assert_eq!(split::<2>(b"\"a\",\"\""), Some([&b"a"[..], b""]));
        assert_eq!(split::<2>(b"a,"), Some([&b"a"[..], b""]));
        assert_eq!(split::<2>(b" a,b \r"), Some([&b" a"[..], b"b \r"]));
        assert_eq!(split::<2>(b"a,b,c"), None);
        assert_eq!(split::<2>(b"a"), None);
        assert_eq!(split::<2>(b""), None);
        assert_eq!(split::<2>(b"\"a,b"), None);
        assert_eq!(split::<2>(b"a,b\""), None);
        assert_eq!(split::<2>(b"a\"x\",b"), None);
        assert_eq!(split::<2>(b"\"a\"x,b"), None);
        assert_eq!(split::<2>(b"\"a,b\""), None);
        assert_eq!(split::<1>(b"\""), None);
    }
}
