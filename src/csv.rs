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
