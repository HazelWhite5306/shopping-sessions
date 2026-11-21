//! Timestamp parsing for the three valid forms (rules.md "Timestamps").
//! Returns milliseconds since the epoch, UTC, or None for a bad timestamp.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const DAY: i64 = 86_400_000;

#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[inline]
fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
    }
}

#[inline]
fn d(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline]
fn d2(s: &[u8], at: usize) -> Option<i64> {
    Some(d(s[at])? * 10 + d(s[at + 1])?)
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `s` (19 bytes), as UTC-naive millis.
#[inline]
fn civil(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(((days * 24 + hour) * 60 + minute) * MINUTE + second * SECOND)
}

/// Days since the epoch of the first of each month, for years
/// `FIRST_YEAR..FIRST_YEAR + YEARS`, and each month's length.
const FIRST_YEAR: i64 = 1970;
const YEARS: usize = 200;

struct Calendar {
    month_start: Vec<i64>,
    month_days: Vec<u8>,
}

fn calendar() -> &'static Calendar {
    static CALENDAR: std::sync::OnceLock<Calendar> = std::sync::OnceLock::new();
    CALENDAR.get_or_init(|| {
        let mut month_start = Vec::with_capacity(YEARS * 12);
        let mut month_days = Vec::with_capacity(YEARS * 12);
        for year in FIRST_YEAR..FIRST_YEAR + YEARS as i64 {
            for month in 1..=12 {
                month_start.push(days_from_civil(year, month, 1));
                month_days.push(days_in_month(year, month) as u8);
            }
        }
        Calendar {
            month_start,
            month_days,
        }
    })
}

/// The fast path for F1 and F2: table lookups for the date and branch-free
/// digit checks. `parse` stays the reference; tests compare the two.
pub struct Parser {
    calendar: &'static Calendar,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            calendar: calendar(),
        }
    }

    /// The UTC-naive milliseconds of `YYYY-MM-DDTHH:MM:SS` at the start of
    /// `s` (20 bytes or more), or None if any part is invalid. Reads the 19
    /// bytes as words: separators are checked by mask, then replaced by '0'
    /// so one SWAR test checks every digit at once.
    #[inline]
    fn civil(&mut self, s: &[u8]) -> Option<i64> {
        const ONES: u64 = 0x0101_0101_0101_0101;
        // "YYYY-MM-": separators at bytes 4 and 7.
        let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
        const A_MASK: u64 = 0xFF << 32 | 0xFF << 56;
        const A_SEP: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
        // "DDTHH:MM": separators at bytes 2 and 5.
        let b = u64::from_le_bytes(s[8..16].try_into().unwrap());
        const B_MASK: u64 = 0xFF << 16 | 0xFF << 40;
        const B_SEP: u64 = (b'T' as u64) << 16 | (b':' as u64) << 40;
        // ":SS?": separator at byte 0; byte 3 is the zone, checked by the caller.
        let c = u64::from(u32::from_le_bytes(s[16..20].try_into().unwrap()));
        const C_MASK: u64 = 0xFF | 0xFF << 24 | 0xFFFF_FFFF << 32;
        const C_SEP: u64 = b':' as u64;
        let zeros = 0x30 * ONES;
        if (a & A_MASK) != A_SEP || (b & B_MASK) != B_SEP || (c & 0xFF) != C_SEP {
            return None;
        }
        let a = (a & !A_MASK) | (zeros & A_MASK);
        let b = (b & !B_MASK) | (zeros & B_MASK);
        let c = (c & !C_MASK) | (zeros & C_MASK);
        let not_digit = |x: u64| (x.wrapping_add(0x46 * ONES) | x.wrapping_sub(zeros)) & (0x80 * ONES);
        if not_digit(a) | not_digit(b) | not_digit(c) != 0 {
            return None;
        }
        let (a, b, c) = (a - zeros, b - zeros, c - zeros);
        let byte = |x: u64, at: u32| ((x >> (8 * at)) & 0xFF) as i64;
        let year = byte(a, 0) * 1000 + byte(a, 1) * 100 + byte(a, 2) * 10 + byte(a, 3);
        let month = byte(a, 5) * 10 + byte(a, 6);
        let day = byte(b, 0) * 10 + byte(b, 1);
        let hour = byte(b, 3) * 10 + byte(b, 4);
        let minute = byte(b, 6) * 10 + byte(b, 7);
        let second = byte(c, 1) * 10 + byte(c, 2);
        if !(1..=12).contains(&month) || day < 1 || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let days = if (FIRST_YEAR..FIRST_YEAR + YEARS as i64).contains(&year) {
            let at = ((year - FIRST_YEAR) * 12 + month - 1) as usize;
            if day > i64::from(self.calendar.month_days[at]) {
                return None;
            }
            self.calendar.month_start[at] + day - 1
        } else {
            if day > days_in_month(year, month) {
                return None;
            }
            days_from_civil(year, month, day)
        };
        Some(((days * 24 + hour) * 60 + minute) * MINUTE + second * SECOND)
    }

    #[inline]
    pub fn parse(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 0 {
            return None;
        }
        if n != 20 && n != 25 {
            // F3 of up to 18 digits cannot overflow; anything else goes to
            // the reference parser.
            if n <= 18 {
                let mut value: i64 = 0;
                let mut bad = false;
                for &b in s {
                    let digit = b.wrapping_sub(b'0');
                    bad |= digit > 9;
                    value = value.wrapping_mul(10).wrapping_add(i64::from(digit));
                }
                if !bad {
                    return Some(value);
                }
            }
            return parse(s);
        }
        if n == 20 {
            if s[19] != b'Z' {
                return parse(s);
            }
            return self.civil(s);
        }
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return parse(s),
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let local = self.civil(s)?;
        Some(local - sign * (oh * 60 + om) * MINUTE)
    }
}

#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n <= 18 && s[0].is_ascii_digit() {
        // F3 that cannot overflow: up to 18 digits is under 10^18.
        let mut value: i64 = 0;
        let mut all_digits = true;
        for &b in s {
            let digit = b.wrapping_sub(b'0');
            all_digits &= digit < 10;
            value = value.wrapping_mul(10).wrapping_add(i64::from(digit));
        }
        if all_digits {
            return Some(value);
        }
    } else if s[0].is_ascii_digit() {
        // F3 if every byte is a digit.
        let mut value: i64 = 0;
        let mut all_digits = true;
        let mut overflow = false;
        for &b in s {
            if !b.is_ascii_digit() {
                all_digits = false;
                break;
            }
            match value
                .checked_mul(10)
                .and_then(|v| v.checked_add((b - b'0') as i64))
            {
                Some(v) => value = v,
                None => overflow = true,
            }
        }
        if all_digits {
            // A number too long for 64 bits is only outside the window.
            return if overflow { None } else { Some(value) };
        }
    }
    match n {
        20 => {
            if s[19] != b'Z' {
                return None;
            }
            civil(s)
        }
        25 => {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = civil(s)?;
            Some(local - sign * (oh * 60 + om) * MINUTE)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:28:10+01:30"), Some(f1));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        for bad in [
            &b""[..],
            b"2026-10-03T23:58:10.000Z",
            b"2026-10-03T23:58:10z",
            b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z",
            b"2026-10-03T24:58:10Z",
            b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
            b"99999999999999999999999",
        ] {
            assert_eq!(parse(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn cached_parser_matches() {
        let seeds: [&[u8]; 6] = [
            b"2026-10-03T23:58:10Z",
            b"2026-10-03T19:58:10-04:00",
            b"2028-02-29T00:00:00+12:00",
            b"1791071890000",
            b"2027-02-28T23:59:59Z",
            b"2026-12-31T00:00:00-00:00",
        ];
        let alphabet = b"0123456789-T:Z+zt ./\xff\x80\xd9\x7f\x00";
        let mut state: u64 = 42;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        let mut parser = Parser::new();
        for _ in 0..1_000_000 {
            let mut s = seeds[next() % seeds.len()].to_vec();
            for _ in 0..next() % 3 {
                let at = next() % s.len();
                s[at] = alphabet[next() % alphabet.len()];
            }
            assert_eq!(parser.parse(&s), parse(&s), "{}", String::from_utf8_lossy(&s));
        }
    }
}
