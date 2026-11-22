//! An ID held in 16 bytes. IDs of up to 15 bytes are stored inline, so
//! equality, ordering and hashing never touch the input files. Longer IDs
//! fall back to a packed reference into the mapped input.
//!
//! Inline layout, as a big-endian 128-bit number: the ID's bytes first,
//! zero-padded, then its length in the last byte. Integer order is then
//! byte-wise string order: equal prefixes fall through to the length, and a
//! shorter string sorts first.

use std::cmp::Ordering;

use crate::{Text, fold, hash_bytes};

const LONG: u64 = 0xFF;

#[derive(Clone, Copy)]
pub struct Key {
    hi: u64,
    lo: u64,
}

impl Key {
    #[inline]
    pub fn new(s: &[u8]) -> Key {
        let len = s.len();
        let pointer = s.as_ptr();
        // One 16-byte load when it stays inside the field's own page, which is
        // mapped because the field is; bytes past the field are masked off.
        if len <= 15 && (pointer as usize & 4095) <= 4096 - 16 {
            let raw = unsafe { std::ptr::read_unaligned(pointer as *const u128) };
            let bytes = u128::from_be(raw);
            let kept = if len == 0 {
                0
            } else {
                bytes & (!0u128 << (128 - 8 * len))
            };
            let value = kept | len as u128;
            return Key {
                hi: (value >> 64) as u64,
                lo: value as u64,
            };
        }
        if len <= 15 {
            let mut bytes = [0u8; 16];
            bytes[..s.len()].copy_from_slice(s);
            bytes[15] = s.len() as u8;
            let value = u128::from_be_bytes(bytes);
            Key {
                hi: (value >> 64) as u64,
                lo: value as u64,
            }
        } else {
            Key {
                hi: Text::new(s).0,
                lo: LONG,
            }
        }
    }

    #[inline]
    fn is_inline(self) -> bool {
        self.lo & 0xFF != LONG
    }

    /// Calls `f` with the ID's bytes.
    #[inline]
    pub fn with_bytes<R>(self, f: impl FnOnce(&[u8]) -> R) -> R {
        if self.is_inline() {
            let value = (u128::from(self.hi) << 64) | u128::from(self.lo);
            let bytes = value.to_be_bytes();
            f(&bytes[..bytes[15] as usize])
        } else {
            f(Text(self.hi).get())
        }
    }

    #[cfg(test)]
    pub fn to_vec(self) -> Vec<u8> {
        self.with_bytes(<[u8]>::to_vec)
    }

    #[inline]
    pub fn hash(self) -> u64 {
        if self.is_inline() {
            // One folded 128-bit multiply mixes both halves (wyhash's core).
            fold(self.hi ^ 0xe703_7ed1_a0b4_28db, self.lo ^ 0x8ebc_6af0_9c88_c6e3)
        } else {
            hash_bytes(Text(self.hi).get())
        }
    }
}

impl PartialEq for Key {
    #[inline]
    fn eq(&self, other: &Key) -> bool {
        if self.hi == other.hi && self.lo == other.lo {
            return true;
        }
        // An inline ID is at most 15 bytes and a long one at least 16, so
        // they never match; two long ones compare their bytes.
        if self.is_inline() || other.is_inline() {
            return false;
        }
        Text(self.hi).get() == Text(other.hi).get()
    }
}

impl Eq for Key {}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Key) -> Ordering {
        if self.is_inline() && other.is_inline() {
            (self.hi, self.lo).cmp(&(other.hi, other.lo))
        } else {
            self.with_bytes(|a| other.with_bytes(|b| a.cmp(b)))
        }
    }
}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_and_equality() {
        let words: Vec<&[u8]> = vec![
            b"",
            b"a",
            b"a\0",
            b"a\0z",
            b"ab",
            b"U1",
            b"u1",
            b" u1",
            b"u1 ",
            b"\xff",
            b"exactly15bytes!",
            b"sixteen_bytes_ab",
            b"sixteen_bytes_ac",
            b"sixteen_bytes_a",
            b"a much longer identifier than sixteen",
        ];
        for a in &words {
            for b in &words {
                let (ka, kb) = (Key::new(a), Key::new(b));
                assert_eq!(ka == kb, a == b, "{a:?} {b:?}");
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} {b:?}");
                if a == b {
                    assert_eq!(ka.hash(), kb.hash());
                }
            }
            assert_eq!(Key::new(a).to_vec(), a.to_vec());
        }
    }

    #[test]
    fn fast_and_slow_loads_agree() {
        // Fields at every offset across a page boundary take both load paths.
        let buffer: Vec<u8> = (0..3 * 4096).map(|i| b"azAZ09_ ,"[i % 9]).collect();
        for start in 4096 - 40..4096 + 8 {
            for len in 0..=15 {
                let field = &buffer[start..start + len];
                let mut bytes = [0u8; 16];
                bytes[..len].copy_from_slice(field);
                bytes[15] = len as u8;
                let want = u128::from_be_bytes(bytes);
                let key = Key::new(field);
                assert_eq!((u128::from(key.hi) << 64) | u128::from(key.lo), want);
            }
        }
    }

    #[test]
    fn every_length_hashes() {
        let bytes: Vec<u8> = (0..100).collect();
        for n in 0..=100 {
            // A long key refers to its bytes, so the copy must outlive it.
            let copy = bytes[..n].to_vec();
            let a = Key::new(&bytes[..n]);
            let b = Key::new(&copy);
            assert_eq!(a.hash(), b.hash());
            assert!(a == b);
        }
    }
}
