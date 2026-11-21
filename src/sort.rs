//! A stable LSD radix sort on a 64-bit key, 11 bits per pass, that skips any
//! pass whose digit is the same for every item.

const DIGIT: u32 = 11;
const RADIX: usize = 1 << DIGIT;

/// Sorts `items` by `key`, stably, using the low `bits` bits of the key.
pub fn radix_sort<T: Copy>(items: &mut Vec<T>, bits: u32, key: impl Fn(&T) -> u64) {
    if items.len() < 64 {
        items.sort_by_key(|item| key(item));
        return;
    }
    let mut scratch: Vec<T> = Vec::with_capacity(items.len());
    let mut shift = 0;
    while shift < bits {
        let mut counts = vec![0usize; RADIX];
        for item in items.iter() {
            counts[((key(item) >> shift) as usize) & (RADIX - 1)] += 1;
        }
        if counts.iter().any(|&count| count == items.len()) {
            shift += DIGIT;
            continue;
        }
        let mut total = 0;
        for count in counts.iter_mut() {
            let here = *count;
            *count = total;
            total += here;
        }
        unsafe {
            scratch.set_len(items.len());
            let target = scratch.as_mut_ptr();
            for item in items.iter() {
                let digit = ((key(item) >> shift) as usize) & (RADIX - 1);
                target.add(counts[digit]).write(*item);
                counts[digit] += 1;
            }
        }
        std::mem::swap(items, &mut scratch);
        shift += DIGIT;
    }
}

/// The number of bits needed to hold `value`.
pub fn bits(value: u64) -> u32 {
    64 - value.leading_zeros()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_stably() {
        let mut state: u64 = 7;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 20
        };
        for len in [0usize, 1, 5, 63, 64, 65, 1000, 50_000] {
            let mut items: Vec<(u64, usize)> = (0..len).map(|at| (next() % 5000, at)).collect();
            let mut want = items.clone();
            want.sort_by_key(|item| item.0);
            radix_sort(&mut items, bits(5000), |item| item.0);
            assert_eq!(items, want);
        }
    }
}
