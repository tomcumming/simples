use std::convert::TryInto;

const SIZE_CHECKED: &str = "Size was checked";

pub const U64SIZE: usize = std::mem::size_of::<u64>();

/// This should be enough to check for corruption, if writes are in order
pub fn parse_tail_position(bytes: [u8; U64SIZE * 3]) -> Option<u64> {
    let first = u64::from_le_bytes((&bytes[..U64SIZE]).try_into().expect(SIZE_CHECKED));
    let second = u64::from_le_bytes(
        (&bytes[U64SIZE..U64SIZE * 2])
            .try_into()
            .expect(SIZE_CHECKED),
    );
    let third = u64::from_le_bytes((&bytes[U64SIZE * 2..]).try_into().expect(SIZE_CHECKED));

    if first == second {
        Some(first)
    } else if second == third {
        Some(second)
    } else {
        None
    }
}
