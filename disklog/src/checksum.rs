use crate::LogPosition;

pub type Checksum = u16;

pub fn calculate(position: LogPosition, item_size: u32) -> Checksum {
    // TODO something clever
    (position as Checksum) + (item_size as Checksum)
}
