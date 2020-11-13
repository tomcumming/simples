use crate::LogPosition;

pub type Checksum = u16;

pub fn calculate(_position: LogPosition, _item_size: u32) -> Checksum {
    // TODO something clever
    123
}
