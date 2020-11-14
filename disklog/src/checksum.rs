use crate::LogPosition;

pub type Checksum = u16;

pub fn calculate(position: LogPosition, item_size: u32) -> Checksum {
    let position_bytes = position.to_le_bytes();
    let item_size_bytes = item_size.to_le_bytes();

    Checksum::from_le_bytes([position_bytes[0], position_bytes[1]])
        ^ Checksum::from_le_bytes([position_bytes[2], position_bytes[3]])
        ^ Checksum::from_le_bytes([position_bytes[4], position_bytes[5]])
        ^ Checksum::from_le_bytes([position_bytes[6], position_bytes[7]])
        ^ Checksum::from_le_bytes([item_size_bytes[0], item_size_bytes[1]])
        ^ Checksum::from_le_bytes([item_size_bytes[2], item_size_bytes[3]])
}
