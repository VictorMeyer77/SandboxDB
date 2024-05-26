use crate::storage::page::encoding::Encoding;
use crate::storage::page::page_error::PageError;
use crate::storage::schema::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Slot {
    pub offset: u32,
    pub length: u32,
}

impl Slot {
    pub fn build(offset: u32, length: u32) -> Slot {
        Slot { offset, length }
    }
}

impl Encoding<Slot> for Slot {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&u32::to_le_bytes(self.offset));
        concat_bytes.extend_from_slice(&u32::to_le_bytes(self.length));
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], _schema: Option<&Schema>) -> Result<Slot, PageError> {
        Ok(Slot {
            offset: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            length: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_slot() {
        assert_eq!(Slot::build(981, 12).as_bytes(), [213, 3, 0, 0, 12, 0, 0, 0])
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            Slot::from_bytes(&[213, 3, 0, 0, 12, 0, 0, 0], None).unwrap(),
            Slot::build(981, 12)
        )
    }
}
