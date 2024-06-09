use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::FileEncoding;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageHeader {
    pub page_size: u32,
    pub slots: u32,
    pub checksum: u32,
    pub visibility: u8,
    pub compression: u8,
}

impl PageHeader {
    pub fn build(page_size: u32, compression: u8) -> PageHeader {
        PageHeader {
            page_size,
            slots: 0,
            checksum: 0,
            visibility: 0,
            compression,
        }
    }
}

impl FileEncoding for PageHeader {}

/*
impl FileEncoding<PageHeader> for PageHeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.page_size.to_le_bytes());
        concat_bytes.extend_from_slice(&self.slots.to_le_bytes());
        concat_bytes.extend_from_slice(&self.checksum.to_le_bytes());
        concat_bytes.extend_from_slice(&[self.visibility]);
        concat_bytes.extend_from_slice(&[self.compression]);
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], _schema: Option<&Schema>) -> Result<PageHeader, Error> {
        Ok(PageHeader {
            page_size: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            slots: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            checksum: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            visibility: u8::from_le_bytes(bytes[12..13].try_into().unwrap()),
            compression: u8::from_le_bytes(bytes[13..14].try_into().unwrap()),
        })
    }
}*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_page_header() {
        assert_eq!(
            PageHeader::build(981, 3).as_bytes(),
            [213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            PageHeader::from_bytes(&[213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3], None,).unwrap(),
            PageHeader::build(981, 3)
        )
    }
}
