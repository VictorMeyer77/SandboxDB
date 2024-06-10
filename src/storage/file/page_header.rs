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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_page_header() {
        assert_eq!(
            PageHeader::build(981, 3).as_bytes().unwrap(),
            [213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            PageHeader::from_bytes(&[213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]).unwrap(),
            PageHeader::build(981, 3)
        )
    }
}
