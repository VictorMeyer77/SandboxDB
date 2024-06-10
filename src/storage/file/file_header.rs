use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::FileEncoding;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileHeader {
    pub file_size: u32,
    pub pages: u32,
    pub visibility: u8,
    pub compression: u8,
    pub version: [u8; 3],
}

impl FileHeader {
    pub fn build(file_size: u32, compression: u8, version: [u8; 3]) -> FileHeader {
        FileHeader {
            file_size,
            pages: 0,
            visibility: 0,
            compression,
            version,
        }
    }
}

impl FileEncoding for FileHeader {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_file_header() {
        assert_eq!(
            FileHeader::build(1000, 3, [0, 12, 54]).as_bytes().unwrap(),
            [232, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 12, 54]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            FileHeader::from_bytes(&[232, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 12, 54]).unwrap(),
            FileHeader::build(1000, 3, [0, 12, 54])
        )
    }
}
