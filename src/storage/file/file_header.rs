use crate::storage::file::encoding::FileEncoding;
use crate::storage::file::page_error::PageError;
use crate::storage::schema::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct FileHeader {
    pub file_size: u32,
    pub pages: u32,
    pub visibility: u8,
    pub compression: u8,
}

impl FileHeader {
    pub fn build(file_size: u32, compression: u8) -> FileHeader {
        FileHeader {
            file_size,
            pages: 0,
            visibility: 0,
            compression,
        }
    }
}

impl FileEncoding<FileHeader> for FileHeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.file_size.to_le_bytes());
        concat_bytes.extend_from_slice(&self.pages.to_le_bytes());
        concat_bytes.extend_from_slice(&[self.visibility]);
        concat_bytes.extend_from_slice(&[self.compression]);
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], _schema: Option<&Schema>) -> Result<FileHeader, PageError> {
        Ok(FileHeader {
            file_size: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            pages: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            visibility: u8::from_le_bytes(bytes[8..9].try_into().unwrap()),
            compression: u8::from_le_bytes(bytes[9..10].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_file_header() {
        assert_eq!(
            FileHeader::build(1000, 3).as_bytes(),
            [232, 3, 0, 0, 0, 0, 0, 0, 0, 3]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            FileHeader::from_bytes(&[232, 3, 0, 0, 0, 0, 0, 0, 0, 3], None).unwrap(),
            FileHeader::build(1000, 3)
        )
    }
}
