use crate::storage::page::encoding::Encoding;
use crate::storage::page::page_error::PageError;
use crate::storage::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct PageHeader {
    pub page_size: u32,
    pub slots: u32,
    pub checksum: [u8; 32], // TODO
    pub version: [u8; 3],
    pub visibility: u8,
    pub compression: u8,
}

impl PageHeader {
    pub fn build(page_size: u32, version: [u8; 3], compression: u8) -> PageHeader {
        PageHeader {
            page_size,
            slots: 0,
            checksum: [0; 32],
            version,
            visibility: 0,
            compression,
        }
    }
}

impl Encoding<PageHeader> for PageHeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.page_size.to_le_bytes());
        concat_bytes.extend_from_slice(&self.slots.to_le_bytes());
        concat_bytes.extend_from_slice(&self.checksum);
        concat_bytes.extend_from_slice(&self.version);
        concat_bytes.extend_from_slice(&self.visibility.to_le_bytes());
        concat_bytes.extend_from_slice(&self.compression.to_le_bytes());
        concat_bytes
    }

    fn from_bytes(bytes: Vec<u8>, _schema: Option<&Schema>) -> Result<PageHeader, PageError> {
        Ok(PageHeader {
            page_size: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            slots: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            checksum: bytes[8..40].try_into().unwrap(),
            version: bytes[40..43].try_into().unwrap(),
            visibility: u8::from_le_bytes(bytes[43..44].try_into().unwrap()),
            compression: u8::from_le_bytes(bytes[44..45].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_page_header() {
        assert_eq!(
            PageHeader::build(981, [0, 12, 54], 3).as_bytes(),
            [
                213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 54, 0, 3
            ]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            PageHeader::from_bytes(
                vec![
                    213, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 54, 0, 3,
                ],
                None,
            )
                .unwrap(),
            PageHeader::build(981, [0, 12, 54], 3)
        )
    }
}
