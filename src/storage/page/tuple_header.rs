use crate::storage::page::encoding::Encoding;
use crate::storage::page::page_error::PageError;
use crate::storage::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct TupleHeader {
    pub visibility: u8,
    pub nulls: Vec<u8>,
}

impl TupleHeader {
    pub fn build(nulls: &Vec<u8>) -> TupleHeader {
        TupleHeader {
            visibility: 0,
            nulls: nulls.clone(),
        }
    }
}

impl Encoding<TupleHeader> for TupleHeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&[self.visibility]);
        concat_bytes.extend_from_slice(self.nulls.as_slice());
        concat_bytes
    }

    fn from_bytes(bytes: Vec<u8>, _schema: Option<&Schema>) -> Result<TupleHeader, PageError> {
        Ok(TupleHeader {
            visibility: bytes[0].clone(),
            nulls: bytes[1..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_tuple_header() {
        assert_eq!(
            TupleHeader::build(&vec![0, 1, 0, 1]).as_bytes(),
            [0, 0, 1, 0, 1]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            TupleHeader::from_bytes([0, 0, 1, 0, 1].to_vec(), None).unwrap(),
            TupleHeader::build(&[0, 1, 0, 1].to_vec())
        )
    }
}
