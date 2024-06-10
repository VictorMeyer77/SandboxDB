use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::FileEncoding;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleHeader {
    pub visibility: u8,
    pub nulls: Vec<u8>,
}

impl TupleHeader {
    pub fn build(nulls: &[u8]) -> TupleHeader {
        TupleHeader {
            visibility: 0,
            nulls: nulls.to_vec(),
        }
    }
}

impl FileEncoding for TupleHeader {}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_bytes_should_convert_tuple_header() {
        assert_eq!(
            TupleHeader::build(&[0, 1, 0, 1]).as_bytes().unwrap(),
            [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1]
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            TupleHeader::from_bytes(&[0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1]).unwrap(),
            TupleHeader::build(&[0, 1, 0, 1])
        )
    }
}
