use serde::{Deserialize, Serialize};

use crate::storage::file::error::Error;
use crate::storage::schema::Schema;

pub trait FileEncoding: Serialize + for<'de> Deserialize<'de> {
    fn as_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<Self, Error> {
        Ok(bincode::deserialize(bytes).unwrap())
    }
    fn bytes_size(&self) -> usize {
        self.as_bytes().len()
    }
}
