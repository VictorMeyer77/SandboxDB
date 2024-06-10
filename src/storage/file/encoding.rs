use serde::{Deserialize, Serialize};

use crate::storage::file::error::Error;

pub trait FileEncoding: Serialize + for<'de> Deserialize<'de> {
    fn as_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(bincode::serialize(&self)?)
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn bytes_size(&self) -> Result<usize, Error> {
        Ok(self.as_bytes()?.len())
    }
}
