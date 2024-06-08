use crate::storage::file::error::Error;
use crate::storage::schema::schema::Schema;

pub trait FileEncoding<T> {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<T, Error>;
    fn bytes_size(&self) -> usize {
        self.as_bytes().len()
    }
}
