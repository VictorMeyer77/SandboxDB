use crate::storage::page::page_error::PageError;
use crate::storage::schema::Schema;

pub trait Encoding<T> {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<T, PageError>;
    fn get_bytes_size(&self) -> usize {
        self.as_bytes().len()
    }
}
