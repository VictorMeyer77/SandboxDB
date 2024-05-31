use crate::storage::file::file_error::FileError;
use crate::storage::schema::schema::Schema;

pub trait FileEncoding<T> {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<T, FileError>;
    fn bytes_size(&self) -> usize {
        self.as_bytes().len()
    }
}
