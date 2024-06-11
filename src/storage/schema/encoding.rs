use crate::storage::schema::error::Error;

pub trait Encoding<T> {
    fn from_str(chars: &str) -> Result<T, Error>;
}
