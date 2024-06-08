use crate::storage::schema::error::Error;

pub trait SchemaEncoding<T> {
    fn from_str(chars: &str) -> Result<T, Error>;
}
