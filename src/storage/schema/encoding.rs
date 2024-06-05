use crate::storage::schema::error::SchemaError;

pub trait SchemaEncoding<T> {
    fn from_str(chars: &str) -> Result<T, SchemaError>;
}
