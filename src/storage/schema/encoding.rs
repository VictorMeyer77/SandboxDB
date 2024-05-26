use crate::storage::schema::schema_error::SchemaError;

pub trait Encoding<T> {
    fn from_str(chars: &str) -> Result<T, SchemaError>;
}
