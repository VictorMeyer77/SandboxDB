use crate::storage::schema::schema_error::SchemaError;

pub trait SchemaEncoding<T> {
    fn from_str(chars: &str) -> Result<T, SchemaError>;
}
