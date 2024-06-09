use std::mem;

use serde::{Deserialize, Serialize};

use crate::storage::schema::encoding::SchemaEncoding;
use crate::storage::schema::error::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Boolean,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Float,
    Timestamp,
    String, // todo
}

impl Type {
    pub fn get_byte_size(&self) -> usize {
        match self {
            Type::Boolean => mem::size_of::<bool>(),
            Type::Tinyint => mem::size_of::<i8>(),
            Type::Smallint => mem::size_of::<i16>(),
            Type::Int => mem::size_of::<i32>(),
            Type::Bigint => mem::size_of::<i128>(),
            Type::Float => mem::size_of::<f64>(),
            Type::Timestamp => mem::size_of::<i64>(),
            Type::String => 0, // TODO
        }
    }
}

impl SchemaEncoding<Type> for Type {
    fn from_str(type_str: &str) -> Result<Type, Error> {
        match type_str.to_uppercase().trim() {
            "BOOLEAN" => Ok(Type::Boolean),
            "TINYINT" => Ok(Type::Tinyint),
            "SMALLINT" => Ok(Type::Smallint),
            "INT" => Ok(Type::Int),
            "BIGINT" => Ok(Type::Bigint),
            "FLOAT" => Ok(Type::Float),
            "TIMESTAMP" => Ok(Type::Timestamp),
            "STRING" => Ok(Type::String),
            _ => Err(Error::InvalidType(format!(
                "\n- Unknown type \"{}\"",
                type_str
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_should_return_enum() {
        assert_eq!(Type::from_str("boolean").unwrap(), Type::Boolean);
        assert_eq!(Type::from_str("TinyINT").unwrap(), Type::Tinyint);
        assert_eq!(Type::from_str("SMALLINT").unwrap(), Type::Smallint);
        assert_eq!(Type::from_str("int").unwrap(), Type::Int);
        assert_eq!(Type::from_str("bigint").unwrap(), Type::Bigint);
        assert_eq!(Type::from_str("float").unwrap(), Type::Float);
        assert_eq!(Type::from_str("Timestamp").unwrap(), Type::Timestamp);
        assert_eq!(Type::from_str("string").unwrap(), Type::String);
    }

    #[test]
    #[should_panic]
    fn from_str_should_return_err_if_not_exist() {
        Type::from_str("unknown").unwrap();
    }
}
