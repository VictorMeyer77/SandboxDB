use std::mem;

use serde::{Deserialize, Serialize};

use crate::storage::schema::encoding::SchemaEncoding;
use crate::storage::schema::error::SchemaError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Type {
    BOOLEAN,   // bool
    TINYINT,   // i8
    SMALLINT,  // i16
    INT,       // i64
    BIGINT,    // i128
    FLOAT,     // f64
    TIMESTAMP, // u64
    STRING,    // TODO
}

impl Type {
    pub fn get_byte_size(&self) -> usize {
        match self {
            Type::BOOLEAN => mem::size_of::<bool>(),
            Type::TINYINT => mem::size_of::<i8>(),
            Type::SMALLINT => mem::size_of::<i16>(),
            Type::INT => mem::size_of::<i64>(),
            Type::BIGINT => mem::size_of::<i128>(),
            Type::FLOAT => mem::size_of::<f64>(),
            Type::TIMESTAMP => mem::size_of::<u64>(),
            Type::STRING => 0, // TODO
        }
    }
}

impl SchemaEncoding<Type> for Type {
    fn from_str(type_str: &str) -> Result<Type, SchemaError> {
        match type_str.to_uppercase().trim() {
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "TINYINT" => Ok(Type::TINYINT),
            "SMALLINT" => Ok(Type::SMALLINT),
            "INT" => Ok(Type::INT),
            "BIGINT" => Ok(Type::BIGINT),
            "FLOAT" => Ok(Type::FLOAT),
            "TIMESTAMP" => Ok(Type::TIMESTAMP),
            "STRING" => Ok(Type::STRING),
            _ => Err(SchemaError::InvalidType(format!(
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
        assert_eq!(Type::from_str("boolean").unwrap(), Type::BOOLEAN);
        assert_eq!(Type::from_str("TinyINT").unwrap(), Type::TINYINT);
        assert_eq!(Type::from_str("SMALLINT").unwrap(), Type::SMALLINT);
        assert_eq!(Type::from_str("int").unwrap(), Type::INT);
        assert_eq!(Type::from_str("bigint").unwrap(), Type::BIGINT);
        assert_eq!(Type::from_str("float").unwrap(), Type::FLOAT);
        assert_eq!(Type::from_str("Timestamp").unwrap(), Type::TIMESTAMP);
        assert_eq!(Type::from_str("string").unwrap(), Type::STRING);
    }

    #[test]
    #[should_panic]
    fn from_str_should_return_err_if_not_exist() {
        Type::from_str("unknown").unwrap();
    }
}
