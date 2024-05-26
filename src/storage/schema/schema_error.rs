use std::error::Error;
use std::fmt;

#[derive(Clone, PartialEq)]
pub enum SchemaError {
    InvalidType(String),
    InvalidField(String),
    InvalidSchema(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaError::InvalidType(ref msg) => write!(f, "{}", msg),
            SchemaError::InvalidField(ref msg) => write!(f, "{}", msg),
            SchemaError::InvalidSchema(ref msg) => write!(f, "{}", msg),
        }
    }
}

impl fmt::Debug for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for SchemaError {}
