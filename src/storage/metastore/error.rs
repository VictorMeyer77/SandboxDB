use std::error::Error;
use std::fmt;

pub enum MetastoreError {
    SerdeJson(serde_json::Error),
    FileError(std::io::Error),
    ObjectExists(String),
    ObjectNotFound(String),
}

impl fmt::Display for MetastoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MetastoreError::SerdeJson(ref err) => write!(f, "Serde Json error: {}", err),
            MetastoreError::FileError(ref err) => write!(f, "File error: {}", err),
            MetastoreError::ObjectExists(ref msg) => write!(f, "{} already exists.", msg),
            MetastoreError::ObjectNotFound(ref msg) => write!(f, "{} not found.", msg),
        }
    }
}

impl fmt::Debug for MetastoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for MetastoreError {}

impl From<serde_json::Error> for MetastoreError {
    fn from(value: serde_json::Error) -> MetastoreError {
        MetastoreError::SerdeJson(value)
    }
}

impl From<std::io::Error> for MetastoreError {
    fn from(value: std::io::Error) -> Self {
        MetastoreError::FileError(value)
    }
}
