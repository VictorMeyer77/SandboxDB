use std::fmt;

#[derive(Clone, PartialEq)]
pub enum Error {
    InvalidType(String),
    InvalidField(String),
    InvalidSchema(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidType(ref msg) => write!(f, "{}", msg),
            Error::InvalidField(ref msg) => write!(f, "{}", msg),
            Error::InvalidSchema(ref msg) => write!(f, "{}", msg),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}
