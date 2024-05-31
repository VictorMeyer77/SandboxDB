use std::error::Error;
use std::fmt;

#[derive(Clone, PartialEq)]
pub enum FileError {
    PageOverflow(String),
    InvalidIndex(u32),
    InvalidSlot((u32, u32)),
    CorruptedTuple(String),
    MissingSchema,
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileError::PageOverflow(ref msg) => write!(f, "{}", msg),
            FileError::InvalidIndex(ref msg) => write!(f, "{:?} not found", msg),
            FileError::InvalidSlot(ref msg) => write!(f, "{:?} not found", msg),
            FileError::CorruptedTuple(ref msg) => write!(f, "{}", msg),
            FileError::MissingSchema => write!(f, "Need a schema to read these bytes"),
        }
    }
}

impl fmt::Debug for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for FileError {}
