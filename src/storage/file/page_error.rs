use std::error::Error;
use std::fmt;

use crate::storage::file::slot::Slot;

#[derive(Clone, PartialEq)]
pub enum PageError {
    PageOverflow(String),
    InvalidSlot(Slot),
    CorruptedTuple(String),
    MissingSchema,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageError::PageOverflow(ref msg) => write!(f, "{}", msg),
            PageError::InvalidSlot(ref msg) => write!(f, "{:?} not found", msg),
            PageError::CorruptedTuple(ref msg) => write!(f, "{}", msg),
            PageError::MissingSchema => write!(f, "Need a schema to read these bytes"),
        }
    }
}

impl fmt::Debug for PageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for PageError {}
