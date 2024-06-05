use std::error::Error;
use std::fmt;

pub enum BufferError {}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            _ => todo!(),
        }
    }
}

impl fmt::Debug for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for BufferError {}
