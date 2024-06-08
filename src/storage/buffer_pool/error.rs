use std::error::Error;
use std::fmt;

use crate::storage::tablespace::error::TablespaceError;

pub enum BufferError {
    UnknownTableKey(u32),
    Tablespace(TablespaceError),
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferError::UnknownTableKey(ref msg) => {
                write!(f, "Table {} doesn't buffered.", msg)
            }
            BufferError::Tablespace(ref err) => write!(f, "Tablespace error: {}.", err),
        }
    }
}

impl fmt::Debug for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for BufferError {}

impl From<TablespaceError> for BufferError {
    fn from(value: TablespaceError) -> Self {
        BufferError::Tablespace(value)
    }
}
