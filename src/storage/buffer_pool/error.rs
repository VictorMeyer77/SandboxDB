use crate::storage::tablespace::error::TablespaceError;
use std::error::Error;
use std::fmt;

pub enum BufferError {
    UnknownCatalogTable(String),
    Tablespace(TablespaceError),
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferError::UnknownCatalogTable(ref msg) => {
                write!(f, "Table {} doesn't exist in catalog", msg)
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
