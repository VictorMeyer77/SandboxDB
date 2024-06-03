use std::error::Error;
use std::fmt;

pub enum TablespaceError {
    SerdeJson(serde_json::Error),
    FileError(std::io::Error),
    ObjectExists(String, String),
    ObjectNotFound(String, String),
}

impl fmt::Display for TablespaceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TablespaceError::SerdeJson(ref err) => write!(f, "Serde Json error: {}.", err),
            TablespaceError::FileError(ref err) => write!(f, "File error: {}.", err),
            TablespaceError::ObjectExists(ref object, ref name) => {
                write!(f, "{} {} already exists.", object, name)
            }
            TablespaceError::ObjectNotFound(ref object, ref name) => {
                write!(f, "{} {} not found.", object, name)
            }
        }
    }
}

impl fmt::Debug for TablespaceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for TablespaceError {}

impl From<serde_json::Error> for TablespaceError {
    fn from(value: serde_json::Error) -> TablespaceError {
        TablespaceError::SerdeJson(value)
    }
}

impl From<std::io::Error> for TablespaceError {
    fn from(value: std::io::Error) -> Self {
        TablespaceError::FileError(value)
    }
}
