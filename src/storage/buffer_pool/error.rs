use std::fmt;

use crate::storage::tablespace;

pub enum Error {
    UnknownTableKey(u32),
    Tablespace(tablespace::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::UnknownTableKey(ref msg) => {
                write!(f, "Table {} doesn't buffered.", msg)
            }
            Error::Tablespace(ref err) => write!(f, "Tablespace error: {}.", err),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl From<tablespace::error::Error> for Error {
    fn from(value: tablespace::error::Error) -> Self {
        Error::Tablespace(value)
    }
}
