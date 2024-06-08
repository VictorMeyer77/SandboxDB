use std::fmt;

pub enum Error {
    SerdeJson(serde_json::Error),
    FileError(std::io::Error),
    ObjectExists(String, String),
    ObjectNotFound(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SerdeJson(ref err) => write!(f, "Serde Json error: {}.", err),
            Error::FileError(ref err) => write!(f, "File error: {}.", err),
            Error::ObjectExists(ref object, ref name) => {
                write!(f, "{} {} already exists.", object, name)
            }
            Error::ObjectNotFound(ref object, ref name) => {
                write!(f, "{} {} not found.", object, name)
            }
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Error {
        Error::SerdeJson(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::FileError(value)
    }
}
