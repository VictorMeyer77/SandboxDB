use std::fmt;


pub enum Error {
    PageOverflow(String),
    InvalidIndex(u32),
    InvalidSlot((u32, u32)),
    CorruptedTuple(String),
    MissingSchema,
    Bincode(Box<bincode::ErrorKind>)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PageOverflow(ref msg) => write!(f, "{}", msg),
            Error::InvalidIndex(ref msg) => write!(f, "{:?} not found", msg),
            Error::InvalidSlot(ref msg) => write!(f, "{:?} not found", msg),
            Error::CorruptedTuple(ref msg) => write!(f, "{}", msg),
            Error::MissingSchema => write!(f, "Need a schema to read these bytes"),
            Error::Bincode(ref err) => write!(f, "Bincode error {}", err),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}


impl From<Box<bincode::ErrorKind>> for Error {
    fn from(value: Box<bincode::ErrorKind>) -> Self {
        Error::Bincode(value)
    }
}
