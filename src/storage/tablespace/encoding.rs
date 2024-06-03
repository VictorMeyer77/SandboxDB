use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::to_string;

use crate::storage::tablespace::error::TablespaceError;

pub trait TablespaceEncoding<'a, T: Deserialize<'a> + Serialize>: Serialize {
    fn as_json(&self) -> Result<String, TablespaceError> {
        Ok(to_string(&self)?)
    }
    fn from_json(str: &'a str) -> Result<T, TablespaceError>;
    fn from_file(path: &PathBuf) -> Result<T, TablespaceError>;
}
