use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::to_string;

use crate::storage::tablespace::error::Error;

pub trait TablespaceEncoding<'a, T: Deserialize<'a> + Serialize>: Serialize {
    fn as_json(&self) -> Result<String, Error> {
        Ok(to_string(&self)?)
    }
    fn from_json(str: &'a str) -> Result<T, Error>;
    fn from_file(path: &PathBuf) -> Result<T, Error>;
}
