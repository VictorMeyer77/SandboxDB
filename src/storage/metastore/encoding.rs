use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};

use crate::storage::metastore::error::MetastoreError;

pub trait MetastoreEncoding<'a, T: Deserialize<'a> + Serialize>: Serialize {
    fn as_json(&self) -> Result<String, MetastoreError> {
        Ok(to_string(&self)?)
    }
    fn from_json(str: &'a str) -> Result<T, MetastoreError> {
        Ok(from_str(str)?)
    }

    fn from_file(path: &PathBuf) -> Result<T, MetastoreError>;
}
