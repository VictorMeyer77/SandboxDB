use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::storage::metastore::database::Database;
use crate::storage::metastore::encoding::MetastoreEncoding;
use crate::storage::metastore::error::MetastoreError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metastore {
    location: String,
    #[serde(skip_serializing)]
    databases: HashMap<String, Database>,
}

impl Metastore {
    pub fn build(location: &str) -> Metastore {
        Metastore {
            location: location.to_string(),
            databases: HashMap::new(),
        }
    }

    pub fn insert_database(&mut self, database: &Database) -> Result<(), MetastoreError> {
        self.databases
            .insert(database.name.clone(), database.clone());
        Ok(())
    }
}

impl<'a> MetastoreEncoding<'a, Metastore> for Metastore {
    fn from_file(path: &PathBuf) -> Result<Metastore, MetastoreError> {
        let file_str = fs::read_to_string(path)?;
        Metastore::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_metastore() -> Metastore {
        let mut metastore = Metastore::build("./metastore_test");
        metastore
            .insert_database(&Database::build("database_test", "./database_test").unwrap())
            .unwrap();
        metastore
    }

    #[test]
    fn as_json_should_return_str_struct() {
        assert_eq!(
            get_test_metastore().as_json().unwrap(),
            "{\"databases\":{\"database_test\":\"./database_test\"}}"
        )
    }

    #[test]
    fn from_json_should_return_struct() {
        assert_eq!(
            get_test_metastore(),
            Metastore::from_json("{\"databases\":{\"database_test\":\"./database_test\"}}")
                .unwrap()
        )
    }
}
