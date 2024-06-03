use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::metastore::database::Database;
use crate::storage::metastore::encoding::MetastoreEncoding;
use crate::storage::metastore::error::MetastoreError;
use crate::storage::metastore::meta::Meta;


const META_FOLDER: &str = ".meta";
const METASTORE_FILE_NAME: &str = "metastore";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metastore {
    location: PathBuf,
    database_paths: HashMap<String, PathBuf>,
    #[serde(skip)]
    databases: HashMap<String, Database>,
    #[serde(skip)]
    pub meta: Meta,
}

impl Metastore {
    pub fn build(location: &str) -> Result<Metastore, MetastoreError> {
        let location = fs::canonicalize(PathBuf::from(location))?;
        fs::create_dir_all(&location)?;
        let mut metastore = Metastore {
            location: location.clone(),
            database_paths: HashMap::new(),
            databases: HashMap::new(),
            meta: Meta::build(location.join(META_FOLDER))?,
        };
        metastore.save()?;
        Ok(metastore)
    }

    fn save(&mut self) -> Result<(), MetastoreError> {
        self.meta.save(METASTORE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    pub fn load_databases(&mut self) -> Result<(), MetastoreError> {
        for (name, path) in &self.database_paths {
            self.databases.insert(name.clone(), Database::from_file(&path)?);
        }
        Ok(())
    }

    pub fn new_database(&mut self, name: &str, location: Option<&str>) -> Result<Database, MetastoreError> {
        let location = location
            .unwrap_or(self.location.join(name).to_str().unwrap())
            .to_string();
        println!("{}", location);
        let database = Database::build(name, &location)?;
        println!("{:?}", database);
        match self.database_paths.entry(database.name.clone()) {
            Entry::Occupied(_) => Err(MetastoreError::ObjectExists(name.to_string())),
            Entry::Vacant(entry) => {
                entry.insert(database.location.clone());
                self.databases.insert(database.name.clone(), database.clone());
                self.save()?;
                Ok(database)
            }
        }
    }

    pub fn delete_database(&mut self, name: &str) -> Result<(), MetastoreError> {
        match self.database_paths.entry(name.to_string()) {
            Entry::Occupied(entry) => {
                fs::remove_dir_all(entry.get())?;
                self.databases.remove(name);
                self.database_paths.remove(name);
                Ok(())
            }
            Entry::Vacant(_) =>  Err(MetastoreError::ObjectNotFound(name.to_string()))
        }
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.database_paths.keys().cloned().collect()
    }

}

impl<'a> MetastoreEncoding<'a, Metastore> for Metastore {

    fn from_json(str: &str) -> Result<Metastore, MetastoreError> {
        let mut metastore: Metastore = from_str(str)?;
        metastore.meta = Meta::build(PathBuf::from(&metastore.location).join(META_FOLDER))?;
        Ok(metastore)
    }

    fn from_file(path: &PathBuf) -> Result<Metastore, MetastoreError> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(METASTORE_FILE_NAME))?;
        Metastore::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use crate::storage::schema::schema::Schema;
    use super::*;

    const TEST_PATH: &str = "target/tests/metastore";

    fn init_test_env(name: &str) -> PathBuf {
        delete_test_env(name);
        let path = PathBuf::from(TEST_PATH).join(name);
        let _ = fs::create_dir_all(&path);
        path
    }

    fn delete_test_env(name: &str) {
        let _ = fs::remove_dir_all(PathBuf::from(TEST_PATH).join(name));
    }
/*
    fn get_test_metastore() -> Metastore {
        let mut metastore = Metastore::build("./metastore_test");
        metastore
            .insert_database(&Database::build("database_test", "./database_test").unwrap())
            .unwrap();
        metastore
    }*/

    #[test]
    fn as_json_should_return_str_struct() {
        let path = init_test_env("as_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        println!("{:?}", absolute_path.to_str().unwrap());
        assert_eq!(
            Metastore::build(path.to_str().unwrap()).unwrap().as_json().unwrap(),
            format!("{{\"location\":\"{}\",\"database_paths\":{{}}}}", absolute_path.to_str().unwrap().replace("\\", "\\\\"))
        );
        delete_test_env("as_json");
    }


    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env("from_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Metastore::build(path.to_str().unwrap()).unwrap(),
            Metastore::from_json( &format!("{{\"location\":\"{}\",\"database_paths\":{{}}}}", absolute_path.to_str().unwrap().replace("\\", "\\\\")))
                .unwrap()
        );
        delete_test_env("from_json");
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env("from_file");
        let metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        assert_eq!(metastore, metastore);
        delete_test_env("from_file");
    }

    #[test]
    fn new_database_should_create_database_in_metastore_default() {
        let path = init_test_env("new_database_01");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("test", None).unwrap();
        assert!(Path::new(&absolute_path.join(".meta/database")).exists());
        assert!(metastore.database_paths.contains_key("test"));
        assert!(metastore.databases.contains_key("test"));
        delete_test_env("new_database_01")
    }

}
