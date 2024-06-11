use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::tablespace::database::Database;
use crate::storage::tablespace::encoding::Encoding;
use crate::storage::tablespace::error::Error;
use crate::storage::tablespace::meta::Meta;

const META_FOLDER: &str = ".meta";
const METASTORE_FILE_NAME: &str = "metastore";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metastore {
    pub location: PathBuf,
    pub database_paths: HashMap<String, PathBuf>,
    #[serde(skip)]
    pub databases: HashMap<String, Database>,
    #[serde(skip)]
    pub meta: Meta,
}

impl Metastore {
    pub fn build(location: &str) -> Result<Metastore, Error> {
        fs::create_dir_all(location)?;
        let location = fs::canonicalize(PathBuf::from(location))?;
        let mut metastore = Metastore {
            location: location.clone(),
            database_paths: HashMap::new(),
            databases: HashMap::new(),
            meta: Meta::build(location.join(META_FOLDER))?,
        };
        metastore.save()?;
        Ok(metastore)
    }

    fn save(&mut self) -> Result<(), Error> {
        self.meta.save(METASTORE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    pub fn load_databases(&mut self) -> Result<(), Error> {
        for (name, path) in &self.database_paths {
            self.databases
                .insert(name.clone(), Database::from_file(path)?);
        }
        Ok(())
    }

    pub fn new_database(&mut self, name: &str, location: Option<&str>) -> Result<Database, Error> {
        let location = location
            .unwrap_or(self.location.join(name).to_str().unwrap())
            .to_string();
        let database = Database::build(name, &location)?;
        match self.database_paths.entry(database.name.clone()) {
            Entry::Occupied(_) => Err(Error::ObjectExists(
                "Database".to_string(),
                name.to_string(),
            )),
            Entry::Vacant(entry) => {
                entry.insert(database.location.clone());
                self.databases
                    .insert(database.name.clone(), database.clone());
                self.save()?;
                Ok(database)
            }
        }
    }

    pub fn delete_database(&mut self, name: &str) -> Result<(), Error> {
        match self.database_paths.entry(name.to_string()) {
            Entry::Occupied(entry) => {
                fs::remove_dir_all(entry.get())?;
                self.databases.remove(name);
                self.database_paths.remove(name);
                Ok(())
            }
            Entry::Vacant(_) => Err(Error::ObjectExists(
                "Database".to_string(),
                name.to_string(),
            )),
        }
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.database_paths.keys().cloned().collect()
    }
}

impl<'a> Encoding<'a, Metastore> for Metastore {
    fn from_json(str: &str) -> Result<Metastore, Error> {
        let mut metastore: Metastore = from_str(str)?;
        metastore.meta = Meta::build(PathBuf::from(&metastore.location).join(META_FOLDER))?;
        Ok(metastore)
    }

    fn from_file(path: &Path) -> Result<Metastore, Error> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(METASTORE_FILE_NAME))?;
        Metastore::from_json(&file_str)
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::Path;

    use crate::storage::tests::{delete_test_env, init_test_env};

    use super::*;

    const TEST_PATH: &str = "target/tests/metastore";

    #[test]
    fn as_json_should_return_str_struct() {
        let path = init_test_env(TEST_PATH, "as_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Metastore::build(path.to_str().unwrap())
                .unwrap()
                .as_json()
                .unwrap(),
            format!(
                "{{\"location\":\"{}\",\"database_paths\":{{}}}}",
                absolute_path.to_str().unwrap().replace('\\', "\\\\")
            )
        );
        delete_test_env(TEST_PATH, "as_json");
    }

    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Metastore::build(path.to_str().unwrap()).unwrap(),
            Metastore::from_json(&format!(
                "{{\"location\":\"{}\",\"database_paths\":{{}}}}",
                absolute_path.to_str().unwrap().replace('\\', "\\\\")
            ))
            .unwrap()
        );
        delete_test_env(TEST_PATH, "from_json");
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_file");
        let metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        assert_eq!(metastore, metastore);
        delete_test_env(TEST_PATH, "from_file");
    }

    #[test]
    fn new_database_should_create_database_in_default() {
        let path = init_test_env(TEST_PATH, "new_database_01");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("test", None).unwrap();
        assert!(Path::new(&absolute_path.join("test/.meta/database")).exists());
        assert!(metastore.database_paths.contains_key("test"));
        assert!(metastore.databases.contains_key("test"));
        delete_test_env(TEST_PATH, "new_database_01")
    }

    #[test]
    fn new_database_should_create_database_in_location() {
        let path = init_test_env(TEST_PATH, "new_database_02");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore
            .new_database(
                "test",
                Some(path.join("some/other/path/test").to_str().unwrap()),
            )
            .unwrap();
        assert!(Path::new(&absolute_path.join("some/other/path/test/.meta/database")).exists());
        assert!(metastore.database_paths.contains_key("test"));
        assert!(metastore.databases.contains_key("test"));
        delete_test_env(TEST_PATH, "new_database_02")
    }

    #[test]
    #[should_panic]
    fn new_database_should_panic_if_database_exist() {
        let path = init_test_env(TEST_PATH, "new_database_03");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore
            .new_database(
                "test",
                Some(path.join("some/other/path/test").to_str().unwrap()),
            )
            .unwrap();
        metastore
            .new_database(
                "test",
                Some(path.join("some/other/path/test").to_str().unwrap()),
            )
            .unwrap();
        delete_test_env(TEST_PATH, "new_database_03")
    }

    #[test]
    fn delete_database_should_delete_database() {
        let path = init_test_env(TEST_PATH, "delete_database_01");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("test", None).unwrap();
        metastore.delete_database("test").unwrap();
        assert!(!Path::new(&absolute_path.join("test/")).exists());
        assert_eq!(metastore.databases.len(), 0);
        assert_eq!(metastore.database_paths.len(), 0);
        delete_test_env(TEST_PATH, "delete_table_01")
    }

    #[test]
    #[should_panic]
    fn delete_database_should_panic_if_not_exist_if_not_exists() {
        let path = init_test_env(TEST_PATH, "delete_database_02");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.delete_database("test").unwrap();
        delete_test_env(TEST_PATH, "delete_table_02")
    }

    #[test]
    fn list_databases_should_return_database_names() {
        let path = init_test_env(TEST_PATH, "list_databases");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("test01", None).unwrap();
        metastore.new_database("test02", None).unwrap();
        let list = metastore.list_databases();
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"test01".to_string()));
        assert!(list.contains(&"test02".to_string()));
        delete_test_env(TEST_PATH, "list_databases")
    }
}
