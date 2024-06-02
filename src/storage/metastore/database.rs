use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::metastore::encoding::MetastoreEncoding;
use crate::storage::metastore::error::MetastoreError;
use crate::storage::metastore::meta::Meta;
use crate::storage::metastore::table::Table;
use crate::storage::schema::schema::Schema;

const META_FOLDER: &str = ".meta";
const DATABASE_FILE_NAME: &str = "database";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub location: PathBuf,
    pub table_paths: HashMap<String, PathBuf>,
    #[serde(skip)]
    pub tables: HashMap<String, Table>,
    #[serde(skip)]
    pub meta: Meta,
}

impl Database {
    pub fn build(name: &str, location: &str) -> Result<Database, MetastoreError> {
        fs::create_dir_all(&location)?;
        let mut database = Database {
            name: name.to_string(),
            location: PathBuf::from(location),
            table_paths: HashMap::new(),
            tables: HashMap::new(),
            meta: Meta::build(PathBuf::from(location).join(META_FOLDER))?,
        };
        database.save()?;
        Ok(database)
    }

    fn save(&mut self) -> Result<(), MetastoreError> {
        self.meta.save(DATABASE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    fn load_tables(&mut self) -> Result<(), MetastoreError> {
        for (name, path) in &self.table_paths {
            let table = Table::from_file(&path)?;
            self.tables.insert(name.clone(), table);
        }
        Ok(())
    }

    pub fn new_table(
        &mut self,
        name: &str,
        location: Option<&str>,
        schema: &Schema,
    ) -> Result<Table, MetastoreError> {
        let location = location
            .unwrap_or(self.location.join(name).to_str().unwrap())
            .to_string();
        let table = Table::build(name, &location, schema)?;
        match self.table_paths.entry(table.name.clone()) {
            Entry::Occupied(_) => Err(MetastoreError::ObjectExists(name.to_string())),
            Entry::Vacant(entry) => {
                entry.insert(table.location.clone());
                self.tables.insert(table.name.clone(), table.clone());
                self.save()?;
                Ok(table)
            }
        }
    }

    pub fn delete_table(&mut self, name: &str) -> Result<(), MetastoreError> {
        match self.table_paths.entry(name.to_string()) {
            Entry::Occupied(entry) => {
                fs::remove_dir_all(entry.get()).unwrap();
                self.tables.remove(name);
                self.table_paths.remove(name);
                Ok(())
            }
            Entry::Vacant(_) => Err(MetastoreError::ObjectNotFound(name.to_string())),
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.table_paths.keys().cloned().collect()
    }
}

impl<'a> MetastoreEncoding<'a, Database> for Database {
    fn from_json(str: &str) -> Result<Database, MetastoreError> {
        let mut database: Database = from_str(str)?;
        database.meta = Meta::build(PathBuf::from(&database.location).join(META_FOLDER))?;
        Ok(database)
    }
    fn from_file(path: &PathBuf) -> Result<Database, MetastoreError> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(DATABASE_FILE_NAME))?;
        Database::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::storage::metastore::database::Database;
    use crate::storage::metastore::encoding::MetastoreEncoding;
    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::schema::Schema;

    const TEST_PATH: &str = "target/tests/database";

    fn init_test_env(name: &str) -> PathBuf {
        delete_test_env(name);
        let path = PathBuf::from(TEST_PATH).join(name);
        let _ = fs::create_dir_all(&path);
        path
    }

    fn delete_test_env(name: &str) {
        let _ = fs::remove_dir_all(PathBuf::from(TEST_PATH).join(name));
    }

    #[test]
    fn as_json_should_return_str_struct() {
        let path = init_test_env("as_json");
        let database = Database::build("test", path.to_str().unwrap()).unwrap();
        assert_eq!(
            Database::build("test", path.to_str().unwrap())
                .unwrap()
                .as_json()
                .unwrap(),
            "{\"name\":\"test\",\"location\":\"target/tests/database/as_json\",\"table_paths\":{}}"
        )
    }

    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env("as_json");
        assert_eq!(
            Database::build("test", path.to_str().unwrap()).unwrap(),
            Database::from_json("{\"name\":\"test\",\"location\":\"target/tests/database/as_json\",\"table_paths\":{}}").unwrap()
        )
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env("from_file");
        let database = Database::build("test", path.to_str().unwrap()).unwrap();
        assert_eq!(database, Database::from_file(&path).unwrap());
        delete_test_env("from_file");
    }

    #[test]
    fn new_table_should_create_table_in_database_default() {
        let path = init_test_env("new_table_01");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database
            .new_table(
                "test",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        assert!(Path::new(&absolute_path.join("test/.meta/table")).exists());
        assert!(database.table_paths.contains_key("test"));
        assert!(database.tables.contains_key("test"));
        delete_test_env("new_table_01")
    }

    #[test]
    fn new_table_should_create_table_in_location() {
        let path = init_test_env("new_table_02");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database
            .new_table(
                "test",
                Some(path.join("other").to_str().unwrap()),
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        assert!(Path::new(&absolute_path.join("other/.meta/table")).exists());
        assert!(database.table_paths.contains_key("test"));
        assert!(database.tables.contains_key("test"));
        delete_test_env("new_table_02")
    }

    #[test]
    #[should_panic]
    fn new_table_should_panic_if_table_exists() {
        let path = init_test_env("new_table_03");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database
            .new_table(
                "test",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        database
            .new_table(
                "test",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        delete_test_env("new_table_03")
    }

    #[test]
    fn delete_table_should_remove_table() {
        let path = init_test_env("delete_table");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database
            .new_table(
                "test",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        database.delete_table("test").unwrap();
        assert!(!Path::new(&absolute_path.join("test/")).exists());
        delete_test_env("delete_table")
    }

    #[test]
    #[should_panic]
    fn delete_table_should_panic_if_not_exist() {
        let path = init_test_env("delete_table");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database.delete_table("test").unwrap();
        delete_test_env("delete_table")
    }
}
