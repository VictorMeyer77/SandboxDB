use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::schema::schema::Schema;
use crate::storage::tablespace::encoding::TablespaceEncoding;
use crate::storage::tablespace::error::TablespaceError;
use crate::storage::tablespace::meta::Meta;
use crate::storage::tablespace::table::Table;

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
    pub fn build(name: &str, location: &str) -> Result<Database, TablespaceError> {
        fs::create_dir_all(&location)?;
        let location = fs::canonicalize(PathBuf::from(location))?;
        let mut database = Database {
            name: name.to_string(),
            location: location.clone(),
            table_paths: HashMap::new(),
            tables: HashMap::new(),
            meta: Meta::build(location.join(META_FOLDER))?,
        };
        database.save()?;
        Ok(database)
    }

    fn save(&mut self) -> Result<(), TablespaceError> {
        self.meta.save(DATABASE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    fn load_tables(&mut self) -> Result<(), TablespaceError> {
        for (name, path) in &self.table_paths {
            self.tables.insert(name.clone(), Table::from_file(&path)?);
        }
        Ok(())
    }

    pub fn new_table(
        &mut self,
        name: &str,
        location: Option<&str>,
        schema: &Schema,
    ) -> Result<Table, TablespaceError> {
        let location = location
            .unwrap_or(self.location.join(name).to_str().unwrap())
            .to_string();
        let table = Table::build(name, &location, schema)?;
        match self.table_paths.entry(table.name.clone()) {
            Entry::Occupied(_) => Err(TablespaceError::ObjectExists(
                "Table".to_string(),
                name.to_string(),
            )),
            Entry::Vacant(entry) => {
                entry.insert(table.location.clone());
                self.tables.insert(table.name.clone(), table.clone());
                self.save()?;
                Ok(table)
            }
        }
    }

    pub fn delete_table(&mut self, name: &str) -> Result<(), TablespaceError> {
        match self.table_paths.entry(name.to_string()) {
            Entry::Occupied(entry) => {
                fs::remove_dir_all(entry.get())?;
                self.tables.remove(name);
                self.table_paths.remove(name);
                Ok(())
            }
            Entry::Vacant(_) => Err(TablespaceError::ObjectNotFound(
                "Table".to_string(),
                name.to_string(),
            )),
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.table_paths.keys().cloned().collect()
    }
}

impl<'a> TablespaceEncoding<'a, Database> for Database {
    fn from_json(str: &str) -> Result<Database, TablespaceError> {
        let mut database: Database = from_str(str)?;
        database.meta = Meta::build(PathBuf::from(&database.location).join(META_FOLDER))?;
        Ok(database)
    }
    fn from_file(path: &PathBuf) -> Result<Database, TablespaceError> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(DATABASE_FILE_NAME))?;
        Database::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::schema::Schema;
    use crate::storage::tablespace::database::Database;
    use crate::storage::tablespace::encoding::TablespaceEncoding;
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};

    const TEST_PATH: &str = "target/tests/database";

    #[test]
    fn as_json_should_return_str_struct() {
        let path = init_test_env(TEST_PATH, "as_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Database::build("test", path.to_str().unwrap())
                .unwrap()
                .as_json()
                .unwrap(),
            format!(
                "{{\"name\":\"test\",\"location\":\"{}\",\"table_paths\":{{}}}}",
                absolute_path.to_str().unwrap().replace("\\", "\\\\")
            )
        );
        delete_test_env(TEST_PATH, "as_json");
    }

    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Database::build("test", path.to_str().unwrap()).unwrap(),
            Database::from_json(&format!(
                "{{\"name\":\"test\",\"location\":\"{}\",\"table_paths\":{{}}}}",
                absolute_path.to_str().unwrap().replace("\\", "\\\\")
            ))
            .unwrap()
        );
        delete_test_env(TEST_PATH, "from_json");
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_file");
        let database = Database::build("test", path.to_str().unwrap()).unwrap();
        assert_eq!(database, Database::from_file(&path).unwrap());
        delete_test_env(TEST_PATH, "from_file");
    }

    #[test]
    fn new_table_should_create_table_in_database_default() {
        let path = init_test_env(TEST_PATH, "new_table_01");
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
        delete_test_env(TEST_PATH, "new_table_01")
    }

    #[test]
    fn new_table_should_create_table_in_location() {
        let path = init_test_env(TEST_PATH, "new_table_02");
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
        delete_test_env(TEST_PATH, "new_table_02")
    }

    #[test]
    #[should_panic]
    fn new_table_should_panic_if_table_exists() {
        let path = init_test_env(TEST_PATH, "new_table_03");
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
        delete_test_env(TEST_PATH, "new_table_03")
    }

    #[test]
    fn delete_table_should_remove_table() {
        let path = init_test_env(TEST_PATH, "delete_table_01");
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
        assert_eq!(database.table_paths.len(), 0);
        assert_eq!(database.tables.len(), 0);
        delete_test_env(TEST_PATH, "delete_table_01")
    }

    #[test]
    #[should_panic]
    fn delete_table_should_panic_if_not_exist() {
        let path = init_test_env(TEST_PATH, "delete_table_02");
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database.delete_table("test").unwrap();
        delete_test_env(TEST_PATH, "delete_table_02")
    }

    #[test]
    fn list_tables_should_return_table_names() {
        let path = init_test_env(TEST_PATH, "list_tables");
        let mut database = Database::build("test", path.to_str().unwrap()).unwrap();
        database
            .new_table(
                "test01",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        database
            .new_table(
                "test02",
                None,
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
            )
            .unwrap();
        let list = database.list_tables();
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"test01".to_string()));
        assert!(list.contains(&"test02".to_string()));
        delete_test_env(TEST_PATH, "list_tables")
    }
}
