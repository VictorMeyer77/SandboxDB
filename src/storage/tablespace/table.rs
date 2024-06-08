use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::schema::schema::Schema;
use crate::storage::tablespace::encoding::TablespaceEncoding;
use crate::storage::tablespace::error::Error;
use crate::storage::tablespace::meta::Meta;

const META_FOLDER: &str = ".meta";
const TABLE_FILE_NAME: &str = "table";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: Schema,
    pub location: PathBuf,
    #[serde(skip)]
    pub file_paths: HashMap<String, PathBuf>,
    #[serde(skip)]
    pub meta: Meta,
}

impl Table {
    pub fn build(name: &str, location: &str, schema: &Schema) -> Result<Table, Error> {
        fs::create_dir_all(&location)?;
        let location = fs::canonicalize(PathBuf::from(location))?;
        let mut table = Table {
            name: name.to_string(),
            schema: schema.clone(),
            location: location.clone(),
            file_paths: HashMap::new(),
            meta: Meta::build(location.join(META_FOLDER))?,
        };
        table.save()?;
        Ok(table)
    }

    fn save(&mut self) -> Result<(), Error> {
        self.meta.save(TABLE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    pub fn load_file_paths(&mut self) -> Result<(), Error> {
        for entry in fs::read_dir(&self.location)? {
            let path = entry?.path();
            if path.is_file() {
                self.file_paths.insert(
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    path,
                );
            }
        }
        Ok(())
    }

    pub fn new_file(&mut self) -> Result<(String, PathBuf), Error> {
        let file_name = self.generate_file_name();
        let file_path = self.location.join(&file_name);
        fs::File::create(&file_path)?;
        self.file_paths
            .insert(file_name.to_string(), file_path.clone());
        self.save()?;
        Ok((file_name, file_path))
    }

    pub fn delete_file(&mut self, name: &str) -> Result<(), Error> {
        fs::remove_file(
            self.file_paths
                .get(name)
                .ok_or(Error::ObjectNotFound("File".to_string(), name.to_string()))?,
        )?;
        self.file_paths.remove(name);
        self.save()?;
        Ok(())
    }

    pub fn list_files(&self) -> Vec<String> {
        self.file_paths.keys().cloned().collect()
    }

    fn generate_file_name(&self) -> String {
        let max_file = self.file_paths.keys().max();
        match max_file {
            None => "0".to_string(),
            Some(value) => (value.parse::<u32>().unwrap() + 1).to_string(),
        }
    }
}

impl TablespaceEncoding<'_, Table> for Table {
    fn from_json(str: &str) -> Result<Table, Error> {
        let mut table: Table = from_str(str)?;
        table.meta = Meta::build(PathBuf::from(&table.location).join(META_FOLDER))?;
        Ok(table)
    }

    fn from_file(path: &PathBuf) -> Result<Table, Error> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(TABLE_FILE_NAME))?;
        Table::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};

    use super::*;

    const TEST_PATH: &str = "target/tests/table";

    #[test]
    fn as_json_should_return_str_struct() {
        let path = init_test_env(TEST_PATH, "as_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Table::build("test", path.to_str().unwrap(), &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap()).unwrap()
                .as_json()
                .unwrap(),
            format!("{{\"name\":\"test\",\"schema\":{{\"fields\":[{{\"name\":\"id\",\"_type\":\"BIGINT\"}},{{\"name\":\"cost\",\"_type\":\"FLOAT\"}},{{\"name\":\"available\",\"_type\":\"BOOLEAN\"}}]}},\"location\":\"{}\"}}", absolute_path.to_str().unwrap().replace("\\", "\\\\")),
        );
        delete_test_env(TEST_PATH, "as_json");
    }

    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Table::build("test", path.to_str().unwrap(),  &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap()).unwrap(),
            Table::from_json(&format!("{{\"name\":\"test\",\"schema\":{{\"fields\":[{{\"name\":\"id\",\"_type\":\"BIGINT\"}},{{\"name\":\"cost\",\"_type\":\"FLOAT\"}},{{\"name\":\"available\",\"_type\":\"BOOLEAN\"}}]}},\"location\":\"{}\"}}", absolute_path.to_str().unwrap().replace("\\", "\\\\")),
            ).unwrap()
        );
        delete_test_env(TEST_PATH, "from_json");
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env(TEST_PATH, "from_file");
        let table = Table::build(
            "test",
            path.to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        assert_eq!(table, Table::from_file(&path).unwrap());
        delete_test_env(TEST_PATH, "from_file");
    }

    #[test]
    fn new_file_should_create_empty_new_file() {
        let path = init_test_env(TEST_PATH, "new_file");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        let (file_name, file_path) = table.new_file().unwrap();
        assert_eq!(file_name, "0");
        assert_eq!(file_path, absolute_path.join("test/0"));
        let (file_name, file_path) = table.new_file().unwrap();
        assert_eq!(file_name, "1");
        assert_eq!(file_path, absolute_path.join("test/1"));
        delete_test_env(TEST_PATH, "new_file");
    }

    #[test]
    fn delete_file_should_remove_file() {
        let path = init_test_env(TEST_PATH, "delete_file");
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        let (file_name_0, _) = table.new_file().unwrap();
        let (file_name_1, _) = table.new_file().unwrap();
        table.delete_file(&file_name_0).unwrap();
        table.delete_file(&file_name_1).unwrap();
        assert_eq!(table.list_files().len(), 0);
        assert!(!path.join("test/0").exists());
        delete_test_env(TEST_PATH, "delete_file");
    }

    #[test]
    fn load_file_paths_should_load_files_hashmap() {
        let path = init_test_env(TEST_PATH, "load_file_paths");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        let _ = fs::File::create(path.join("test/0"));
        let _ = fs::File::create(path.join("test/1"));
        let _ = fs::File::create(path.join("test/2"));
        table.load_file_paths().unwrap();
        assert_eq!(table.file_paths["0"], absolute_path.join("test/0"));
        assert_eq!(table.file_paths["1"], absolute_path.join("test/1"));
        assert_eq!(table.file_paths["2"], absolute_path.join("test/2"));
        delete_test_env(TEST_PATH, "load_file_paths");
    }

    #[test]
    fn list_should_return_file_names() {
        let path = init_test_env(TEST_PATH, "list");
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        table.new_file().unwrap();
        table.new_file().unwrap();
        let list = table.list_files();
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"0".to_string()));
        assert!(list.contains(&"1".to_string()));
        delete_test_env(TEST_PATH, "list");
    }
}
