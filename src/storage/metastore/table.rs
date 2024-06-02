use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use crate::storage::metastore::encoding::MetastoreEncoding;
use crate::storage::metastore::error::MetastoreError;
use crate::storage::metastore::meta::Meta;
use crate::storage::schema::schema::Schema;

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
    pub fn build(name: &str, location: &str, schema: &Schema) -> Result<Table, MetastoreError> {
        fs::create_dir_all(&location)?;
        let mut table = Table {
            name: name.to_string(),
            schema: schema.clone(),
            location: fs::canonicalize(PathBuf::from(location))?,
            file_paths: HashMap::new(),
            meta: Meta::build(PathBuf::from(location).join(META_FOLDER))?,
        };
        table.save()?;
        Ok(table)
    }

    fn save(&mut self) -> Result<(), MetastoreError> {
        self.meta.save(TABLE_FILE_NAME, &self.as_json()?)?;
        Ok(())
    }

    fn load_file_paths(&mut self) -> Result<(), MetastoreError> {
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

    pub fn new_file(&mut self) -> Result<(String, PathBuf), MetastoreError> {
        let file_name = self.generate_file_name();
        let file_path = self.location.join(&file_name);
        fs::File::create(&file_path)?;
        self.file_paths
            .insert(file_name.to_string(), file_path.clone());
        self.save()?;
        Ok((file_name, file_path))
    }

    pub fn delete_file(&mut self, name: &str) -> Result<(), MetastoreError> {
        fs::remove_file(
            self.file_paths
                .get(name)
                .ok_or(MetastoreError::ObjectNotFound(format!("File {}", name)))?,
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

impl MetastoreEncoding<'_, Table> for Table {
    fn from_json(str: &str) -> Result<Table, MetastoreError> {
        let mut table: Table = from_str(str)?;
        table.meta = Meta::build(PathBuf::from(&table.location).join(META_FOLDER))?;
        Ok(table)
    }

    fn from_file(path: &PathBuf) -> Result<Table, MetastoreError> {
        let file_str = fs::read_to_string(path.join(META_FOLDER).join(TABLE_FILE_NAME))?;
        Table::from_json(&file_str)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::schema::encoding::SchemaEncoding;

    use super::*;

    const TEST_PATH: &str = "target/tests/table";

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
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Table::build("test", path.to_str().unwrap(),  &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap()).unwrap()
                .as_json()
                .unwrap(),
            format!("{{\"name\":\"test\",\"schema\":{{\"fields\":[{{\"name\":\"id\",\"_type\":\"BIGINT\"}},{{\"name\":\"cost\",\"_type\":\"FLOAT\"}},{{\"name\":\"available\",\"_type\":\"BOOLEAN\"}}]}},\"location\":\"{}\"}}", absolute_path.to_str().unwrap()),
        );
        delete_test_env("as_json");
    }

    #[test]
    fn from_json_should_return_struct() {
        let path = init_test_env("from_json");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Table::build("test", path.to_str().unwrap(),  &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap()).unwrap(),
            Table::from_json(&format!("{{\"name\":\"test\",\"schema\":{{\"fields\":[{{\"name\":\"id\",\"_type\":\"BIGINT\"}},{{\"name\":\"cost\",\"_type\":\"FLOAT\"}},{{\"name\":\"available\",\"_type\":\"BOOLEAN\"}}]}},\"location\":\"{}\"}}", absolute_path.to_str().unwrap()),
            ).unwrap()
        );
        delete_test_env("from_json");
    }

    #[test]
    fn from_file_should_return_struct() {
        let path = init_test_env("from_file");
        let table = Table::build(
            "test",
            path.to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        assert_eq!(table, Table::from_file(&path).unwrap());
        delete_test_env("from_file");
    }

    #[test]
    fn new_file_should_create_empty_new_file() {
        let path = init_test_env("new_file");
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
        delete_test_env("new_file");
    }

    #[test]
    fn delete_file_should_remove_file() {
        let path = init_test_env("delete_file");
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        let (file_name_0, file_path) = table.new_file().unwrap();
        let (file_name_1, file_path) = table.new_file().unwrap();
        table.delete_file(&file_name_0).unwrap();
        table.delete_file(&file_name_1).unwrap();
        assert_eq!(table.list_files().len(), 0);
        assert!(!path.join("test/0").exists());
        delete_test_env("delete_file");
    }

    #[test]
    fn load_file_paths_should_load_files_hashmap() {
        let path = init_test_env("load_file_paths");
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
        delete_test_env("load_file_paths");
    }

    #[test]
    fn list_should_return_file_names() {
        let path = init_test_env("list");
        let mut table = Table::build(
            "test",
            path.join("test").to_str().unwrap(),
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
        let (file_name, file_path) = table.new_file().unwrap();
        let (file_name, file_path) = table.new_file().unwrap();
        let list = table.list_files();
        assert!(list.contains(&"0".to_string()));
        assert!(list.contains(&"1".to_string()));
        delete_test_env("list");
    }
}