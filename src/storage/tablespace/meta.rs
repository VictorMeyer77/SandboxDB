use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::storage::tablespace::error::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Meta {
    pub location: PathBuf,
    meta_paths: HashMap<String, PathBuf>,
}

impl Meta {
    pub fn build(location: PathBuf) -> Result<Meta, Error> {
        fs::create_dir_all(&location)?;
        let mut meta = Meta {
            location: fs::canonicalize(PathBuf::from(location))?,
            meta_paths: HashMap::new(),
        };
        meta.load_meta_paths()?;
        Ok(meta)
    }

    fn load_meta_paths(&mut self) -> Result<(), Error> {
        for entry in fs::read_dir(&self.location)? {
            let path = entry?.path();
            if path.is_file() {
                self.meta_paths.insert(
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    path,
                );
            }
        }
        Ok(())
    }

    pub fn save(&mut self, name: &str, meta_str: &str) -> Result<(), Error> {
        let mut file = fs::File::create(self.location.join(name))?;
        file.write_all(meta_str.as_bytes())?;
        self.meta_paths
            .insert(name.to_string(), self.location.join(name));
        Ok(())
    }

    pub fn load(&self, name: &str) -> Result<String, Error> {
        Ok(fs::read_to_string(self.meta_paths.get(name).ok_or(
            Error::ObjectNotFound("Meta".to_string(), name.to_string()),
        )?)?)
    }

    pub fn delete(&mut self, name: &str) -> Result<(), Error> {
        fs::remove_file(
            self.meta_paths
                .get(name)
                .ok_or(Error::ObjectNotFound("Meta".to_string(), name.to_string()))?,
        )?;
        self.meta_paths.remove(name);
        Ok(())
    }

    pub fn list(&self) -> Vec<String> {
        self.meta_paths.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};

    use super::*;

    const TEST_PATH: &str = "target/tests/meta";

    #[test]
    fn load_meta_paths_should_load_map() {
        let path = init_test_env(TEST_PATH, "load_meta_paths");
        let mut meta_01 = Meta::build(path.clone()).unwrap();
        meta_01.save("test_1", "content").unwrap();
        meta_01.save("test_2", "content").unwrap();
        let meta_02 = Meta::build(path).unwrap();
        assert_eq!(meta_01, meta_02);
        delete_test_env(TEST_PATH, "load_meta_paths");
    }

    #[test]
    fn save_should_add_meta_file() {
        let path = init_test_env(TEST_PATH, "save");
        let mut meta = Meta::build(path.clone()).unwrap();
        meta.save("test", "content").unwrap();
        let content = fs::read_to_string(path.join("test")).unwrap();
        assert_eq!(content, "content".to_string());
        assert_eq!(
            *meta.meta_paths.get("test").unwrap(),
            fs::canonicalize(path.join("test")).unwrap()
        );
        delete_test_env(TEST_PATH, "save");
    }

    #[test]
    fn load_should_return_file_content() {
        let path = init_test_env(TEST_PATH, "load");
        let mut meta = Meta::build(path).unwrap();
        meta.save("test", "content").unwrap();
        let content = meta.load("test").unwrap();
        assert_eq!(content, "content".to_string());
        delete_test_env(TEST_PATH, "load");
    }

    #[test]
    fn delete_should_remove_meta() {
        let path = init_test_env(TEST_PATH, "delete");
        let mut meta = Meta::build(path.clone()).unwrap();
        meta.save("test", "content").unwrap();
        meta.delete("test").unwrap();
        assert!(!path.join("test").exists());
        assert!(!meta.meta_paths.contains_key("test"));
        delete_test_env(TEST_PATH, "delete");
    }

    #[test]
    fn list_should_return_meta_names() {
        let path = init_test_env(TEST_PATH, "list");
        let mut meta = Meta::build(path).unwrap();
        meta.save("test_1", "content").unwrap();
        meta.save("test_2", "content").unwrap();
        let list: Vec<String> = meta.list();
        assert!(list.contains(&"test_1".to_string()));
        assert!(list.contains(&"test_2".to_string()));
        delete_test_env(TEST_PATH, "list");
    }
}
