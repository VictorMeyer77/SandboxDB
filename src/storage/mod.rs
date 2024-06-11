pub mod buffer;
pub mod file;
pub mod schema;
pub mod tablespace;

#[cfg(test)]
pub mod tests {
    use crate::storage::schema::encoding::Encoding;
    use crate::storage::schema::Schema;
    use std::fs;
    use std::path::PathBuf;

    pub fn init_test_env(test_path: &str, name: &str) -> PathBuf {
        delete_test_env(name, test_path);
        let path = PathBuf::from(test_path).join(name);
        let _ = fs::create_dir_all(&path);
        path
    }

    pub fn delete_test_env(test_path: &str, name: &str) {
        let _ = fs::remove_dir_all(PathBuf::from(test_path).join(name));
    }

    pub fn get_test_schema() -> Schema {
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap()
    }
}
