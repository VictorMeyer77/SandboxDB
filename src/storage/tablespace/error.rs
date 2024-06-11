use std::fmt;

pub enum Error {
    SerdeJson(serde_json::Error),
    FileError(std::io::Error),
    ObjectExists(String, String),
    ObjectNotFound(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SerdeJson(ref err) => write!(f, "Serde Json error: {}.", err),
            Error::FileError(ref err) => write!(f, "File error: {}.", err),
            Error::ObjectExists(ref object, ref name) => {
                write!(f, "{} {} already exists.", object, name)
            }
            Error::ObjectNotFound(ref object, ref name) => {
                write!(f, "{} {} not found.", object, name)
            }
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Error {
        Error::SerdeJson(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::FileError(value)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::storage::schema::encoding::Encoding as SchemaEncoding;
    use crate::storage::schema::Schema;
    use crate::storage::tablespace::encoding::Encoding as TablespaceEncoding;
    use crate::storage::tablespace::table::Table;
    use crate::storage::tests::{delete_test_env, init_test_env};

    const TEST_PATH: &str = "target/tests/tablespace_error";

    #[test]
    #[should_panic]
    fn serde_json_error() {
        let path = init_test_env(TEST_PATH, "serde_json_error");
        let absolute_path = fs::canonicalize(&path).unwrap();
        assert_eq!(
            Table::build("test", path.to_str().unwrap(),  &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap()).unwrap(),
            Table::from_json(&format!("{{\"name\":\"test\",\"schema\"{{\"fields\":[{{\"name\":\"id\",\"_type\":\"Bigint\"}},{{\"name\":\"cost\",\"_type\":\"Float\"}},{{\"name\":\"available\",\"_type\":\"Boolean\"}}]}},\"location\":\"{}\"}}", absolute_path.to_str().unwrap().replace('\\', "\\\\")),
            ).unwrap()
        );
        delete_test_env(TEST_PATH, "serde_json_error");
    }

    #[test]
    #[should_panic]
    fn std_io_error() {
        Table::from_file(Path::new("/unknown")).unwrap();
    }
}
