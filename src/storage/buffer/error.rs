use std::fmt;

use crate::storage::tablespace;

pub enum Error {
    UnknownTableKey(u32),
    Tablespace(tablespace::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownTableKey(ref msg) => {
                write!(f, "Table {} doesn't buffered.", msg)
            }
            Error::Tablespace(ref err) => write!(f, "Tablespace error: {}.", err),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl From<tablespace::error::Error> for Error {
    fn from(value: tablespace::error::Error) -> Self {
        Error::Tablespace(value)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::storage::buffer::buffer_pool::tests::get_buffer_pool_test;
    use crate::storage::file::page::Page;
    use crate::storage::tablespace::metastore::Metastore;
    use crate::storage::tests::{delete_test_env, init_test_env};

    const TEST_PATH: &str = "target/tests/buffer_pool_error";

    #[test]
    #[should_panic]
    fn tablespace_error() {
        let path = init_test_env(TEST_PATH, "tablespace_error");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        fs::remove_dir_all(path).unwrap();
        buffer_pool
            .load_page(Page::build(2, 1).unwrap(), "wrong", "0", 3)
            .unwrap();
        delete_test_env(TEST_PATH, "tablespace_error");
    }
}
