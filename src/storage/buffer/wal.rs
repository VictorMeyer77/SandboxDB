// todo build new / -> Self

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::storage::buffer::error::Error;
use crate::storage::buffer::wal_raw::WalRaw;
use crate::storage::file::encoding::Encoding;

const WAL_FILE_NAME: &str = ".wal";

pub struct Wal {
    file: BufWriter<File>,
}

impl Wal {
    pub fn build(path: &str) -> Result<Wal, Error> {
        Ok(Wal {
            file: BufWriter::new(File::create(PathBuf::from(path).join(WAL_FILE_NAME))?),
        })
    }

    pub fn write_raw(&mut self, raw: &WalRaw) -> Result<(), Error> {
        self.file.write_all(&raw.as_bytes()?)?;
        self.file.write_all(b"\n")?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        Ok(self.file.flush()?)
    }
}

#[cfg(test)]
pub mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    use crate::storage::buffer::wal::Wal;
    use crate::storage::buffer::wal_raw::tests::get_test_wal_raw;
    use crate::storage::buffer::wal_raw::WalRaw;
    use crate::storage::tests::init_test_env;

    const TEST_PATH: &str = "target/tests/wal";

    #[test]
    fn wal_should_log_in_file() {
        let path = init_test_env(TEST_PATH, "wal");
        let mut wal = Wal::build(path.to_str().unwrap()).unwrap();
        wal.write_raw(&get_test_wal_raw()).unwrap();
        wal.write_raw(&get_test_wal_raw()).unwrap();
        wal.write_raw(&get_test_wal_raw()).unwrap();
        let file = File::open(path.join(".wal"))?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            assert_eq!(WalRaw::from_bytes(&line.unwrap()))
        }
        //delete_test_env(TEST_PATH, "wal");
    }
}
