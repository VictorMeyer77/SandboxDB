use std::collections::HashMap;

use crate::storage::file;
use crate::storage::file::encoding::Encoding;
use serde::{Deserialize, Serialize};

use crate::storage::file::error::Error;
use crate::storage::file::file_header::FileHeader;
use crate::storage::file::page::Page;
use crate::storage::file::page_header::PageHeader;

pub mod encoding;
pub mod error;
pub mod file_header;
pub mod page;
pub mod page_header;
pub mod tuple;
pub mod tuple_header;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct File {
    header: FileHeader,
    pages: HashMap<u32, Page>,
}

impl File {
    pub fn build(file_size: u32, compression: u8, version: [u8; 3]) -> File {
        File {
            header: FileHeader::build(file_size, compression, version),
            pages: HashMap::new(),
        }
    }

    pub fn insert_page(&mut self, page: &Page) -> Result<(), Error> {
        let page_index = self.pages.len() as u32;
        if self.header.bytes_size()? as u32 + (page_index + 1) * page.header.page_size
            > self.header.file_size
        {
            Err(Error::PageOverflow(
                "Insertion failed, no more place on this file.".to_string(),
            ))
        } else {
            self.pages.insert(page_index, page.clone());
            Ok(())
        }
    }

    pub fn delete_by_index(&mut self, index: u32) -> Result<(), Error> {
        self.pages
            .remove(&index)
            .ok_or(Error::InvalidIndex(index))?;
        Ok(())
    }

    pub fn update_by_index(&mut self, index: u32, page: &Page) -> Result<(), Error> {
        if let Some(value) = self.pages.get_mut(&index) {
            *value = page.clone();
            Ok(())
        } else {
            Err(Error::InvalidIndex(index))
        }
    }

    pub fn select_by_indexes(&mut self, indexes: &[u32]) -> Result<HashMap<u32, &Page>, Error> {
        let mut pages: HashMap<u32, &Page> = HashMap::new();
        for index in indexes {
            if let Some(page) = self.pages.get(index) {
                pages.insert(*index, page);
            }
        }
        Ok(pages)
    }
}

impl Encoding for File {
    fn as_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.header.as_bytes()?);
        self.pages
            .iter()
            .for_each(|(_, v)| concat_bytes.extend_from_slice(&v.as_bytes().unwrap()));
        Ok(concat_bytes)
    }

    fn from_bytes(bytes: &[u8]) -> Result<File, Error> {
        let mut pages: HashMap<u32, Page> = HashMap::new();
        let header = FileHeader::from_bytes(&bytes[..13])?;
        let page_size = PageHeader::from_bytes(&bytes[13..27])?.page_size as usize;
        let chunks = bytes[13..].chunks(page_size);
        for (index, chunk) in (0_u32..).zip(chunks) {
            let page = Page::from_bytes(chunk)?;
            pages.insert(index, page);
        }
        Ok(File { header, pages })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::page::tests::get_test_page;

    use super::*;

    fn get_test_file() -> File {
        let mut file = File::build(500 * 10 + 10, 0, [0, 10, 28]);
        file.pages.insert(0, get_test_page());
        file
    }

    fn get_test_bytes() -> Vec<u8> {
        vec![
            146, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 28, 244, 1, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            78, 1, 0, 0, 38, 0, 0, 0, 190, 1, 0, 0, 54, 0, 0, 0, 234, 0, 0, 0, 46, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 25, 0, 0, 0, 0, 0, 0, 0, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17, 0, 0,
            0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0,
            0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
        ]
    }

    #[test]
    fn as_bytes_should_convert_file() {
        assert_eq!(
            File::from_bytes(&get_test_file().as_bytes().unwrap()).unwrap(),
            get_test_file()
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            File::from_bytes(&get_test_bytes()).unwrap(),
            get_test_file()
        )
    }

    #[test]
    fn insert_page_should_push_in_hashmap() {
        let mut file = get_test_file();
        file.insert_page(&get_test_page()).unwrap();
        assert_eq!(file.pages[&1], get_test_page())
    }

    #[test]
    #[should_panic]
    fn insert_page_should_panic_if_full_file() {
        let mut file = get_test_file();
        for _ in 0..10 {
            file.insert_page(&get_test_page()).unwrap();
        }
    }

    #[test]
    fn remove_by_index_should_delete_page() {
        let mut file = get_test_file();
        file.delete_by_index(0).unwrap();
        assert!(file.pages.is_empty());
    }

    #[test]
    #[should_panic]
    fn remove_by_index_should_panic_invalid_page() {
        let mut file = get_test_file();
        file.delete_by_index(1).unwrap();
    }

    #[test]
    fn update_by_index_should_update_page() {
        let mut file = get_test_file();
        file.update_by_index(0, &get_test_page()).unwrap();
        assert_eq!(file.pages[&0], get_test_page());
    }

    #[test]
    #[should_panic]
    fn update_by_index_should_panic_invalid_page() {
        let mut file = get_test_file();
        file.update_by_index(1, &get_test_page()).unwrap();
    }

    #[test]
    fn select_by_indexes_should_return_page_pointer() {
        let mut file = get_test_file();
        file.insert_page(&get_test_page()).unwrap();
        assert_eq!(
            file.select_by_indexes(&[0, 2]).unwrap()[&0],
            &get_test_page()
        );
    }
}
