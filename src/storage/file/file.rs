use std::collections::HashMap;

use crate::storage::file::encoding::FileEncoding;
use crate::storage::file::file_error::FileError;
use crate::storage::file::file_header::FileHeader;
use crate::storage::file::page::Page;
use crate::storage::file::page_header::PageHeader;
use crate::storage::schema::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
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

    pub fn insert_page(&mut self, page: &Page) -> Result<(), FileError> {
        let page_index = self.pages.len() as u32;
        if self.header.bytes_size() as u32 + (page_index + 1) * page.header.page_size
            > self.header.file_size
        {
            Err(FileError::PageOverflow(
                "Insertion failed, no more place on this file.".to_string(),
            ))
        } else {
            self.pages.insert(page_index, page.clone());
            Ok(())
        }
    }

    pub fn delete_by_index(&mut self, index: u32) -> Result<(), FileError> {
        self.pages
            .remove(&index)
            .ok_or(FileError::InvalidIndex(index))?;
        Ok(())
    }

    pub fn update_by_index(&mut self, index: u32, page: &Page) -> Result<(), FileError> {
        if let Some(value) = self.pages.get_mut(&index) {
            *value = page.clone();
            Ok(())
        } else {
            Err(FileError::InvalidIndex(index))
        }
    }

    pub fn select_by_indexes(&mut self, indexes: &[u32]) -> Result<HashMap<u32, &Page>, FileError> {
        let mut pages: HashMap<u32, &Page> = HashMap::new();
        for index in indexes {
            if let Some(page) = self.pages.get(&index) {
                pages.insert(*index, page);
            }
        }
        Ok(pages)
    }
}

impl FileEncoding<File> for File {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.header.as_bytes());
        self.pages
            .iter()
            .for_each(|(_, v)| concat_bytes.extend_from_slice(&v.as_bytes()));
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<File, FileError> {
        let mut pages: HashMap<u32, Page> = HashMap::new();
        let header = FileHeader::from_bytes(&bytes[..13], None)?;
        let page_size = PageHeader::from_bytes(&bytes[13..27], None)?.page_size as usize;
        let chunks = bytes[13..].chunks(page_size);
        let mut index: u32 = 0;
        for chunk in chunks {
            let page = Page::from_bytes(chunk, schema)?;
            pages.insert(index, page);
            index += 1;
        }
        Ok(File { header, pages })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::tuple::Tuple;
    use crate::storage::schema::encoding::SchemaEncoding;

    use super::*;

    fn get_test_schema() -> Schema {
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap()
    }

    fn get_test_page() -> Page {
        let mut page = Page::build(&get_test_schema(), 500, 1).unwrap();
        page.tuples.insert(
            (462, 38),
            Tuple::build(&get_test_schema(), &[0; 4], &[2; 33]).unwrap(),
        );
        page.tuples.insert(
            (350, 22),
            Tuple::build(&get_test_schema(), &[1, 0, 0, 0], &[8; 17]).unwrap(),
        );
        page.tuples.insert(
            (250, 30),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.header.slots = 3;
        page
    }

    fn get_test_file() -> File {
        let mut file = File::build(500 * 10 + 10, 0, [0, 10, 28]);
        let page = get_test_page();
        file.pages.insert(0, page.clone());
        file
    }

    fn get_test_bytes() -> Vec<u8> {
        vec![
            146, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 28, 244, 1, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            206, 1, 0, 0, 38, 0, 0, 0, 94, 1, 0, 0, 22, 0, 0, 0, 250, 0, 0, 0, 30, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2,
        ]
    }

    #[test]
    fn as_bytes_should_convert_file() {
        assert_eq!(
            File::from_bytes(&get_test_file().as_bytes(), Some(&get_test_schema())).unwrap(),
            get_test_file()
        )
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            File::from_bytes(&get_test_bytes(), Some(&get_test_schema())).unwrap(),
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
