use std::collections::HashMap;

use crate::storage::file::encoding::FileEncoding;
use crate::storage::file::file_header::FileHeader;
use crate::storage::file::page::Page;
use crate::storage::file::page_error::PageError;
use crate::storage::file::page_header::PageHeader;
use crate::storage::schema::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
struct File {
    header: FileHeader,
    pages: HashMap<u32, Page>,
}

impl File {
    pub fn build(file_size: u32, compression: u8) -> File {
        File {
            header: FileHeader::build(file_size, compression),
            pages: HashMap::new(),
        }
    }

    pub fn insert_page(&mut self, page: &Page) -> Result<(), PageError> {
        let page_index = self.pages.len() as u32;
        if page_index * page.header.page_size > self.header.file_size {
            Err(PageError::PageOverflow(
                "Insertion failed, no more place on this file.".to_string(),
            ))
        } else {
            self.pages.insert(page_index, page.clone());
            Ok(())
        }
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

    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<File, PageError> {
        let header = FileHeader::from_bytes(&bytes[..10], None)?;
        let page_size = PageHeader::from_bytes(&bytes[10..27], None)?.page_size as usize;
        let mut pages: HashMap<u32, Page> = HashMap::new();
        let chunks = bytes[10..].chunks(page_size);
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
    use crate::storage::file::slot::Slot;
    use crate::storage::file::tuple::Tuple;
    use crate::storage::schema::encoding::SchemaEncoding;

    use super::*;

    fn get_test_schema() -> Schema {
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap()
    }

    fn get_test_page() -> Page {
        let schema = get_test_schema();
        let mut page = Page::build(&schema, 500, [10, 28, 45], 1).unwrap();
        page.slots = vec![
            Slot::build(462, 38),
            Slot::build(350, 22),
            Slot::build(250, 30),
        ];
        page.tuples = vec![
            Tuple::build(&schema, &[0; 4], &[2; 33]).unwrap(),
            Tuple::build(&schema, &[1, 0, 0, 0], &[8; 17]).unwrap(),
            Tuple::build(&schema, &[0, 0, 0, 1], &[65; 25]).unwrap(),
        ];
        page.header.slots = 3;
        page
    }

    fn get_test_file() -> File {
        let mut file = File::build(500 * 10 + 10, 0);
        let page = get_test_page();
        file.pages.insert(0, page.clone());
        file
    }

    fn get_test_bytes() -> Vec<u8> {
        vec![
            146, 19, 0, 0, 0, 0, 0, 0, 0, 0, 244, 1, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 10, 28, 45, 0,
            1, 206, 1, 0, 0, 38, 0, 0, 0, 94, 1, 0, 0, 22, 0, 0, 0, 250, 0, 0, 0, 30, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2,
        ]
    }

    #[test]
    fn as_bytes_should_convert_file() {
        assert_eq!(get_test_file().as_bytes(), get_test_bytes())
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            File::from_bytes(&get_test_bytes(), Some(&get_test_schema())).unwrap(),
            get_test_file()
        )
    }
}
