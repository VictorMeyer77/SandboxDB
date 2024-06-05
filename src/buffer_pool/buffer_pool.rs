use std::collections::HashMap;
use std::hash::Hash;

use crate::buffer_pool::error::BufferError;
use crate::storage::file::page::Page;

const BUFFER_LIMIT_USED_SIZE: f32 = 0.95;

pub struct BufferPool {
    size: usize,
    page_metas: HashMap<String, PageMeta>,
    pages: HashMap<String, Page>,
}

pub struct PageMeta {
    last_access: u32,
}

impl BufferPool {
    pub fn build(size: usize) -> BufferPool {
        BufferPool {
            size,
            page_metas: HashMap::new(),
            pages: HashMap::new(),
        }
    }

    fn used_space(&self) -> f32 {
        let used_space: usize = self
            .pages
            .iter()
            .map(|(_, page)| page.header.page_size as usize)
            .sum();
        used_space as f32 / self.size as f32 * 100.0
    }

    pub fn load_page(&self, file_path: &str, page_id: u32) -> Result<(), BufferError> {
        if self.used_space() > BUFFER_LIMIT_USED_SIZE {
            todo!() // libérer espace
        }

        Ok(())
    }

    pub fn close_page() {
        todo!()
    }

    pub fn get_page() {
        todo!()
    }

    pub fn vacuum() {
        todo!()
    }
}

// spill
