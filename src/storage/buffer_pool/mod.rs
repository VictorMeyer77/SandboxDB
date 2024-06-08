use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use crc32fast::hash;

use crate::storage::buffer_pool::error::Error;
use crate::storage::buffer_pool::page_meta::PageMeta;
use crate::storage::file::page::Page;
use crate::storage::tablespace::catalog::{Catalog, CatalogTable};

mod error;
mod page_meta;

const BUFFER_LIMIT_USED_SIZE: f32 = 0.95;
const VACUUM_SIZE: f32 = 0.05;

#[derive(Debug, Clone, PartialEq)]
pub struct BufferPool {
    pub size: usize,
    pub catalog: Catalog,
    page_metas: HashMap<u32, PageMeta>,
    page_catalogs: HashMap<u32, Rc<CatalogTable>>,
    pages: HashMap<u32, Page>,
}

impl BufferPool {
    pub fn build(size: usize, metastore_path: &str) -> BufferPool {
        BufferPool {
            size,
            catalog: Catalog::build(metastore_path).unwrap(),
            page_metas: HashMap::new(),
            page_catalogs: HashMap::new(),
            pages: HashMap::new(),
        }
    }

    fn used_space(&self) -> f32 {
        let used_space: usize = self
            .pages
            .values()
            .map(|page| page.header.page_size as usize)
            .sum();
        used_space as f32 / self.size as f32 * 100.0
    }

    pub fn load_page(
        &mut self,
        page: Page,
        catalog_id: &str,
        file_id: &str,
        page_id: u32,
    ) -> Result<u32, Error> {
        if !self.catalog.tables.contains_key(catalog_id) {
            self.catalog.refresh()?;
        }
        if self.used_space() + page.header.page_size as f32
            > BUFFER_LIMIT_USED_SIZE * self.size as f32
        {
            self.vacuum();
        }
        let page_key = Self::buffer_page_key(catalog_id, file_id, page_id);
        self.pages.insert(page_key, page);
        self.page_metas.insert(page_key, PageMeta::new());
        self.page_catalogs.insert(
            page_key,
            Rc::clone(self.catalog.tables.get(catalog_id).unwrap()), // todo
        );
        Ok(page_key)
    }

    fn buffer_page_key(catalog_id: &str, file_id: &str, page_id: u32) -> u32 {
        let mut key = Vec::new();
        key.extend_from_slice(catalog_id.as_bytes());
        key.extend_from_slice(file_id.as_bytes());
        key.extend_from_slice(&page_id.to_le_bytes());
        hash(&key)
    }

    pub fn update_page(&mut self, page_key: &u32, page: Page) -> Result<(), Error> {
        if self.pages.contains_key(page_key) {
            if self.used_space() + page.header.page_size as f32
                > BUFFER_LIMIT_USED_SIZE * self.size as f32
            {
                self.vacuum();
            }
            self.pages.insert(*page_key, page);
            self.page_metas
                .get_mut(page_key)
                .unwrap()
                .increment_access();
            Ok(())
        } else {
            Err(Error::UnknownTableKey(*page_key))
        }
    }

    pub fn get_page(&mut self, key: &u32) -> Result<&Page, Error> {
        if let Some(page) = self.pages.get(key) {
            self.page_metas.get_mut(key).unwrap().increment_access();
            Ok(page)
        } else {
            Err(Error::UnknownTableKey(*key))
        }
    }

    pub fn get_page_catalog(&mut self, key: &u32) -> Result<Rc<CatalogTable>, Error> {
        if let Some(page_catalog) = self.page_catalogs.get(key) {
            self.page_metas.get_mut(key).unwrap().increment_access();
            Ok(Rc::clone(page_catalog))
        } else {
            Err(Error::UnknownTableKey(*key))
        }
    }

    pub fn get_pages_by_table(&mut self, catalog_key: &str) -> Vec<(&u32, &Page)> {
        let keys: Vec<&u32> = self
            .page_catalogs
            .iter()
            .filter(|&(_, v)| *self.catalog.tables.get(catalog_key).unwrap() == *v)
            .map(|(k, _)| k)
            .collect();
        for key in &keys {
            self.page_metas.get_mut(key).unwrap().increment_access()
        }
        self.pages
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .collect()
    }

    pub fn vacuum(&mut self) {
        let mut size_to_free = self.size as f32 * VACUUM_SIZE;
        let mut meta_sorted = self.get_page_access_sorted();
        while size_to_free
            > self
                .pages
                .get(meta_sorted.first().unwrap())
                .unwrap()
                .header
                .page_size as f32
        {
            let page_key = meta_sorted.first().unwrap();
            let page = self.pages.remove(page_key).unwrap();
            size_to_free -= page.header.page_size as f32;
            self.page_catalogs.remove(page_key);
            self.page_metas.remove(page_key);
            meta_sorted.remove(0);
        }
    }

    fn get_page_access_sorted(&self) -> Vec<u32> {
        let max_last_access = self.max_last_access() as f64;
        let max_count_access = self.max_count_access() as f64;
        let mut metas: Vec<(u32, (f64, f64))> = self
            .page_metas
            .iter()
            .map(|(k, v)| (*k, (v.last_access as f64, v.count_access as f64)))
            .collect();
        metas.sort_by(|(_, a), (_, b)| {
            (a.0 / max_last_access + a.1 / max_count_access)
                .partial_cmp(&(b.0 / max_last_access + b.1 / max_count_access))
                .unwrap_or(Ordering::Equal)
        });
        metas.iter().map(|(k, _)| *k).collect()
    }

    fn max_last_access(&self) -> usize {
        self.page_metas
            .values()
            .map(|meta| meta.last_access)
            .max()
            .unwrap()
    }

    fn max_count_access(&self) -> usize {
        self.page_metas
            .values()
            .map(|meta| meta.count_access)
            .max()
            .unwrap()
    }
}

// todo spill

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::Schema;
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};
    use crate::storage::tablespace::metastore::Metastore;

    use super::*;

    const TEST_PATH: &str = "target/tests/buffer_pool";

    fn get_buffer_pool_test(metastore: &mut Metastore) -> (BufferPool, Vec<u32>) {
        let mut buffer_pool = BufferPool::build(100, metastore.location.to_str().unwrap());
        let mut database = metastore.new_database("db_test", None).unwrap();
        let schema =
            Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
        let _ = database.new_table("tb_test", None, &schema).unwrap();
        let page_key_one = buffer_pool
            .load_page(
                Page::build(&schema, 2, 1).unwrap(),
                "db_test.tb_test",
                "0",
                0,
            )
            .unwrap();
        let page_key_two = buffer_pool
            .load_page(
                Page::build(&schema, 2, 1).unwrap(),
                "db_test.tb_test",
                "0",
                1,
            )
            .unwrap();
        let page_key_three = buffer_pool
            .load_page(
                Page::build(&schema, 2, 1).unwrap(),
                "db_test.tb_test",
                "0",
                2,
            )
            .unwrap();
        (
            buffer_pool,
            vec![page_key_one, page_key_two, page_key_three],
        )
    }

    #[test]
    fn used_space_should_compute_memory() {
        let path = init_test_env(TEST_PATH, "used_space");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        let used_space = buffer_pool.used_space();
        assert_eq!(used_space, 6.0);
        delete_test_env(TEST_PATH, "used_space");
    }

    #[test]
    fn load_page_should_buffer_page() {
        let path = init_test_env(TEST_PATH, "load_page");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        let page_key_four = buffer_pool
            .load_page(
                Page::build(
                    &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                        .unwrap(),
                    2,
                    1,
                )
                .unwrap(),
                "db_test.tb_test",
                "0",
                3,
            )
            .unwrap();
        assert!(buffer_pool.page_catalogs.contains_key(&page_key_four));
        assert!(buffer_pool.page_metas.contains_key(&page_key_four));
        assert!(buffer_pool.pages.contains_key(&page_key_four));
        assert_eq!(buffer_pool.page_catalogs.len(), 4);
        assert_eq!(buffer_pool.page_metas.len(), 4);
        assert_eq!(buffer_pool.pages.len(), 4);
        delete_test_env(TEST_PATH, "load_page");
    }

    #[test]
    fn update_page_should_replace_existing_page() {
        let path = init_test_env(TEST_PATH, "update_page_01");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        buffer_pool
            .update_page(
                &page_keys[1],
                Page::build(
                    &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                        .unwrap(),
                    42,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            buffer_pool
                .pages
                .get(&page_keys[1])
                .unwrap()
                .header
                .page_size,
            42
        );
        assert_eq!(
            buffer_pool
                .page_metas
                .get(&page_keys[1])
                .unwrap()
                .count_access,
            2
        );
        delete_test_env(TEST_PATH, "update_page_01");
    }

    #[test]
    #[should_panic]
    fn update_page_should_panic_if_unknown_key() {
        let path = init_test_env(TEST_PATH, "update_page_02");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        buffer_pool
            .update_page(
                &7,
                Page::build(
                    &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                        .unwrap(),
                    42,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        delete_test_env(TEST_PATH, "update_page_02");
    }

    #[test]
    fn get_page_should_return_page_pointer() {
        let path = init_test_env(TEST_PATH, "get_page_01");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        let page = buffer_pool.get_page(&page_keys[0]).unwrap();
        assert_eq!(
            *page,
            Page::build(
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                    .unwrap(),
                2,
                1
            )
            .unwrap()
        );
        assert_eq!(
            buffer_pool
                .page_metas
                .get(&page_keys[0])
                .unwrap()
                .count_access,
            2
        );
        delete_test_env(TEST_PATH, "get_page_01");
    }

    #[test]
    #[should_panic]
    fn get_page_should_panic_if_unknown_key() {
        let path = init_test_env(TEST_PATH, "get_page_02");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        let _ = buffer_pool.get_page(&9).unwrap();
        delete_test_env(TEST_PATH, "get_page_02");
    }

    #[test]
    fn get_page_catalog_should_return_page_pointer() {
        let path = init_test_env(TEST_PATH, "get_page_catalog_01");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        let catalog_table = buffer_pool.get_page_catalog(&page_keys[0]).unwrap();
        assert_eq!(catalog_table.database.name, "db_test");
        assert_eq!(catalog_table.table.name, "tb_test");
        assert_eq!(
            buffer_pool
                .page_metas
                .get(&page_keys[0])
                .unwrap()
                .count_access,
            2
        );
        delete_test_env(TEST_PATH, "get_page_catalog_01");
    }

    #[test]
    #[should_panic]
    fn get_page_catalog_should_panic_if_unknown_key() {
        let path = init_test_env(TEST_PATH, "get_page_catalog_02");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        let _ = buffer_pool.get_page_catalog(&9).unwrap();
        delete_test_env(TEST_PATH, "get_page_catalog_02");
    }

    #[test]
    fn get_pages_by_table_should_gather_page_by_table() {
        let path = init_test_env(TEST_PATH, "get_pages_by_table");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, _) = get_buffer_pool_test(&mut metastore);
        let pages: Vec<(&u32, &Page)> = buffer_pool.get_pages_by_table("db_test.tb_test");
        assert_eq!(pages.len(), 3);
        delete_test_env(TEST_PATH, "get_pages_by_table");
    }

    #[test]
    fn vacuum_should_remove_page() {
        let path = init_test_env(TEST_PATH, "vacuum");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        let _ = buffer_pool.get_page(&page_keys[0]).unwrap();
        let page_key_four = buffer_pool
            .load_page(
                Page::build(
                    &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                        .unwrap(),
                    92,
                    1,
                )
                .unwrap(),
                "db_test.tb_test",
                "0",
                3,
            )
            .unwrap();

        let page_metas: Vec<u32> = buffer_pool.page_metas.keys().cloned().collect();
        let pages: Vec<u32> = buffer_pool.pages.keys().cloned().collect();
        let page_catalogs: Vec<u32> = buffer_pool.page_catalogs.keys().cloned().collect();

        assert!(page_metas.contains(&page_key_four));
        assert!(page_metas.contains(&page_keys[0]));
        assert_eq!(page_metas.len(), 2);
        assert!(pages.contains(&page_key_four));
        assert!(pages.contains(&page_keys[0]));
        assert_eq!(pages.len(), 2);
        assert!(page_catalogs.contains(&page_key_four));
        assert!(page_catalogs.contains(&page_keys[0]));
        assert_eq!(page_catalogs.len(), 2);
        delete_test_env(TEST_PATH, "vacuum");
    }

    #[test]
    fn get_page_access_sorted_should_compute_ordering() {
        let path = init_test_env(TEST_PATH, "get_page_access_sorted");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        buffer_pool.get_page(&page_keys[1]).unwrap();
        buffer_pool.get_page(&page_keys[1]).unwrap();
        thread::sleep(Duration::from_millis(1000));
        let page_key_four = buffer_pool
            .load_page(
                Page::build(
                    &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                        .unwrap(),
                    2,
                    1,
                )
                .unwrap(),
                "db_test.tb_test",
                "0",
                3,
            )
            .unwrap();
        let metas = buffer_pool.get_page_access_sorted();
        assert!(metas[0] == page_keys[0] || metas[0] == page_keys[2]);
        assert!(metas[1] == page_keys[0] || metas[1] == page_keys[2]);
        assert_eq!(metas[2], page_key_four);
        assert_eq!(metas[3], page_keys[1]);
        delete_test_env(TEST_PATH, "get_page_access_sorted");
    }

    #[test]
    fn max_count_access() {
        let path = init_test_env(TEST_PATH, "max_count_access");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (mut buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        buffer_pool.get_page(&page_keys[1]).unwrap();
        buffer_pool.get_page(&page_keys[1]).unwrap();
        assert_eq!(buffer_pool.max_count_access(), 3);
        delete_test_env(TEST_PATH, "max_count_access");
    }

    #[test]
    fn max_last_access() {
        let path = init_test_env(TEST_PATH, "max_last_access");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let (buffer_pool, page_keys) = get_buffer_pool_test(&mut metastore);
        assert_eq!(
            buffer_pool.max_last_access(),
            buffer_pool
                .page_metas
                .get(&page_keys[2])
                .unwrap()
                .last_access
        );
        delete_test_env(TEST_PATH, "max_last_access");
    }
}
