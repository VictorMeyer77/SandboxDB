use std::collections::HashMap;

use crate::storage::buffer_pool::error::BufferError;
use crate::storage::buffer_pool::page_meta::PageMeta;
use crate::storage::file::page::Page;
use crate::storage::tablespace::catalog::{Catalog, CatalogTable};

const BUFFER_LIMIT_USED_SIZE: f32 = 0.95;
const VACUUM_SIZE: f32 = 0.05;

#[derive(Debug, Clone, PartialEq)]
pub struct BufferPool {
    size: usize,
    catalog: Catalog,
    page_metas: HashMap<String, PageMeta>,
    page_catalogs: HashMap<String, Box<CatalogTable>>,
    pages: HashMap<String, Page>,
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
            .iter()
            .map(|(_, page)| page.header.page_size as usize)
            .sum();
        used_space as f32 / self.size as f32 * 100.0
    }

    pub fn load_page(&mut self, page: Page, catalog_key: &str) -> Result<(), BufferError> {
        if !self.catalog.tables.contains_key(catalog_key) {
            self.catalog.refresh()?;
        }
        if self.used_space() + page.header.page_size as f32 > BUFFER_LIMIT_USED_SIZE {
            self.vacuum();
        }
        self.pages
            .insert(catalog_key.to_string(), page)
            .ok_or(BufferError::UnknownCatalogTable(catalog_key.to_string()))?;
        self.page_metas
            .insert(catalog_key.to_string(), PageMeta::new())
            .ok_or(BufferError::UnknownCatalogTable(catalog_key.to_string()))?;
        self.page_catalogs.insert(
            catalog_key.to_string(),
            Box::new(
                self.catalog
                    .tables
                    .get(catalog_key)
                    .ok_or(BufferError::UnknownCatalogTable(catalog_key.to_string()))?
                    .clone(),
            ),
        );
        Ok(())
    }

    pub fn get_page() {
        todo!()
    }

    pub fn get_page_catalog() {
        todo!()
    }

    pub fn vacuum(&mut self) {
        let meta_clone = self.page_metas.clone();
        let mut size_to_free = self.size as f32 * VACUUM_SIZE;
        let max_last_access = self.max_last_access();
        let max_count_access = self.max_count_access();
        let mut metas: Vec<(&String, &PageMeta)> = meta_clone.iter().collect();
        metas.sort_by(|(_, a), (_, b)| {
            (a.count_access / max_count_access + a.last_access / max_last_access)
                .cmp(&(b.count_access / max_count_access + b.last_access / max_last_access))
        });
        while size_to_free
            > self
                .pages
                .get(metas.first().unwrap().0)
                .unwrap()
                .header
                .page_size as f32
        {
            let page_name = metas.first().unwrap().0;
            self.pages.remove(page_name);
            self.page_catalogs.remove(page_name);
            self.page_metas.remove(page_name);
            metas.remove(0);
        }
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

// spill

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::schema::Schema;
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};
    use crate::storage::tablespace::metastore::Metastore;

    const TEST_PATH: &str = "target/tests/buffer_pool";

    #[test]
    fn vacuum_should_remove_page() {
        let path = init_test_env(TEST_PATH, "vacuum");
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        let schema =
            Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();

        let mut pages: HashMap<String, Page> = HashMap::new();
        pages.insert("page0".to_string(), Page::build(&schema, 2, 1).unwrap());
        pages.insert("page1".to_string(), Page::build(&schema, 2, 1).unwrap());
        pages.insert("page2".to_string(), Page::build(&schema, 2, 1).unwrap());
        pages.insert("page3".to_string(), Page::build(&schema, 92, 1).unwrap());

        let mut page_metas: HashMap<String, PageMeta> = HashMap::new();
        page_metas.insert(
            "page0".to_string(),
            PageMeta {
                last_access: 0,
                count_access: 5,
            },
        );
        page_metas.insert(
            "page1".to_string(),
            PageMeta {
                last_access: 1,
                count_access: 1,
            },
        );
        page_metas.insert(
            "page2".to_string(),
            PageMeta {
                last_access: 2,
                count_access: 1,
            },
        );
        page_metas.insert(
            "page3".to_string(),
            PageMeta {
                last_access: 3,
                count_access: 1,
            },
        );

        let mut page_catalogs: HashMap<String, Box<CatalogTable>> = HashMap::new();
        let database = metastore.new_database("test", None).unwrap();
        let mut database = Box::new(database);
        let table = Box::new(database.new_table("test", None, &schema).unwrap());
        let catalog_table_box = Box::new(CatalogTable::build(database, table));
        page_catalogs.insert("page0".to_string(), catalog_table_box.clone());
        page_catalogs.insert("page1".to_string(), catalog_table_box.clone());
        page_catalogs.insert("page2".to_string(), catalog_table_box.clone());
        page_catalogs.insert("page3".to_string(), catalog_table_box.clone());

        let mut buffer_pool = BufferPool {
            size: 100,
            catalog: Catalog::build(path.to_str().unwrap()).unwrap(),
            pages,
            page_metas,
            page_catalogs,
        };

        buffer_pool.vacuum();

        let page_metas: Vec<String> = buffer_pool.page_metas.keys().cloned().collect();
        let pages: Vec<String> = buffer_pool.pages.keys().cloned().collect();
        let page_catalogs: Vec<String> = buffer_pool.page_catalogs.keys().cloned().collect();

        println!("{:?}", pages);

        assert!(page_metas.contains(&"page0".to_string()));
        assert!(page_metas.contains(&"page3".to_string()));
        assert_eq!(page_metas.len(), 2);
        assert!(pages.contains(&"page0".to_string()));
        assert!(pages.contains(&"page3".to_string()));
        assert_eq!(pages.len(), 2);
        assert!(page_catalogs.contains(&"page0".to_string()));
        assert!(page_catalogs.contains(&"page3".to_string()));
        assert_eq!(page_catalogs.len(), 2);

        delete_test_env(TEST_PATH, "vacuum");
    }
}
