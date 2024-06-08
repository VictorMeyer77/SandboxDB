use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use crate::storage::tablespace::database::Database;
use crate::storage::tablespace::encoding::TablespaceEncoding;
use crate::storage::tablespace::error::Error;
use crate::storage::tablespace::metastore::Metastore;
use crate::storage::tablespace::table::Table;

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    metastore: Metastore,
    pub tables: HashMap<String, Rc<CatalogTable>>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTable {
    pub database: Rc<Database>,
    pub table: Table, // todo index, droits, keys...
}

impl CatalogTable {
    pub fn build(database: Rc<Database>, table: Table) -> CatalogTable {
        CatalogTable { database, table }
    }
}

impl Catalog {
    pub fn build(metastore_path: &str) -> Result<Catalog, Error> {
        let mut catalog = Catalog {
            metastore: Metastore::from_file(PathBuf::from(metastore_path).as_path())?,
            tables: HashMap::new(),
        };
        catalog.refresh()?;
        Ok(catalog)
    }

    pub fn refresh(&mut self) -> Result<(), Error> {
        self.metastore = Metastore::from_file(&self.metastore.location)?;
        self.metastore.load_databases()?;
        for database in self.metastore.list_databases() {
            let mut database = self.metastore.databases.get(&database).unwrap().clone();
            database.load_tables()?;
            let database = Rc::new(database);
            for table in database.list_tables() {
                let mut table = database.tables.get(&table).unwrap().clone();
                table.load_file_paths()?;
                let table_key = format!("{}.{}", &database.name, &table.name);
                let catalog_table = Rc::new(CatalogTable::build(Rc::clone(&database), table));
                self.tables.insert(table_key, catalog_table);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::rc::Rc;

    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::schema::Schema;
    use crate::storage::tablespace::catalog::{Catalog, CatalogTable};
    use crate::storage::tablespace::database::Database;
    use crate::storage::tablespace::encoding::TablespaceEncoding;
    use crate::storage::tablespace::metastore::Metastore;
    use crate::storage::tablespace::metastore::tests::{delete_test_env, init_test_env};
    use crate::storage::tablespace::table::Table;

    const TEST_PATH: &str = "target/tests/catalog";

    #[test]
    fn build_should_gather_catalog() {
        let path = init_test_env(TEST_PATH, "build");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let schema = Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("database_01", None).unwrap();
        let mut database_01: Database = metastore.databases.get("database_01").unwrap().clone();
        database_01.new_table("table_010", None, &schema).unwrap();
        database_01.new_table("table_011", None, &schema).unwrap();
        metastore.new_database("database_02", None).unwrap();
        let mut database_02: Database = metastore.databases.get("database_02").unwrap().clone();
        database_02.new_table("table_020", None, &schema).unwrap();
        database_02.new_table("table_021", None, &schema).unwrap();
        let catalog: Catalog = Catalog::build(path.to_str().unwrap()).unwrap();
        metastore.load_databases().unwrap();
        assert_eq!(catalog.metastore, metastore);
        assert_eq!(
            *catalog.tables.get("database_01.table_010").unwrap(),
            Rc::new(CatalogTable::build(
                Rc::new(database_01.clone()),
                Table::from_file(&absolute_path.join("database_01/table_010")).unwrap()
            ))
        );
        assert_eq!(
            *catalog.tables.get("database_01.table_011").unwrap(),
            Rc::new(CatalogTable::build(
                Rc::new(database_01.clone()),
                Table::from_file(&absolute_path.join("database_01/table_011")).unwrap()
            ))
        );
        assert_eq!(
            *catalog.tables.get("database_02.table_020").unwrap(),
            Rc::new(CatalogTable::build(
                Rc::new(database_02.clone()),
                Table::from_file(&absolute_path.join("database_02/table_020")).unwrap()
            ))
        );
        assert_eq!(
            *catalog.tables.get("database_02.table_021").unwrap(),
            Rc::new(CatalogTable::build(
                Rc::new(database_02.clone()),
                Table::from_file(&absolute_path.join("database_02/table_021")).unwrap()
            ))
        );
        assert_eq!(catalog.tables.len(), 4);
        delete_test_env(TEST_PATH, "build");
    }
}
