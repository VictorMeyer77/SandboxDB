use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::storage::tablespace::database::Database;
use crate::storage::tablespace::encoding::TablespaceEncoding;
use crate::storage::tablespace::error::TablespaceError;
use crate::storage::tablespace::metastore::Metastore;
use crate::storage::tablespace::table::Table;

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    metastore: Metastore,
    tables: HashMap<String, CatalogTable>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTable {
    database: Box<Database>,
    table: Box<Table>, // todo index, droits, keys...
}

impl CatalogTable {
    pub fn build(database: Box<Database>, table: Box<Table>) -> CatalogTable {
        CatalogTable { database, table }
    }
}

impl Catalog {
    pub fn build(metastore_path: &str) -> Result<Catalog, TablespaceError> {
        let mut catalog = Catalog {
            metastore: Metastore::from_file(&PathBuf::from(metastore_path))?,
            tables: HashMap::new(),
        };
        catalog.refresh()?;
        Ok(catalog)
    }

    fn refresh(&mut self) -> Result<(), TablespaceError> {
        self.metastore.load_databases()?;
        for database in self.metastore.list_databases() {
            let mut database = Box::new(self.metastore.databases.get(&database).unwrap().clone());
            database.load_tables()?;
            for table in database.list_tables() {
                let table = Box::new(database.tables.get(&table).unwrap().clone());
                let table_key = format!("{}.{}", &database.name, &table.name);
                let catalog_table = CatalogTable::build(database.clone(), table);
                self.tables.insert(table_key, catalog_table);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::schema::Schema;
    use crate::storage::tablespace::catalog::{Catalog, CatalogTable};
    use crate::storage::tablespace::database::Database;
    use crate::storage::tablespace::encoding::TablespaceEncoding;
    use crate::storage::tablespace::metastore::Metastore;
    use crate::storage::tablespace::metastore::tests::init_test_env;
    use crate::storage::tablespace::table::Table;

    const TEST_PATH: &str = "target/tests/catalog";

    #[test]
    fn build_should_gather_catalog() {
        let path = init_test_env(TEST_PATH, "build");
        let absolute_path = fs::canonicalize(&path).unwrap();
        let schema = Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap();
        let mut metastore = Metastore::build(path.to_str().unwrap()).unwrap();
        metastore.new_database("database_01", None).unwrap();
        let mut database: Database = metastore.databases.get("database_01").unwrap().clone();
        database.new_table("table_010", None, &schema).unwrap();
        database.new_table("table_011", None, &schema).unwrap();
        metastore.new_database("database_02", None).unwrap();
        database.new_table("table_020", None, &schema).unwrap();
        database.new_table("table_021", None, &schema).unwrap();
        let catalog: Catalog = Catalog::build(path.to_str().unwrap()).unwrap();
        metastore.load_databases().unwrap();
        assert_eq!(catalog.metastore, metastore);
        assert_eq!(
            *catalog.tables.get("database_01.table_010").unwrap(),
            CatalogTable::build(
                Box::new(database),
                Box::new(Table::from_file(&absolute_path.join("database_01/table_010")).unwrap())
            )
        );
        println!("{:?}", catalog.metastore);
        println!("{:?}", catalog.tables);
    }
}
