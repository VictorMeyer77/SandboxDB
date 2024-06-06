use crate::storage::tablespace::database::Database;
use crate::storage::tablespace::table::Table;

pub struct Catalog {
    database: Database,
    table: Table
}