use chrono::Local;
use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::Encoding;
use crate::storage::file::tuple::Tuple;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalRow {
    pub date_created: i64,
    pub transaction_id: u32,
    pub transaction_size: u32,
    pub catalog_table_id: String,
    pub operation: Operation,
    pub old_data: Option<Tuple>,
    pub new_data: Option<Tuple>,
    pub buffer_page_id: Option<u32>,
    pub file_id: Option<u32>,
    pub page_id: Option<u32>,
    pub slot: Option<(u32, u32)>,
}

impl WalRow {
    pub fn insert(
        transaction_id: u32,
        transaction_size: u32,
        catalog_table_id: &str,
        new_data: Tuple,
    ) -> WalRow {
        WalRow {
            date_created: Local::now().timestamp_millis(),
            transaction_id,
            transaction_size,
            catalog_table_id: catalog_table_id.to_string(),
            operation: Operation::Insert,
            old_data: None,
            new_data: Some(new_data),
            buffer_page_id: None,
            file_id: None,
            page_id: None,
            slot: None,
        }
    }
    pub fn delete(
        transaction_id: u32,
        transaction_size: u32,
        catalog_table_id: &str,
        old_data: Tuple,
        id: (u32, u32, u32, u32, u32),
    ) -> WalRow {
        WalRow {
            date_created: Local::now().timestamp_millis(),
            transaction_id,
            transaction_size,
            catalog_table_id: catalog_table_id.to_string(),
            operation: Operation::Delete,
            old_data: Some(old_data),
            new_data: None,
            buffer_page_id: Some(id.0),
            file_id: Some(id.1),
            page_id: Some(id.2),
            slot: Some((id.3, id.4)),
        }
    }

    pub fn update(
        transaction_id: u32,
        transaction_size: u32,
        catalog_table_id: &str,
        new_data: Tuple,
        old_data: Tuple,
        id: (u32, u32, u32, u32, u32),
    ) -> WalRow {
        WalRow {
            date_created: Local::now().timestamp_millis(),
            transaction_id,
            transaction_size,
            catalog_table_id: catalog_table_id.to_string(),
            operation: Operation::Update,
            old_data: Some(old_data),
            new_data: Some(new_data),
            buffer_page_id: Some(id.0),
            file_id: Some(id.1),
            page_id: Some(id.2),
            slot: Some((id.3, id.4)),
        }
    }
}

impl Encoding for WalRow {}

#[cfg(test)]
pub mod tests {
    use crate::storage::buffer::wal_row::{Operation, WalRow};
    use crate::storage::file::encoding::Encoding as TablespaceEncoding;
    use crate::storage::file::tuple::Tuple;
    use crate::storage::schema::encoding::Encoding as SchemaEncoding;
    use crate::storage::schema::Schema;

    pub fn get_test_wal_row() -> WalRow {
        WalRow::insert(
            23,
            66,
            "87",
            Tuple::build(
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                    .unwrap(),
                &[0, 0, 1, 0],
                &[4; 32],
            )
            .unwrap(),
        )
    }

    #[test]
    fn as_bytes_should_convert_wal_row() {
        assert_eq!(
            get_test_wal_row().as_bytes().unwrap()[8..],
            vec![
                23, 0, 0, 0, 66, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 56, 55, 0, 0, 0, 0, 0, 1, 0, 4,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 32, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4,
                4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        let mut row = get_test_wal_row();
        row.date_created = 0;
        assert_eq!(
            WalRow::from_bytes(&[
                0, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 66, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 56, 55,
                0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 32, 0, 0, 0, 0, 0, 0, 0,
                4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                4, 4, 4, 4, 0, 0, 0, 0
            ])
            .unwrap(),
            row
        )
    }

    #[test]
    fn insert_should_generate_row() {
        let row = get_test_wal_row();
        assert_eq!(row.transaction_id, 23);
        assert_eq!(row.transaction_size, 66);
        assert_eq!(row.catalog_table_id, "87");
        assert_eq!(row.operation, Operation::Insert);
        assert!(row.new_data.is_some());
        assert!(row.old_data.is_none());
        assert!(row.buffer_page_id.is_none());
        assert!(row.file_id.is_none());
        assert!(row.page_id.is_none());
        assert!(row.slot.is_none());
    }

    #[test]
    fn delete_should_generate_row() {
        let row = WalRow::delete(
            23,
            66,
            "87",
            Tuple::build(
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                    .unwrap(),
                &[0, 0, 1, 0],
                &[4; 32],
            )
            .unwrap(),
            (0, 1, 2, 3, 4),
        );
        assert_eq!(row.transaction_id, 23);
        assert_eq!(row.transaction_size, 66);
        assert_eq!(row.catalog_table_id, "87");
        assert_eq!(row.operation, Operation::Delete);
        assert!(row.new_data.is_none());
        assert!(row.old_data.is_some());
        assert_eq!(row.buffer_page_id.unwrap(), 0);
        assert_eq!(row.file_id.unwrap(), 1);
        assert_eq!(row.page_id.unwrap(), 2);
        assert_eq!(row.slot.unwrap(), (3, 4));
    }

    #[test]
    fn update_should_generate_row() {
        let row = WalRow::update(
            23,
            66,
            "87",
            Tuple::build(
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                    .unwrap(),
                &[0, 0, 1, 0],
                &[4; 32],
            )
            .unwrap(),
            Tuple::build(
                &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP")
                    .unwrap(),
                &[0, 0, 1, 0],
                &[4; 32],
            )
            .unwrap(),
            (0, 1, 2, 3, 4),
        );
        assert_eq!(row.transaction_id, 23);
        assert_eq!(row.transaction_size, 66);
        assert_eq!(row.catalog_table_id, "87");
        assert_eq!(row.operation, Operation::Update);
        assert!(row.new_data.is_some());
        assert!(row.old_data.is_some());
        assert_eq!(row.buffer_page_id.unwrap(), 0);
        assert_eq!(row.file_id.unwrap(), 1);
        assert_eq!(row.page_id.unwrap(), 2);
        assert_eq!(row.slot.unwrap(), (3, 4));
    }
}
