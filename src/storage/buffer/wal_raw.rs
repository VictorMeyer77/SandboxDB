use crate::storage::file::encoding::Encoding;
use chrono::Local;
use serde::{Deserialize, Serialize};

use crate::storage::file::tuple::Tuple;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalRaw {
    date_created: i64,
    transaction_id: u32,
    transaction_size: u32,
    catalog_table_id: String,
    operation: Operation,
    old_data: Option<Tuple>,
    new_data: Option<Tuple>,
    buffer_page_id: Option<u32>,
    file_id: Option<u32>,
    page_id: Option<u32>,
    slot: Option<(u32, u32)>,
}

impl WalRaw {
    pub fn insert(
        transaction_id: u32,
        transaction_size: u32,
        catalog_table_id: &str,
        new_data: Tuple,
    ) -> WalRaw {
        WalRaw {
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
    ) -> WalRaw {
        WalRaw {
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
    ) -> WalRaw {
        WalRaw {
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

impl Encoding for WalRaw {}

#[cfg(test)]
mod tests {
    use crate::storage::buffer::wal_raw::{Operation, WalRaw};
    use crate::storage::file::encoding::Encoding as TablespaceEncoding;
    use crate::storage::file::tuple::Tuple;
    use crate::storage::schema::encoding::Encoding as SchemaEncoding;
    use crate::storage::schema::Schema;

    fn get_test_wal_raw() -> WalRaw {
        WalRaw::insert(
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
    fn as_bytes_should_convert_wal_raw() {
        assert_eq!(
            get_test_wal_raw().as_bytes().unwrap()[8..],
            vec![
                23, 0, 0, 0, 66, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 56, 55, 0, 0, 0, 0, 0, 1, 0, 4,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 32, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4,
                4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        let mut raw = get_test_wal_raw();
        raw.date_created = 0;
        assert_eq!(
            WalRaw::from_bytes(&[
                0, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 66, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 56, 55,
                0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 32, 0, 0, 0, 0, 0, 0, 0,
                4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                4, 4, 4, 4, 0, 0, 0, 0
            ])
            .unwrap(),
            raw
        )
    }

    #[test]
    fn insert_should_generate_raw() {
        let raw = get_test_wal_raw();
        assert_eq!(raw.transaction_id, 23);
        assert_eq!(raw.transaction_size, 66);
        assert_eq!(raw.catalog_table_id, "87");
        assert_eq!(raw.operation, Operation::Insert);
        assert!(raw.new_data.is_some());
        assert!(raw.old_data.is_none());
        assert!(raw.buffer_page_id.is_none());
        assert!(raw.file_id.is_none());
        assert!(raw.page_id.is_none());
        assert!(raw.slot.is_none());
    }

    #[test]
    fn delete_should_generate_raw() {
        let raw = WalRaw::delete(
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
        assert_eq!(raw.transaction_id, 23);
        assert_eq!(raw.transaction_size, 66);
        assert_eq!(raw.catalog_table_id, "87");
        assert_eq!(raw.operation, Operation::Delete);
        assert!(raw.new_data.is_none());
        assert!(raw.old_data.is_some());
        assert_eq!(raw.buffer_page_id.unwrap(), 0);
        assert_eq!(raw.file_id.unwrap(), 1);
        assert_eq!(raw.page_id.unwrap(), 2);
        assert_eq!(raw.slot.unwrap(), (3, 4));
    }

    #[test]
    fn update_should_generate_raw() {
        let raw = WalRaw::update(
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
        assert_eq!(raw.transaction_id, 23);
        assert_eq!(raw.transaction_size, 66);
        assert_eq!(raw.catalog_table_id, "87");
        assert_eq!(raw.operation, Operation::Update);
        assert!(raw.new_data.is_some());
        assert!(raw.old_data.is_some());
        assert_eq!(raw.buffer_page_id.unwrap(), 0);
        assert_eq!(raw.file_id.unwrap(), 1);
        assert_eq!(raw.page_id.unwrap(), 2);
        assert_eq!(raw.slot.unwrap(), (3, 4));
    }
}
