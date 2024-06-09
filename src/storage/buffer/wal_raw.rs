use bincode;
use chrono::Local;
use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::FileEncoding;
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
        buffer_page_id: u32,
        file_id: u32,
        page_id: u32,
        slot: (u32, u32),
    ) -> WalRaw {
        WalRaw {
            date_created: Local::now().timestamp_millis(),
            transaction_id,
            transaction_size,
            catalog_table_id: catalog_table_id.to_string(),
            operation: Operation::Delete,
            old_data: Some(old_data),
            new_data: None,
            buffer_page_id: Some(buffer_page_id),
            file_id: Some(file_id),
            page_id: Some(page_id),
            slot: Some(slot),
        }
    }

    pub fn update(
        transaction_id: u32,
        transaction_size: u32,
        catalog_table_id: &str,
        new_data: Tuple,
        old_data: Tuple,
        buffer_page_id: u32,
        file_id: u32,
        page_id: u32,
        slot: (u32, u32),
    ) -> WalRaw {
        WalRaw {
            date_created: Local::now().timestamp_millis(),
            transaction_id,
            transaction_size,
            catalog_table_id: catalog_table_id.to_string(),
            operation: Operation::Update,
            old_data: Some(old_data),
            new_data: Some(new_data),
            buffer_page_id: Some(buffer_page_id),
            file_id: Some(file_id),
            page_id: Some(page_id),
            slot: Some(slot),
        }
    }
}
/*
impl FileEncoding<WalRaw> for WalRaw {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.date_created.to_le_bytes());
        concat_bytes.extend_from_slice(&self.transaction_id.to_le_bytes());
        concat_bytes.extend_from_slice(&self.transaction_size.to_le_bytes());
        concat_bytes.extend_from_slice(&self.catalog_table_id.to_le_bytes());
        concat_bytes.extend_from_slice(&self.operation.to_le_bytes());
        concat_bytes.extend_from_slice(&bincode::serialize());
    }

    fn from_bytes(bytes: &[u8], _schema: Option<&Schema>) -> Result<WalRaw, Error> {
        todo!()
    }
}
*/

#[cfg(test)]
mod tests {
    use crate::storage::buffer::wal_raw::WalRaw;
    use crate::storage::file::tuple::Tuple;
    use crate::storage::schema::encoding::SchemaEncoding;
    use crate::storage::schema::Schema;

    #[test]
    fn test() {
        let ins = WalRaw::insert(
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
        );
        println!("{:?}", bincode::serialize(&ins));
    }
}
