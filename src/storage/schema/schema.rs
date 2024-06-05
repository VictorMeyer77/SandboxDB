use serde::{Deserialize, Serialize};

use crate::storage::schema::encoding::SchemaEncoding;
use crate::storage::schema::field::Field;
use crate::storage::schema::error::SchemaError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn tuple_size(&self, nulls: Option<&[u8]>) -> usize {
        self.fields
            .iter()
            .zip(nulls.unwrap_or(&vec![0; self.fields.len()]).iter())
            .filter(|(_, n)| **n == 0)
            .map(|(f, _)| f.get_type().get_byte_size())
            .sum()
    }
}

impl SchemaEncoding<Schema> for Schema {
    fn from_str(schema: &str) -> Result<Schema, SchemaError> {
        let fields_str = schema.trim().split_terminator(",");
        let fields_result: Vec<Result<Field, SchemaError>> =
            fields_str.map(|field| Field::from_str(field)).collect();
        if fields_result.iter().any(|res| res.is_err()) {
            let errors: Vec<SchemaError> =
                fields_result.into_iter().filter_map(Result::err).collect();
            Err(SchemaError::InvalidSchema(format!(
                "There are some errors in your schema: {:?}\nGiven schema: {}",
                errors, schema
            )))
        } else {
            let fields: Vec<Field> = fields_result.into_iter().filter_map(Result::ok).collect();
            Ok(Schema { fields })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::schema::_type::Type;

    use super::*;

    #[test]
    fn from_str_should_return_struct() {
        assert_eq!(
            Schema::from_str("id INT, name STRING, minor BOOLEAN, ").unwrap(),
            Schema {
                fields: vec![
                    Field::build("id".to_string(), Type::INT),
                    Field::build("name".to_string(), Type::STRING),
                    Field::build("minor".to_string(), Type::BOOLEAN)
                ],
            },
        );
    }

    #[test]
    #[should_panic]
    fn from_str_should_panic_with_invalid_schema() {
        Schema::from_str("id INT, name STRIN, minor ").unwrap();
    }

    #[test]
    fn tuple_size_should_return_max_bytes() {
        let schema =
            Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
        assert_eq!(33, schema.tuple_size(None));
    }

    #[test]
    fn tuple_size_should_return_size_with_null() {
        let schema =
            Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
        assert_eq!(9, schema.tuple_size(Some(&[1, 0, 0, 1])));
    }
}
