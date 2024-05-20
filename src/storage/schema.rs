use std::error::Error;
use std::fmt;

// TODO display/debug properly

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    _type: Type,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    BOOLEAN,   // bool
    TINYINT,   // i8
    SMALLINT,  // i16
    INT,       // i64
    BIGINT,    // i128
    FLOAT,     // f64
    TIMESTAMP, // u64
    STRING,
}

#[derive(Clone, PartialEq)]
pub enum SchemaError {
    InvalidType(String),
    InvalidField(String),
    InvalidSchema(String),
}

impl Schema {
    pub fn from_string(schema: String) -> Result<Schema, SchemaError> {
        let fields_str = schema.trim().split_terminator(",");
        let fields_result: Vec<Result<Field, SchemaError>> = fields_str
            .map(|field| Field::from_string(field.to_string()))
            .collect();
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

impl Field {
    fn from_string(field_string: String) -> Result<Field, SchemaError> {
        let name_and_type: Vec<&str> = field_string.trim().split_whitespace().collect();
        if name_and_type.len() != 2 {
            Err(SchemaError::InvalidField(format!(
                "\n- Invalid field syntax. Expected \"column_name column_type\" Actual \"{}\"",
                field_string
            )))
        } else {
            Ok(Field {
                name: name_and_type[0].to_string(),
                _type: Type::from_string(name_and_type[1].to_string())?,
            })
        }
    }
}

impl Type {
    fn from_string(type_string: String) -> Result<Type, SchemaError> {
        match type_string.to_uppercase().trim() {
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "TINYINT" => Ok(Type::TINYINT),
            "SMALLINT" => Ok(Type::SMALLINT),
            "INT" => Ok(Type::INT),
            "BIGINT" => Ok(Type::BIGINT),
            "FLOAT" => Ok(Type::FLOAT),
            "TIMESTAMP" => Ok(Type::TIMESTAMP),
            "STRING" => Ok(Type::STRING),
            _ => Err(SchemaError::InvalidType(format!(
                "\n- Unknown type \"{}\"",
                type_string
            ))),
        }
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaError::InvalidType(ref msg) => write!(f, "{}", msg),
            SchemaError::InvalidField(ref msg) => write!(f, "{}", msg),
            SchemaError::InvalidSchema(ref msg) => write!(f, "{}", msg),
        }
    }
}

impl fmt::Debug for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for SchemaError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Schema

    #[test]
    fn schema_from_string_should_return_struct() {
        assert_eq!(
            Schema::from_string("id INT, name STRING, minor BOOLEAN, ".to_string()).unwrap(),
            Schema {
                fields: vec![
                    Field {
                        name: "id".to_string(),
                        _type: Type::INT,
                    },
                    Field {
                        name: "name".to_string(),
                        _type: Type::STRING,
                    },
                    Field {
                        name: "minor".to_string(),
                        _type: Type::BOOLEAN,
                    },
                ],
            },
        );
    }

    #[test]
    #[should_panic]
    fn schema_from_string_should_panic_with_invalid_schema() {
        Schema::from_string("id INT, name STRIN, minor ".to_string()).unwrap();
    }

    // Field

    #[test]
    fn field_from_string_should_return_struct() {
        assert_eq!(
            Field::from_string("id INT".to_string()).unwrap(),
            Field {
                name: "id".to_string(),
                _type: Type::INT
            }
        );
    }

    #[test]
    #[should_panic]
    fn field_from_string_should_panic_if_invalid_string() {
        Field::from_string("id INT fail".to_string()).unwrap();
    }

    // Type

    #[test]
    fn type_from_string_should_return_enum() {
        assert_eq!(
            Type::from_string("boolean".to_string()).unwrap(),
            Type::BOOLEAN
        );
        assert_eq!(
            Type::from_string("TinyINT".to_string()).unwrap(),
            Type::TINYINT
        );
        assert_eq!(
            Type::from_string("SMALLINT".to_string()).unwrap(),
            Type::SMALLINT
        );
        assert_eq!(Type::from_string("int".to_string()).unwrap(), Type::INT);
        assert_eq!(
            Type::from_string("bigint".to_string()).unwrap(),
            Type::BIGINT
        );
        assert_eq!(Type::from_string("float".to_string()).unwrap(), Type::FLOAT);
        assert_eq!(
            Type::from_string("Timestamp".to_string()).unwrap(),
            Type::TIMESTAMP
        );
        assert_eq!(
            Type::from_string("string".to_string()).unwrap(),
            Type::STRING
        );
    }

    #[test]
    #[should_panic]
    fn type_from_string_should_return_err_if_not_exist() {
        Type::from_string("unknown".to_string()).unwrap();
    }
}
