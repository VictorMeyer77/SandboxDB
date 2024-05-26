use std::hash::{DefaultHasher, Hash, Hasher};

use sandboxdb::storage::file::page::Page;
use sandboxdb::storage::schema::encoding::SchemaEncoding;
use sandboxdb::storage::schema::schema::Schema;

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(&schema, 8192, [0, 0, 1], 0).unwrap();
    let mut hasher = DefaultHasher::new();
    page.hash(&mut hasher);
    println!("{:?}", hasher.finish());
}
