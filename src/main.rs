use crc32fast::hash;

use sandboxdb::storage::page::encoding::Encoding;
use sandboxdb::storage::page::page::Page;
use sandboxdb::storage::schema::encoding::Encoding as SchemaEncoding;
use sandboxdb::storage::schema::schema::Schema;

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(&schema, 8192, [0, 0, 1], 0).unwrap();
    let hash = hash(&page.as_bytes());
}
