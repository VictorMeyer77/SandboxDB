use sandboxdb::storage::file::file::File;
use sandboxdb::storage::file::page::Page;
use sandboxdb::storage::schema::encoding::SchemaEncoding;
use sandboxdb::storage::schema::schema::Schema;

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(&schema, 8192, 0).unwrap();
    let mut file = File::build(10 * 8192 + 50, 0, [0, 0, 1]);
    file.insert_page(&page).unwrap()
}
