use std::fs;
use std::io::Write;

use sandboxdb::storage::buffer::buffer_pool::BufferPool;
use sandboxdb::storage::file::encoding::Encoding as FileEncoding;
use sandboxdb::storage::file::page::Page;
use sandboxdb::storage::file::tuple::Tuple;
use sandboxdb::storage::file::File;
use sandboxdb::storage::schema::encoding::Encoding as SchemaEncoding;
use sandboxdb::storage::schema::Schema;
use sandboxdb::storage::tablespace::metastore::Metastore;

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(8192, 0).unwrap();
    let mut file = File::build(10 * 8192 + 50, 0, [0, 0, 1]);
    file.insert_page(&page).unwrap();

    use_case_01();
}

fn use_case_01() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let mut metastore = Metastore::build("./metastore01").unwrap();
    let mut database = metastore.new_database("bronze", None).unwrap();
    let mut table = database.new_table("free", None, &schema).unwrap();
    let (_, path) = table.new_file().unwrap();
    let mut file = File::build(8192 * 10 + 13, 0, [0, 0, 1]);
    let mut page = Page::build(8192, 0).unwrap();
    let tuple = Tuple::build(&schema, &[0; 4], &[2; 33]).unwrap();

    for _ in 0..100 {
        page.insert(tuple.clone()).unwrap()
    }

    file.insert_page(&page).unwrap();

    let f = fs::File::options().write(true).open(path);
    f.unwrap().write_all(&file.as_bytes().unwrap()).unwrap();

    //let f = fs::read_to_string("C:/Users/vmeyer/OneDrive - TF1/Documents/Dev/sandboxdb/metastore01/bronze/free/0").unwrap();
    //let file = File::from_bytes(f.as_bytes(), Some(&schema)).unwrap();

    let _ = BufferPool::build(1024 ^ 3, metastore.location.to_str().unwrap());
}
