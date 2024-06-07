use sandboxdb::storage::file::encoding::FileEncoding;
use sandboxdb::storage::file::file::File;
use sandboxdb::storage::file::page::Page;
use sandboxdb::storage::schema::encoding::SchemaEncoding;
use sandboxdb::storage::schema::schema::Schema;
use sandboxdb::storage::tablespace::metastore::Metastore;
use std::fs;
use std::io::{Read, Write};

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(&schema, 8192, 0).unwrap();
    let mut file = File::build(10 * 8192 + 50, 0, [0, 0, 1]);
    file.insert_page(&page).unwrap();

    use_case_01();
}

fn use_case_01() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    /*let mut metastore = Metastore::build("./metastore01").unwrap();
    let mut database = metastore.new_database("bronze", None).unwrap();
    let mut table = database.new_table("free", None, &schema).unwrap();
    let (name, path) = table.new_file().unwrap();
    let mut file = File::build(8192 * 10 + 13, 0, [0 , 0, 1]);
    let mut page = Page::build(&schema, 8192, 0).unwrap();

    for _ in 0..100 {
        page.insert( &[0; 4], &[2; 33]).unwrap()
    }

    file.insert_page(&page).unwrap();

    let mut f = fs::File::options().write(true).open(path);
    f.unwrap().write_all(&file.as_bytes()).unwrap();

    println!("{:?}", metastore)*/
    //let f = fs::read_to_string("C:/Users/vmeyer/OneDrive - TF1/Documents/Dev/sandboxdb/metastore01/bronze/free/0").unwrap();
    //let file = File::from_bytes(f.as_bytes(), Some(&schema)).unwrap();
    let tt = fs::read(
        "C:/Users/vmeyer/OneDrive - TF1/Documents/Dev/sandboxdb/metastore01/bronze/free/0",
    )
    .unwrap();
    println!("{:?}", tt.len());
    println!("{:?}", tt);
    println!("{:?}", File::from_bytes(&tt, Some(&schema)));
}
