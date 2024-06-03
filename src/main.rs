use sandboxdb::storage::file::file::File;
use sandboxdb::storage::file::page::Page;
use sandboxdb::storage::schema::encoding::SchemaEncoding;
use sandboxdb::storage::schema::schema::Schema;
use sandboxdb::storage::tablespace::metastore::Metastore;

fn main() {
    let schema =
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap();
    let page = Page::build(&schema, 8192, 0).unwrap();
    let mut file = File::build(10 * 8192 + 50, 0, [0, 0, 1]);
    file.insert_page(&page).unwrap();
    //let mut metastore = Metastore::from_file(&PathBuf::from("./metastore01")).unwrap();
    let mut metastore = Metastore::build("./metastore01").unwrap();
    let mut database = metastore.new_database("bronze", None).unwrap();
    let mut table = database
        .new_table(
            "free",
            None,
            &Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN").unwrap(),
        )
        .unwrap();
    let (name, path) = table.new_file().unwrap();
    metastore.load_databases().unwrap();
}
