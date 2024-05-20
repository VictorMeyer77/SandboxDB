use sandboxdb::storage::schema::Schema;

fn main() {
    println!(
        "{:?}",
        Schema::from_string(
            "id INT, \
        date TIMESTAMP"
                .to_string()
        )
        .unwrap()
    );
}
