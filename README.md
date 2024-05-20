# sandboxdb

#### run test with coverage

https://github.com/mozilla/grcov

cargo install grcov
rustup component add llvm-tools-preview
export RUSTFLAGS="-Cinstrument-coverage"
cargo build
export LLVM_PROFILE_FILE="your_name-%p-%m.profraw"
cargo test
grcov . -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/