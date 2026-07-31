#!sh

case $1 in
    --single)
        cargo build --release --target x86_64-unknown-linux-musl
    ;;
    *)
        cargo build --release --target x86_64-unknown-linux-musl
        cargo build --release --target x86_64-apple-darwin
        cargo build --release --target aarch64-unknown-linux-musl
        cargo build --release --target aarch64-apple-darwin
    ;;
esac
