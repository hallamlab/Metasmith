FROM rust:trixie

RUN rustup target add x86_64-unknown-linux-musl
RUN rustup target add x86_64-apple-darwin
RUN rustup target add aarch64-unknown-linux-musl
RUN rustup target add aarch64-apple-darwin

RUN cargo install cargo-zigbuild