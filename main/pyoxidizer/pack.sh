# [ -f ./relay/pyoxidizer.bzl ] || pyoxidizer init-config-file relay
# pyoxidizer build --path ./relay --release --target-triple x86_64-pc-unknown-linux-gnu


# pyoxidizer init-config-file .
# PATH="../
pyoxidizer build --release --target-triple x86_64-unknown-linux-musl
# pyoxidizer build --release
