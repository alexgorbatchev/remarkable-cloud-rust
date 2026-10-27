default: check

fmt:
    cargo fmt --package remarkable-cloud

test:
    cargo test --all-targets
    cargo test --doc

lint:
    cargo clippy --all-targets -- -D warnings

coverage:
    cargo llvm-cov --all-targets --fail-under-lines 90

check:
    cargo fmt --package remarkable-cloud -- --check
    cargo clippy --all-targets -- -D warnings
    cargo test --all-targets
    cargo test --doc
    cargo llvm-cov --all-targets --fail-under-lines 90

build:
    cargo build --all-targets
