# Makefile - Nexus Rust NanoClaw runtime gates

.PHONY: build check test verify show-config clean

build:
	cargo build --bin nanoclaw

check:
	cargo check --all-targets

test:
	cargo test --all-targets

show-config:
	cargo run --quiet --bin nanoclaw -- show-config

verify: check test show-config
	git diff --check

clean:
	cargo clean
