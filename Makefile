# Makefile - Nexus Rust NanoClaw runtime gates

.PHONY: build check test verify show-config runtime-status runtime-inspect clean

build:
	cargo build --bin nanoclaw

check:
	cargo check --all-targets

test:
	cargo test --all-targets

show-config:
	cargo run --quiet --bin nanoclaw -- show-config

runtime-status:
	cargo run --quiet --bin nanoclaw -- runtime status

runtime-inspect:
	cargo run --quiet --bin nanoclaw -- runtime inspect --limit 5

verify: check test show-config runtime-status runtime-inspect
	git diff --check

clean:
	cargo clean
