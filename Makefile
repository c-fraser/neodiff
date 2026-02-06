.PHONY: all build check clean test

default: help

help: ## show this help
	@echo 'usage: make [target]'
	@echo ''
	@echo 'targets:'
	@egrep '^(.+)\:\ .*##\ (.+)' ${MAKEFILE_LIST} | sed 's/:.*##/#/' | column -t -c 2 -s '#'

all: check build test ## check, build, and test all code

build: clean ## build rust release binaries
	cargo build --release

check: ## check, format, and lint rust code
	cargo check --workspace
	cargo fmt --all
	cargo clippy --workspace -- -D warnings
	docker run --rm -v $(CURDIR)/src:/src ghcr.io/google/addlicense -c c-fraser -l apache -y 2025 /src

clean: ## remove build files
	cargo clean

test: ## run rust tests
	cargo test --workspace
