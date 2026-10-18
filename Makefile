CARGO ?= cargo

# C++ compiler for the Seastar examples. Override with e.g. `make seastar CXX=g++`.
CXX = clang++

# Interop tests, `bench`, `example` and `dhat` talk to the C++ server from examples/seastar.
SEASTAR_RPC_ADDR ?= 127.0.0.1:10000
SEASTAR_DIR := examples/seastar
SEASTAR_BUILD := $(SEASTAR_DIR)/build

.DEFAULT_GOAL := help
.PHONY: help all build release test test-interop bench fmt fmt-check lint check doc \
	example examples dhat seastar seastar-server clean clean-seastar ci

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

all: fmt-check lint test ## Format check, lint and test

build: ## Build the library, examples and tests
	$(CARGO) build --all-targets

release: ## Build in release mode
	$(CARGO) build --release --all-targets

test: ## Run unit and fake-server tests
	$(CARGO) test

test-interop: ## Run interop tests against the C++ server at SEASTAR_RPC_ADDR
	SEASTAR_RPC_ADDR=$(SEASTAR_RPC_ADDR) $(CARGO) test --test interop -- --ignored

bench: ## Run call benchmarks against the C++ server at SEASTAR_RPC_ADDR
	SEASTAR_RPC_ADDR=$(SEASTAR_RPC_ADDR) $(CARGO) bench --bench call

fmt: ## Format the code
	$(CARGO) fmt --all

fmt-check: ## Check formatting
	$(CARGO) fmt --all -- --check

lint: ## Run clippy on all targets and features, warnings are errors
	$(CARGO) clippy --all-targets --all-features -- -D warnings

check: ## Type-check all targets
	$(CARGO) check --all-targets --all-features

doc: ## Build the docs
	$(CARGO) doc --no-deps

example: ## Run the client example, e.g. make example ARGS="2 3 4"
	$(CARGO) run --example client -- --server $(SEASTAR_RPC_ADDR) $(ARGS)

examples: ## Run the calls, concurrent, batches, raw and serde examples against the C++ server
	for e in calls concurrent batches raw; do $(CARGO) run --release --example $$e -- $(SEASTAR_RPC_ADDR) || exit 1; done
	$(CARGO) run --release --example serde -- --server $(SEASTAR_RPC_ADDR)

dhat: ## Run the client example with heap profiling (writes dhat-heap.json)
	RUSTFLAGS="$$RUSTFLAGS --cfg dhat_heap" $(CARGO) run --example client -- --server $(SEASTAR_RPC_ADDR) $(ARGS)

seastar: ## Build the C++ Seastar server and client (see examples/seastar/HACKING.md)
	cmake -S $(SEASTAR_DIR) -B $(SEASTAR_BUILD) -G Ninja \
		-DCMAKE_CXX_COMPILER=$(CXX)
	cmake --build $(SEASTAR_BUILD)

seastar-server: seastar ## Run the C++ Seastar server on the SEASTAR_RPC_ADDR port
	$(SEASTAR_BUILD)/rpc_server --compress true --port $(lastword $(subst :, ,$(SEASTAR_RPC_ADDR)))

ci: fmt-check lint test doc ## Everything CI should run

clean: ## Remove Rust build artifacts
	$(CARGO) clean
	rm -f dhat-heap.json

clean-seastar: ## Remove the C++ Seastar build
	rm -rf $(SEASTAR_BUILD)
