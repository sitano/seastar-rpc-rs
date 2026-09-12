# Building the Seastar RPC example

`rpc_server.cc` and `rpc_client.cc` are a small Seastar RPC server and
client demo, built against a local Seastar checkout via CMake's
`find_package(Seastar)` support. They share the payload serializer and LZ4
compressor factory in `rpc_serializer.hh`.

## Prerequisites

- A built Seastar tree with CMake package files generated (`SeastarConfig.cmake`,
  `SeastarTargets.cmake` in the build directory). This is produced by
  Seastar's own `configure.py` / CMake build, e.g.:

  ```
  cd ~/Projects/seastar
  ./configure.py --mode=debug
  ninja -C build/debug
  ```

- A compiler matching what Seastar was built with (currently GNU 16.2.1,
  C++26). A mismatch prints a CMake warning but usually still builds.

## Configuring seastar paths

By default the build looks for Seastar at:

- source tree: `~/Projects/seastar`
- build tree: `~/Projects/seastar/build/debug`

Override either with environment variables:

```
export SEASTAR_SOURCE_DIR=/path/to/seastar
export SEASTAR_BUILD_DIR=/path/to/seastar/build/dev
```

or with CMake cache variables (takes precedence over the environment):

```
cmake -S . -B build -G Ninja \
  -DSEASTAR_SOURCE_DIR=/path/to/seastar \
  -DSEASTAR_BUILD_DIR=/path/to/seastar/build/dev
```

`SEASTAR_BUILD_DIR` defaults to `${SEASTAR_SOURCE_DIR}/build/debug` when not
set explicitly.

## Build

```
cmake -S . -B build -G Ninja
cmake --build build
```

This produces `build/rpc_server` and `build/rpc_client`.

## Run

```
# server (default port 10000)
./build/rpc_server --port 10000

# client, in another terminal: one call per invocation
./build/rpc_client --server 127.0.0.1:10000 VERB [args...]
./build/rpc_client --server 127.0.0.1:10000 2 3 4      # verb 2 -> 7
```

Pass `--compress true` on both sides to exercise the LZ4 compressor path.
The client also takes `--timeout <ms>` (default 5000).

## Verbs

| Verb | Signature | Server behavior |
|---|---|---|
| 1 | `void (int)` | prints a call counter and the argument |
| 2 | `int (int, int)` | returns `a + b` |
| 3 | `double (double)` | returns `sin(x)` |
| 4 | `void ()` | always throws (`USER` exception) |
| 5 | `no_wait_type ()` | fire and forget, no response frame |
| 6 | `void (int)` | handler also receives `client_info`, prints peer address |
| 7 | `long (long, long)` | returns `a - b` after a 1 s delay |
| 8 | `void ()` | sleeps 2 s |
| 9 | `long (long, long, optional<int>)` | returns 2, or 3 if the optional is present |
| 10 | `tuple<long, int> ()` | returns `(1, 2)` |
| 11 | `uint64_t ()` | returns 1 |
| 12 | `void (int sleep_ms, sstring)` | sleeps `sleep_ms` |
| 13 | `void ()` | sleeps 1 ms |
| 14 | `void (sstring)` | rejected once the request exceeds the server's 10 MB `max_memory` |
| 15 | `sstring (sstring)` | echoes the string back |

## Payload encoding

`rpc_serializer.hh` defines the payload format a Rust peer must match
(see `docs/rpc-wire-protocol-client-guide.md` §7 for the envelope rules
that apply regardless of serializer):

- `int32_t`, `uint32_t`, `int64_t`, `uint64_t`, `double`: raw in-memory
  bytes, i.e. host byte order (little-endian on x86-64 / aarch64), no
  length prefix. `int` is `int32_t`, `long` is `int64_t` on Linux.
- `sstring`: `uint32_t` byte length (host order) followed by the raw bytes,
  no terminator.
- Multiple arguments are concatenated; a trailing `rpc::optional<T>` is
  encoded as nothing when absent; `rpc::tuple` return values are flattened.
