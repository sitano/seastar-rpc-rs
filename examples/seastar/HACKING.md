# Building the Seastar RPC example

`rpc_server.cc` and `rpc_client.cc` are a small Seastar RPC server and
client demo, built against Seastar via CMake's `find_package(Seastar)`
support: either an installed Seastar or a local build tree. They share the payload serializer and LZ4
compressor factory in `rpc_serializer.hh`.

## Prerequisites

- Seastar, either installed (`SeastarConfig.cmake` under
  `<prefix>/lib/cmake/Seastar`) or as a built source tree with CMake package
  files generated (`SeastarConfig.cmake`, `SeastarTargets.cmake` in the build
  directory). A build tree is produced by Seastar's own `configure.py` /
  CMake build, e.g.:

  ```
  cd ~/Projects/seastar
  ./configure.py --mode=debug
  ninja -C build/debug
  ```

- A compiler matching what Seastar was built with (currently GNU 16.2.1,
  C++26). A mismatch prints a CMake warning but usually still builds.

## Configuring seastar paths

Seastar is looked up in this order:

1. **Explicit build tree.** If `SEASTAR_BUILD_DIR` is set, that tree is
   used and configuration fails if it is not valid. Set it with an
   environment variable or a CMake cache variable (the cache variable takes
   precedence):

   ```
   export SEASTAR_BUILD_DIR=/path/to/seastar/build/dev
   # or
   cmake -S . -B build -G Ninja -DSEASTAR_BUILD_DIR=/path/to/seastar/build/dev
   ```

   A build tree also needs the Seastar source tree's `cmake/` modules.
   `SEASTAR_SOURCE_DIR` defaults to two levels above the build dir
   (`<src>/build/<mode>`). Set it too (env or `-D`) for any other layout.

2. **Standard discovery.** `find_package(Seastar)` with CMake's normal
   search: `Seastar_DIR`, `CMAKE_PREFIX_PATH`, and system install prefixes.
   Use this for an installed Seastar, e.g.
   `-DCMAKE_PREFIX_PATH=/opt/seastar`. The user package registry
   (`~/.cmake/packages`) is skipped on purpose: every Seastar build tree
   registers itself there, and build trees need the source-tree setup from
   step 1 or 3.

3. **Local checkout fallback.** `~/Projects/seastar/build/<build type>`,
   where `<build type>` is the lowercased `CMAKE_BUILD_TYPE` (default
   `Debug`, so `build/debug`; `-DCMAKE_BUILD_TYPE=Release` uses
   `build/release`).

Configure prints the Seastar it picked:

```
-- Using Seastar from /home/you/Projects/seastar/build/debug
```

`SEASTAR_SOURCE_DIR` and `SEASTAR_BUILD_DIR` are cached in the build
directory, so to change them start a fresh build directory or pass the new
values with `-D`.

## Build

```
cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++
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
| 16 | `int (int, int)` | returns `a + b` without printing (for benchmarks) |

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
