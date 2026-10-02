#!/usr/bin/env python3
"""Reproduces a Seastar RPC server bug: a second simultaneous connection
gets RST, even though the first one negotiates fine.

No Rust, no project code involved - just two raw TCP sockets and a
hand-built Seastar RPC negotiation frame. Point it at any
rpc::protocol<...>::server (this project's rpc_server, or upstream
Seastar's demos/rpc_demo), e.g.:

    python3 repro_second_conn_reset.py --port 10000
"""

import argparse
import socket
import struct
import time


def negotiation_frame() -> bytes:
    # MAGIC "SSTARRPC" + u32 LE records-length. Zero records: we offer no
    # features, which is a valid, minimal negotiation offer.
    return b"SSTARRPC" + struct.pack("<I", 0)


def connect_and_negotiate(host: str, port: int, tag: str) -> socket.socket:
    s = socket.create_connection((host, port), timeout=3)
    t0 = time.time()
    s.sendall(negotiation_frame())
    print(f"[{t0:.3f}] {tag}: connected+sent negotiation, local={s.getsockname()}")
    return s


def check_reply(tag: str, s: socket.socket) -> None:
    s.settimeout(2.0)
    try:
        data = s.recv(4096)
        if data:
            print(f"[{time.time():.3f}] {tag}: OK, got negotiation reply "
                  f"({len(data)} bytes): {data[:20]!r}")
        else:
            print(f"[{time.time():.3f}] {tag}: EOF (graceful close) on recv")
    except ConnectionResetError as e:
        print(f"[{time.time():.3f}] {tag}: RESET on recv -> {e}")
    except socket.timeout:
        print(f"[{time.time():.3f}] {tag}: recv timed out, still open (unexpected)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=10000)
    ap.add_argument("--delay", type=float, default=0.1,
                     help="seconds between opening conn1 and conn2")
    args = ap.parse_args()

    s1 = connect_and_negotiate(args.host, args.port, "conn1")
    time.sleep(args.delay)
    s2 = connect_and_negotiate(args.host, args.port, "conn2")

    check_reply("conn1", s1)
    check_reply("conn2", s2)

    s1.close()
    s2.close()


if __name__ == "__main__":
    main()
