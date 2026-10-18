//! Payloads encoded with serde: a `Bin<T>` wrapper that implements `Encode`
//! and `Decode` for any `Serialize` / `Deserialize` type using bincode.
//!
//! bincode's legacy config writes integers fixed-width little-endian and
//! struct fields back to back, so `Sum { x, y }` is 8 bytes, the same as
//! `(i32, i32)`. The server reads it with two plain `int32_t` reads (see
//! `sum` in `examples/seastar/rpc_serializer.hh`).
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --example serde -- [--server host:port] [--compress lz4|lz4_fragmented] [--timeout ms] [X Y]
//! cargo run --example serde -- 3 4        # Sum { x: 3, y: 4 } -> 7
//! ```

use std::time::Duration;

use bincode::config::{self, Config};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use seastar_rpc::rpc::{Client, Decode, DecodeError, Encode, Verb};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

mod common;

const USAGE: &str = "usage: serde [--server host:port] [--compress lz4|lz4_fragmented[,...]] [--timeout ms] [X Y]\n";

/// `int (sum)`: returns `x + y`.
const SUM: Verb = 17;

#[derive(Debug, Serialize, Deserialize)]
struct Sum {
    x: i32,
    y: i32,
}

/// Fixed-width little-endian integers, matching the server's serializer.
fn bincode_config() -> impl Config {
    config::legacy()
}

/// A value encoded with bincode, in place in the payload.
struct Bin<T>(T);

impl<T: Serialize> Encode for Bin<T> {
    fn encode(&self, out: &mut BytesMut) {
        bincode::serde::encode_into_std_write(&self.0, &mut out.writer(), bincode_config())
            .expect("value serializes with bincode");
    }
}

impl<T: DeserializeOwned> Decode for Bin<T> {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        let (value, read) = bincode::serde::decode_from_slice(input, bincode_config())
            .map_err(|e| DecodeError::Invalid(e.to_string()))?;
        input.advance(read);
        Ok(Bin(value))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = "127.0.0.1:10000".to_string();
    let mut timeout = Duration::from_millis(5000);
    let mut rest = Vec::new();
    let (args, compression) = common::args()?;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().ok_or(USAGE)?,
            "--timeout" => timeout = Duration::from_millis(args.next().ok_or(USAGE)?.parse()?),
            _ => rest.push(arg),
        }
    }
    let (x, y) = match rest.as_slice() {
        [] => (3, 4),
        [x, y] => (x.parse()?, y.parse()?),
        _ => return Err(USAGE.into()),
    };

    let client = Client::builder()
        .offer_compression(compression)
        .connect_timeout(timeout)
        .default_timeout(timeout)
        .connect(server)
        .await?;

    let req = Sum { x, y };
    let sum: i32 = client.call(SUM, Bin(&req)).await?;
    println!("{req:?} -> {sum}");
    assert_eq!(sum, x + y);

    // Many requests in one batch: same wrapper, replies in push order.
    let sums: Vec<i32> = client
        .call_many::<_, i32>(SUM, (0..5).map(|i| Bin(Sum { x: i, y: 10 * i })))
        .await?
        .collect::<Result<_, _>>()?;
    println!("batch -> {sums:?}");
    assert_eq!(sums, [0, 11, 22, 33, 44]);

    // `Bin` decodes too; here a reply-shaped round trip without the server.
    let mut payload = seastar_rpc::rpc::codec::serialize::to_payload(&Bin(&req));
    assert_eq!(payload.len(), 8);
    let Bin(back): Bin<Sum> = Decode::decode(&mut payload)?;
    println!("local round trip -> {back:?}");
    Ok(())
}
