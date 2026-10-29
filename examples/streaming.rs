//! Stream: open a stream next to a client connection, hand it to a verb
//! that returns a sink on the same stream, then write ints and read them
//! back doubled, both directions at once.
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --release --example streaming [-- [--compress lz4|lz4_fragmented] host:port [count]]
//! ```

use std::time::{Duration, Instant};

use seastar_rpc::rpc::{Client, Error, StreamConnection, StreamId, Verb};

mod common;

/// `rpc::sink<int> (rpc::source<int>)`: writes back every int doubled.
const DOUBLE: Verb = 18;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (args, compression) = common::args()?;
    let mut args = args.into_iter();
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:10000".into());
    let count: i32 = args.next().map_or(Ok(10_000), |s| s.parse())?;

    let client = Client::builder()
        .offer_compression(compression)
        .connect_timeout(Duration::from_secs(3))
        .default_timeout(Duration::from_secs(5))
        .connect(&addr)
        .await?;

    let StreamConnection {
        mut sink,
        mut source,
        id,
        ..
    } = client.open_stream().await?;
    println!("connected to {addr}, stream {id:?}");

    // The server answers with its sink, which lives on the same stream.
    let back: StreamId = client.call(DOUBLE, id).await?;
    assert_eq!(back, id);

    let started = Instant::now();

    let send = async {
        for i in 0..count {
            sink.send_item(&i).await?;
        }
        sink.close().await
    };

    let receive = async {
        let mut expect = 0;
        while let Some(x) = source.recv_item::<i32>().await? {
            assert_eq!(x, expect * 2);
            expect += 1;
        }
        assert_eq!(expect, count, "stream ended early");
        Ok::<_, Error>(())
    };

    // Both directions run concurrently: the server stops reading once its
    // replies fill a buffer nobody drains.
    tokio::try_join!(send, receive)?;

    let elapsed = started.elapsed();
    println!(
        "{count} items in {elapsed:?}, {:.0} items/s",
        f64::from(count) / elapsed.as_secs_f64()
    );
    Ok(())
}
