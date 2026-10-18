//! Raw connection: pipeline many `add` requests and read the replies, with no
//! multiplexing client in between.
//!
//! The sender is fed every request without a flush per frame, so frames are
//! coalesced into large writes, and the receiver drains replies at the same
//! time. Replies may arrive out of order; `msg_id` tells which call each one
//! answers.
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --release --example raw [-- [--compress lz4|lz4_fragmented] host:port [count]]
//! ```

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt, stream};
use seastar_rpc::rpc::raw::{RawConnection, RequestFrame};
use seastar_rpc::rpc::{Client, Error, Verb, from_payload, to_payload};

mod common;

/// `int (int, int)`: returns `a + b`.
const ADD: Verb = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (args, compression) = common::args()?;
    let mut args = args.into_iter();
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:10000".into());
    let count: i32 = args.next().map_or(Ok(100_000), |s| s.parse())?;

    let RawConnection {
        mut sender,
        mut receiver,
        features,
        ..
    } = Client::builder()
        .offer_compression(compression)
        .connect_timeout(Duration::from_secs(3))
        .connect_raw(&addr)
        .await?;
    println!("connected to {addr}, features {features:?}");

    let started = Instant::now();

    // Call `i` is `add(i, 1)` with `msg_id = i`; msg ids must be positive.
    let mut requests = stream::iter(1..=count)
        .map(|i| Ok(RequestFrame::new(i64::from(i), ADD, to_payload(&(i, 1i32)))));

    // `send_all` feeds frames into the write buffer and flushes only when it
    // fills or the stream ends, instead of one syscall per request.
    let send = sender.send_all(&mut requests);

    let receive = async {
        for _ in 0..count {
            let frame = receiver.next().await.ok_or(Error::Closed)??;
            let i = i32::try_from(frame.call_id()).expect("msg_id is an i32 we sent");
            let sum: i32 = from_payload(frame.into_result()?)?;
            assert_eq!(sum, i + 1, "wrong reply for msg_id {i}");
        }
        Ok::<_, Error>(())
    };

    // Both directions run concurrently in one task: if we wrote everything
    // first, the server could block writing replies nobody reads while we
    // block writing requests it no longer reads.
    tokio::try_join!(send, receive)?;

    let elapsed = started.elapsed();
    println!(
        "{count} calls in {elapsed:?}, {:.0} calls/s",
        f64::from(count) / elapsed.as_secs_f64()
    );
    Ok(())
}
