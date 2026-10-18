//! Interop against the C++ reference server in `examples/seastar`.
//!
//! Opt-in: build and start the server (see `examples/seastar/HACKING.md`),
//! then run
//!
//! ```text
//! SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo test --test interop -- --ignored
//! ```
//!
//! `compressed_calls` needs the server started with `--compress true`, as
//! `make seastar-server` does.

use std::time::Duration;

use bytes::Bytes;
use seastar_rpc::rpc::{CallOptions, Client, Compression, Error, with_timeout};

// Verb ids and signatures follow examples/seastar/rpc_server.cc.
// C++ `int` = i32, `long` = i64 on Linux.

async fn client() -> Client {
    client_compressed(None).await
}

async fn client_compressed(compression: Option<Compression>) -> Client {
    let addr = std::env::var("SEASTAR_RPC_ADDR").unwrap_or_else(|_| "127.0.0.1:10000".into());
    Client::builder()
        .offer_compression(compression)
        .connect_timeout(Duration::from_secs(3))
        .default_timeout(Duration::from_secs(5))
        .connect(addr)
        .await
        .expect("is the C++ rpc_server running? set SEASTAR_RPC_ADDR")
}

#[tokio::test]
#[ignore]
async fn negotiation() {
    let c = client().await;
    assert!(c.features().timeout, "{:?}", c.features());
}

#[tokio::test]
#[ignore]
async fn simple_verbs() {
    let c = client().await;
    c.call::<(i32,), ()>(1, (1,)).await.unwrap();
    assert_eq!(c.call::<(i32, i32), i32>(2, (3, 4)).await.unwrap(), 7);
    assert!((c.call::<(f64,), f64>(3, (1.0,)).await.unwrap() - 1f64.sin()).abs() < 1e-12);
    c.call_with::<(), ()>(5, (), CallOptions::default().no_wait())
        .await
        .unwrap();
    c.call::<(i32,), ()>(6, (42,)).await.unwrap();
    assert_eq!(c.call::<(i64, i64), i64>(7, (10, 3)).await.unwrap(), 7);
    assert_eq!(c.call::<(), (i64, i32)>(10, ()).await.unwrap(), (1, 2));
    assert_eq!(c.call::<(), u64>(11, ()).await.unwrap(), 1);
    c.call::<(i32, String), ()>(12, (10, "payload".into()))
        .await
        .unwrap();
    c.call::<(), ()>(13, ()).await.unwrap();
    assert_eq!(
        c.call::<(String,), String>(15, ("héllo".into(),))
            .await
            .unwrap(),
        "héllo"
    );
}

#[tokio::test]
#[ignore]
async fn trailing_optional() {
    let c = client().await;
    assert_eq!(
        c.call::<(i64, i64, Option<i32>), i64>(9, (1, 2, None))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        c.call::<(i64, i64, Option<i32>), i64>(9, (1, 2, Some(3)))
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
#[ignore]
async fn exceptions() {
    let c = client().await;
    let err = c.call::<(), ()>(4, ()).await.unwrap_err();
    assert!(
        matches!(&err, Error::Remote(m) if m.to_string_lossy().contains("exception!")),
        "{err:?}"
    );
    let err = c
        .call_raw(999, Bytes::new(), CallOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UnknownVerb(999)), "{err:?}");
    // The connection survives both.
    assert_eq!(c.call::<(i32, i32), i32>(2, (1, 1)).await.unwrap(), 2);
}

#[tokio::test]
#[ignore]
async fn timeout() {
    let c = client().await;
    let err = c
        .call_with::<(), ()>(8, (), with_timeout(Duration::from_millis(500)))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout), "{err:?}");
    assert_eq!(c.call::<(i32, i32), i32>(2, (2, 2)).await.unwrap(), 4);
}

#[tokio::test]
#[ignore]
async fn concurrent() {
    let c = client().await;
    let calls: Vec<_> = (0..100)
        .map(|i| {
            let c = c.clone();
            tokio::spawn(async move { (i, c.call::<(i32, i32), i32>(2, (i, i)).await.unwrap()) })
        })
        .collect();
    for call in calls {
        let (i, r) = call.await.unwrap();
        assert_eq!(r, 2 * i);
    }
}

#[tokio::test]
#[ignore]
async fn compressed_calls() {
    for algo in [Compression::Lz4, Compression::Lz4Fragmented] {
        let c = client_compressed(Some(algo)).await;
        let chosen = c.features().compress.as_deref();
        assert_eq!(
            chosen,
            Some(algo.name().as_bytes()),
            "start the server with --compress true"
        );

        assert_eq!(c.call::<(i32, i32), i32>(2, (3, 4)).await.unwrap(), 7);

        // Over 32 KiB, so `LZ4_FRAGMENTED` splits it into chunks both ways.
        let big: String = (0..100_000)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        assert_eq!(
            c.call::<(String,), String>(15, (big.clone(),))
                .await
                .unwrap(),
            big
        );

        let err = c.call::<(), ()>(4, ()).await.unwrap_err();
        assert!(matches!(err, Error::Remote(_)), "{err:?}");
    }
}
