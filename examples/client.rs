//! Rust counterpart of `examples/seastar/rpc_client.cc` for a few verbs.
//!
//! ```text
//! cargo run --example client -- [--server host:port] [--timeout ms] [--no-wait-recv] [--no-wait-send] VERB [args...]
//! cargo run --example client -- 2 3 4        # verb 2 -> 7
//! cargo run --example client -- --no-wait-recv no_wait   # don't wait for a reply, only the write
//! RUSTFLAGS="--cfg dhat_heap" cargo run --example client -- 2 3 4   # also writes dhat-heap.json
//! ```

use std::time::Duration;

use seastar_rpc::rpc::{Client, with_timeout};

#[cfg(dhat_heap)]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const USAGE: &str =
    "usage: client [--server host:port] [--timeout ms] [--count N] [--no-wait-recv] [--no-wait-send] [--no-wait] VERB [args...]
verbs: add(2) <int> <int> | sin(3) <double> | throw(4) | no_wait(5) | pair(10) | echo(15) <string>
";

#[derive(Clone, Copy)]
#[repr(u64)]
enum Verb {
    Add = 2,
    Sin = 3,
    Throw = 4,
    NoWait = 5,
    Pair = 10,
    Echo = 15,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Writes dhat-heap.json on drop (view at https://nnethercote.github.io/dh_view/dh_view.html).
    #[cfg(dhat_heap)]
    let _profiler = dhat::Profiler::new_heap();

    let mut server = "127.0.0.1:10000".to_string();
    let mut timeout = Duration::from_millis(5000);
    let mut count = 1;
    let mut no_wait_send = false;
    let mut no_wait_recv = false;
    let mut rest = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().ok_or(USAGE)?,
            "--timeout" => timeout = Duration::from_millis(args.next().ok_or(USAGE)?.parse()?),
            "--count" => count = args.next().ok_or(USAGE)?.parse()?,
            "--no-wait-send" => no_wait_send = true,
            "--no-wait-recv" => no_wait_recv = true,
            "--no-wait" => (no_wait_send, no_wait_recv) = (true, true),
            _ => rest.push(arg),
        }
    }

    let (verb, rest) = rest.split_first().ok_or(USAGE)?;
    let verb = verb.parse::<Verb>()?;

    let client = Client::builder()
        .connect_timeout(timeout)
        .connect(server)
        .await?;

    for _ in 0..count {
        let id = verb.id();
        let mut opts = with_timeout(timeout);
        if no_wait_send {
            opts = opts.no_wait_send();
        }
        if no_wait_recv {
            opts = opts.no_wait_recv();
        }
        let result = match verb {
            Verb::Add => {
                let a = rest.first().ok_or(USAGE)?.parse::<i32>()?;
                let b = rest.get(1).ok_or(USAGE)?.parse::<i32>()?;
                client
                    .call_with::<_, i32>(id, &(a, b), opts)
                    .await
                    .map(|r| r.to_string())
            }
            Verb::Sin => {
                let x = rest.first().ok_or(USAGE)?.parse::<f64>()?;
                client
                    .call_with::<_, f64>(id, &x, opts)
                    .await
                    .map(|r| r.to_string())
            }
            Verb::Throw | Verb::NoWait => client
                .call_with::<_, ()>(id, &(), opts)
                .await
                .map(|_| "()".to_string()),
            Verb::Pair => client
                .call_with::<_, (i64, i32)>(id, (), opts)
                .await
                .map(|r| format!("{r:?}")),
            Verb::Echo => {
                let s = rest.first().ok_or(USAGE)?.clone();
                client.call_with::<_, String>(id, &s, opts).await
            }
        };

        match result {
            Ok(r) => println!("verb {verb} -> {r}"),
            Err(e) => println!("call failed: {e}"),
        }
    }

    Ok(())
}

impl Verb {
    fn id(self) -> u64 {
        self as u64
    }
}

impl std::str::FromStr for Verb {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "add" | "2" => Ok(Verb::Add),
            "sin" | "3" => Ok(Verb::Sin),
            "throw" | "4" => Ok(Verb::Throw),
            "no_wait" | "5" => Ok(Verb::NoWait),
            "pair" | "10" => Ok(Verb::Pair),
            "echo" | "15" => Ok(Verb::Echo),
            _ => Err(format!("unknown verb {s:?}\n{USAGE}")),
        }
    }
}

impl std::fmt::Display for Verb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}
