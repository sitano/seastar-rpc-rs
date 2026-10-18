//! Command-line handling shared by the examples.

use seastar_rpc::rpc::Compression;

/// The command line without the program name and `--compress ALGOS`, and
/// the algorithms to offer, most preferred first. `ALGOS` is a comma list of
/// `lz4` and `lz4_fragmented`; without the flag nothing is offered.
pub fn args() -> Result<(Vec<String>, Vec<Compression>), String> {
    let mut rest = Vec::new();
    let mut compression = Vec::new();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        if arg != "--compress" {
            rest.push(arg);
            continue;
        }

        let names = args
            .next()
            .ok_or("--compress needs lz4 or lz4_fragmented")?;

        for name in names.split(',') {
            let algo = Compression::from_name(name.to_ascii_uppercase().as_bytes())
                .ok_or(format!("unknown compression {name:?}"))?;
            compression.push(algo);
        }
    }

    Ok((rest, compression))
}
