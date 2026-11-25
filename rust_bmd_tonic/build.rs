use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = if let Ok(p) = std::env::var("BMD_PROTO_ROOT") {
        PathBuf::from(p)
    } else {
        let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
        manifest_dir
            .join("..")
            .join("..")
            .join("BinanceMarketDataContracts")
            .join("src")
            .join("binance_market_data_contracts")
            .join("proto")
    };

    let service_proto = proto_root.join("binance_market_data/gateway/v1/gateway_service.proto");
    println!("cargo:rerun-if-changed={}", proto_root.display());

    tonic_build::configure()
        .build_server(false)
        .compile_protos(&[service_proto], &[proto_root])?;
    Ok(())
}
