fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-changed=proto/ledger/v1/ledger.proto");
    tonic_prost_build::configure().compile_protos(&["proto/ledger/v1/ledger.proto"], &["proto"])?;
    Ok(())
}
