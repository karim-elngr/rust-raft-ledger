fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-changed=proto/peer.proto");
    tonic_prost_build::compile_protos("proto/peer.proto")?;
    Ok(())
}
