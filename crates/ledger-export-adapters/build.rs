fn main() -> std::io::Result<()> {
    const EVENTS: &str = "proto/ledger/export/v1/events.proto";
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-changed={EVENTS}");
    println!("cargo:rerun-if-changed=../ledger-api/proto/ledger/v1/ledger.proto");
    prost_build::Config::new()
        .extern_path(".ledger.v1", "::ledger_api::v1")
        .compile_protos(&[EVENTS], &["proto", "../ledger-api/proto"])
}
