fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/chopflow.proto");

    // Point tonic-build (and prost-build) at a vendored `protoc` binary so the
    // crate builds without a system-installed protobuf compiler. CI images ship
    // `protoc`, but end users running `cargo install chopflow` frequently don't —
    // this was the cause of the v0.1.2 install failure.
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .map_err(|e| format!("failed to locate vendored protoc: {e}"))?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::compile_protos("proto/chopflow.proto")?;
    Ok(())
}
