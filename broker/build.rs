fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/chopflow.proto");
    tonic_build::compile_protos("proto/chopflow.proto")?;
    Ok(())
}
