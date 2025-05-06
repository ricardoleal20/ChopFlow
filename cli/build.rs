fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../broker/proto/chopflow.proto");

    tonic_build::compile_protos("../broker/proto/chopflow.proto")?;

    Ok(())
}
