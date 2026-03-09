fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../../proto/relos.proto"], &["../../proto"])?;
    Ok(())
}
