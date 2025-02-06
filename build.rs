fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/network/messages")
        .compile_protos(
            &["src/network/messages/chord.proto"],
            &["src/network/messages/"],
        )?;
    Ok(())
}
