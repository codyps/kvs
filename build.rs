#[cfg(feature = "capnproto")]
fn main() {
    ::capnpc::CompilerCommand::new()
        .file("kvs.capnp")
        .run()
        .expect("compiling schema");
}

#[cfg(not(feature = "capnproto"))]
fn main() {}
