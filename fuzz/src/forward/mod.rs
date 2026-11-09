//! Production Forward modules linked directly into the storage-free fuzz harness.
// These paths compile the production modules verbatim. Keep the harness free of
// alternate parsers so findings exercise the same code as the receiver binary.
#[path = "../../../src/forward/codec.rs"]
pub mod codec;

#[path = "../../../src/forward/decode.rs"]
pub mod decode;

#[path = "../../../src/forward/docker.rs"]
pub mod docker;

#[path = "../../../src/forward/normalize.rs"]
pub mod normalize;

#[path = "../../../src/forward/protocol.rs"]
pub mod protocol;
