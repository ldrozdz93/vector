//! Shared Azure client infrastructure used by both sources and sinks.

/// Client construction: authentication, credentials, and HTTP transport assembly.
pub mod client;
/// Azure Storage connection-string parsing and endpoint resolution.
pub mod connection_string;
/// SharedKey request signing for the new Azure SDK pipeline.
pub mod shared_key_policy;
