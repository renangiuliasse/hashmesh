use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;

use crate::client::ClientMessage;

pub mod client;
pub mod node;

pub const BUFFER_DEFAULT_SIZE: usize = 4096;

#[derive(Debug, Serialize, Deserialize, PartialEq, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct SoftwareVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SoftwareVersion {
    pub fn project_version() -> SoftwareVersion {
        let version_str = env!("CARGO_PKG_VERSION");
        let parts: Vec<&str> = version_str.split('.').collect();
        let major = parts[0].parse().expect("Failed to parse major version");
        let minor = parts[1].parse().expect("Failed to parse minor version");
        let patch = parts[2].parse().expect("Failed to parse patch version");

        SoftwareVersion {
            major,
            minor,
            patch,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Ping {
    pub version: SoftwareVersion,
    pub peer: String,
}

impl Ping {
    fn new(peer_type: &str) -> Self {
        Ping {
            version: SoftwareVersion::project_version(),
            peer: peer_type.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum Message {
    ClientMessage(ClientMessage),
    NodeMessage,
}
