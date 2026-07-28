use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor::Error, to_bytes,
    util::AlignedVec,
};

use crate::client::ClientMessage;

pub mod client;
pub mod node;

#[derive(Debug, Serialize, Deserialize, PartialEq, Archive)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct SoftwareVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SoftwareVersion {
    pub fn project_version() -> Self {
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
    fn new(version: SoftwareVersion, peer_type: &str) -> Self {
        Ping {
            version,
            peer: peer_type.to_string(),
        }
    }
}

pub fn serialize_message(msg: &ClientMessage) -> Result<AlignedVec, Error> {
    to_bytes::<Error>(msg)
}

pub fn deserialize_message(bytes: &[u8]) -> Result<ClientMessage, Error> {
    let archived = access::<Archived<ClientMessage>, Error>(bytes).unwrap();
    deserialize::<ClientMessage, Error>(archived)
}
