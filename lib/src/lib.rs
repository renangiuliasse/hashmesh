/*!
 * Low-level library that handles (at least should) handle many common
 * abstractions and features for both the Server (Node) and the Client.
 *
 * ## Some things aren't really to pay attention!
 * Classes, structs, and enums that end or start with "Archive", "Mocked", "Archived" or
 * "Resolved" are some examples, these classes are **generated** for
 * serialization and deserialization of abstract objects for sending
 * and receiving objects in TCP connections or for storing and encrypting
 * objects and information.
 */

use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;

use crate::client::ClientMessage;

pub mod client;
pub mod node;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ArchitectureType {
    ClientPossibleArchitecture,
    NodePossibleArchitecture,
}
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ClientPossibleArchitecture {
    P2P,
    WorstFlood,
    Shout,
    Neighbour,
}
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum NodePossibleArchitecture {
    Shout,
    Neighbour,
}

pub const BUFFER_DEFAULT_SIZE: usize = 4096;
pub const MAC_SIZE: usize = 64;
pub const USER_ID_SIZE: usize = 32;

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
