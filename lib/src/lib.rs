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
use std::{
    alloc::{Layout, alloc},
    fmt::Debug,
    u8,
};

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
pub const MESSAGE_MAX_SIZE: usize = 512;

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

/// Fixes byte array to length "size" and fills missing bytes
pub fn pad_bytes<const F: usize>(bytes: &[u8], size: usize) -> [u8; F] {
    let mut buffer = [0u8; F];
    let min_len = bytes.len().min(size);

    buffer[0..min_len].copy_from_slice(&bytes[0..min_len]);

    return buffer;
}

/// Shears byte array to length "size" and removes left bytes
pub fn shear_bytes<const F: usize>(bytes: &[u8]) -> Option<[u8; F]> {
    if bytes.len() < USER_ID_SIZE {
        return None;
    }

    Some(bytes[0..USER_ID_SIZE].try_into().unwrap())
}

/// Fixes byte array to length "size" by either removing or padding bytes
pub fn fix_byte_buffer<const F: usize>(bytes: &[u8], size: usize) -> [u8; F] {
    match shear_bytes::<F>(bytes) {
        None => {
            let new_bytes = pad_bytes::<F>(bytes, size);
            new_bytes
        }
        Some(new_bytes) => new_bytes,
    }
}
