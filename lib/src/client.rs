use std::io::Error;

use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor, to_bytes,
    util::AlignedVec,
};

use tokio::net::UdpSocket;

use crate::{Ping, SoftwareVersion};

pub mod p2p;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientAck {
    version: SoftwareVersion,
    own_uid: String,
    target_type: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Fingerprint {
    pub key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientP2PAck {
    pub version: SoftwareVersion,
    pub fingerprint: Fingerprint,
}

impl ClientP2PAck {
    pub fn new(fingerprint: Fingerprint) -> Self {
        ClientP2PAck {
            version: SoftwareVersion::project_version(),
            fingerprint,
        }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ClientMessage {
    ClientAck,
    Ping(Ping),
    ClientP2PAck(ClientP2PAck),
}

impl ClientMessage {
    pub fn new_ping() -> Self {
        ClientMessage::Ping(Ping::new("peer"))
    }

    pub fn new_p2p_ack(fingerprint: Fingerprint) -> Self {
        ClientMessage::ClientP2PAck(ClientP2PAck::new(fingerprint))
    }

    pub fn serialize(msg: &ClientMessage) -> Result<AlignedVec, rancor::Error> {
        to_bytes::<rancor::Error>(msg)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<ClientMessage, Error> {
        let archived: &<ClientMessage as Archive>::Archived =
            access::<Archived<ClientMessage>, rancor::Error>(bytes).unwrap();
        deserialize::<ClientMessage, Error>(archived)
    }
}

pub async fn ping(ip: String) -> Result<usize, std::io::Error> {
    let ping_message = ClientMessage::new_ping();

    let buffer: rkyv::util::AlignedVec = ClientMessage::serialize(&ping_message).unwrap();

    let udp_sock = UdpSocket::bind(ip).await.unwrap();
    udp_sock.send(&buffer).await
}
