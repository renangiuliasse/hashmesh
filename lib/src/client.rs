use std::io::Error;

use crypto::hmac::Hmac;
use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor, to_bytes,
    util::AlignedVec,
};

use tokio::net::UdpSocket;
use uuid::{Builder, Uuid, uuid};

use crate::{Ping, SoftwareVersion};

pub mod p2p;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct User {
    id: String
}
impl User {
    pub fn change_id(&self, id: String) -> Result<Self, &Self> {
        if id.len() > 32 { return Err(&self) }

        Ok(User { id })
    }
    pub fn generate_id(&self) -> Self {
        let uuid = Uuid::new_v4().simple().to_string();
        
        User { id: uuid }
    }
}

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

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientP2PExchangePayload {
    encrypted_message: Vec<u8>,
    mac: Vec<u8>,
    peer_id: String
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
