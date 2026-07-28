use rkyv::{Archive, Deserialize, Serialize};
use tokio::net::UdpSocket;

use crate::{Ping, SoftwareVersion, serialize_message};

pub struct ClientAck {
    version: SoftwareVersion,
    own_uid: String,
    target_type: String,
}
pub struct ClientP2PAck {
    version: SoftwareVersion,
    fingerprint: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ClientMessage {
    ClientAck,
    Ping(Ping),
    ClientP2PAck,
}

impl ClientMessage {
    pub fn new_ping() -> Self {
        ClientMessage::Ping(Ping::new(SoftwareVersion::project_version(), "peer"))
    }
}

pub async fn ping(ip: String) -> Result<usize, std::io::Error> {
    let ping_message = ClientMessage::new_ping();

    let buffer = serialize_message(&ping_message).unwrap();

    let udp_sock = UdpSocket::bind(ip).await.unwrap();
    udp_sock.send(&buffer).await
}
