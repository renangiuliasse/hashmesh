/*!
 * The module client implements various functionalities, communication
 * architectures and features of the client both as in it's abstract and
 * low-level nature. (encryption, identification, etc.)
 *
 * ## User
 * The user is any device that chooses to be hosted or to communicate, it holds and stores
 * information about neighbouring peer's in Nodeless architectures and shares minimal personal info
 * about itself. Relies heavely on identification out-of-band from other more trusted ways of communication.
 *
 * Information that could describe a user and other peers are:
 * - this software's version
 * - fingerprint
 * - own ID
 *
 * Remember that, the User ID is a field solely for confirmation or temporal identification, it can be read, modified
 * and shared however the user may like. It is not a trusted factor for identifying users or pinning past messages.
 *
 * ## Messaging/Communication architectures
 * Comunication architectures can be found in other modules, such as p2p. (messaging via direct communication peer-to-peer)
 * These architectures rely on shared information and sometimes the type of architecture peer's want to use and types supported by the
 * server in question will be shared in the Handshake process, in which important information is used to secure connection for both
 * parties, or with a server;
 * - User fingerprint
 * - Software version
 * - Server config
 * - MAC (HMAC) data
 * - Encrypted messages
 * - User own ID
 * - Message destination User ID
 *
 * Not all of this information is shared at every architecture, it is type-dependent.
 */

use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor, to_bytes,
    util::AlignedVec,
};

use tokio::net::UdpSocket;
use uuid::Uuid;

use crate::{ClientPossibleArchitecture, Ping, SoftwareVersion};

pub mod p2p;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct User {
    id: String,
}
impl User {
    /// Changes the referred User's ID. Limited to <= (less or equal to) 32 characters only.
    pub fn change_id(&self, id: String) -> Result<Self, &Self> {
        if id.len() > 32 {
            return Err(&self);
        }

        Ok(User { id })
    }

    /// Generates (and changes) a User UUID (random v4) for the referred User.
    pub fn random_id(mut self) -> User {
        let uuid = Uuid::new_v4().simple().to_string();

        self.id = uuid;
        self
    }

    pub fn new() -> User {
        User { id: Uuid::new_v4().simple().to_string() }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientAck {
    version: SoftwareVersion,
    own_uid: String,
    target_type: ClientPossibleArchitecture,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Fingerprint {
    pub key: String,
}

/// Client P2P ACK Handshake, used to initiate or to respond to a handshake P2P connection.
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

/// Used to transport and receive a structed payload (message) from another peer.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientP2PExchangePayload {
    encrypted_message: Vec<u8>,
    mac: Vec<u8>,
    peer_id: String,
}

impl ClientP2PExchangePayload {
    pub fn new(encrypted_message: Vec<u8>, mac: Vec<u8>, peer_id: String) -> Self {
        ClientP2PExchangePayload {
            encrypted_message,
            mac,
            peer_id,
        }
    }
}

/// A Client Message can be in many forms, could be a message payload or
/// just a handshake, all of them are located here
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ClientMessage {
    ClientAck,
    Ping(Ping),
    ClientP2PAck(ClientP2PAck),
    ClientP2PExchangePayload(ClientP2PExchangePayload),
}

impl ClientMessage {
    pub fn build_ping() -> Self {
        ClientMessage::Ping(Ping::new("peer"))
    }

    pub fn build_p2p_ack(fingerprint: Fingerprint) -> Self {
        ClientMessage::ClientP2PAck(ClientP2PAck::new(fingerprint))
    }

    pub fn build_p2p_payload(encrypted_message: Vec<u8>, mac: Vec<u8>, peer_id: String) -> Self {
        ClientMessage::ClientP2PExchangePayload(ClientP2PExchangePayload::new(
            encrypted_message,
            mac,
            peer_id,
        ))
    }

    pub fn serialize(msg: &ClientMessage) -> Result<AlignedVec, rancor::Error> {
        to_bytes::<rancor::Error>(msg)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<ClientMessage, rancor::Error> {
        let archived: &<ClientMessage as Archive>::Archived =
            access::<Archived<ClientMessage>, rancor::Error>(bytes).unwrap();
        deserialize::<ClientMessage, rancor::Error>(archived)
    }
}

/// Pings another device. It can either be a Client/Peer or a Node (Server). It **does not establish** a connection,
/// it serializes, and fires a single Ping UDP packet, and then either:
/// - returns the alive UDP socket
/// - returns the Error object
pub async fn ping(ip: String) -> Result<UdpSocket, std::io::Error> {
    let ping_message = ClientMessage::build_ping();

    let buffer: rkyv::util::AlignedVec = ClientMessage::serialize(&ping_message).unwrap();

    let udp_sock_result = UdpSocket::bind(ip).await;
    if let Err(err) = udp_sock_result {
        return Err(err);
    }

    let udp_sock = udp_sock_result.unwrap();
    if let Err(err) = udp_sock.send(&buffer).await {
        return Err(err);
    };

    Ok(udp_sock)
}
