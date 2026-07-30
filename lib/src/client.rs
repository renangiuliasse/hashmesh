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

use std::io::{Error, ErrorKind::{InvalidData}, Read};

use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor, to_bytes,
    util::AlignedVec
};

use tokio::net::UdpSocket;
use uuid::Uuid;

use crate::{
    ClientPossibleArchitecture, MAC_SIZE, MESSAGE_MAX_SIZE, Ping, SoftwareVersion, USER_ID_SIZE,
};

pub mod p2p;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct User {
    id: UserID,
}
impl User {
    /// Appends null bytes for too small UserIDs
    fn pad_id(id: String) -> UserID {
        let id_bytes = id.as_bytes();

        let mut buffer = [0u8; USER_ID_SIZE];
        let min_len = id_bytes.len().min(USER_ID_SIZE);

        buffer[0..min_len].copy_from_slice(&id_bytes[0..min_len]);

        let new_id: UserID = buffer;
        new_id
    }

    /// Shears too big UserIDs
    fn shear_id(id: String) -> Option<UserID> {
        let bytes = id.as_bytes();
        if bytes.len() < USER_ID_SIZE {
            return None;
        }

        Some(bytes[0..USER_ID_SIZE].try_into().unwrap())
    }

    /// Changes the referred User's ID. Limited to <= (less or equal to) 32 characters only.
    pub fn change_id(mut self, id: String) -> Self {
        match Self::shear_id(id.clone()) {
            None => {
                let new_id = Self::pad_id(id);
                self.id = new_id;
            }
            Some(new_id) => self.id = new_id,
        }

        self
    }

    /// Generates (and changes) a random User ID (UUID v4) for the referred User.
    pub fn random_id(self) -> Self {
        let uuid = Uuid::new_v4().simple().to_string();

        Self::change_id(self, uuid)
    }

    /// Builds a new User with random UserID
    pub fn new() -> User {
        let user = User {
            id: [0; USER_ID_SIZE],
        };
        user.random_id()
    }
}

impl Default for User {
    fn default() -> Self {
        Self::new()
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

// be very careful with these values as they need to fit under the DEFAULT_BUFFER_SIZE const.
pub type EncryptedMessage = [u8; MESSAGE_MAX_SIZE];
pub type MAC = [u8; MAC_SIZE];
pub type UserID = [u8; USER_ID_SIZE];

/// Used to transport and receive a structed payload (message) from another peer.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientP2PExchangePayload {
    encrypted_message: EncryptedMessage,
    mac: MAC,
    peer_id: UserID,
}

impl ClientP2PExchangePayload {
    pub fn new(encrypted_message: EncryptedMessage, mac: MAC, peer_id: UserID) -> Self {
        ClientP2PExchangePayload {
            encrypted_message,
            mac,
            peer_id,
        }
    }

    /// Reads the current holding message and parses it from raw bytes into ASCII String.
    /// If message buffer is not ASCII or is corrupted/invalid, returns nothing.
    pub fn read_message(self) -> Option<String> {
        let mut raw_bytes: &[u8] = &self.encrypted_message;
        if raw_bytes.is_ascii() {
            let mut msg = String::new();
            if raw_bytes.read_to_string(&mut msg).is_ok() {
                return Some(msg);
            }
        }

        None
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
    ClientP2PExchangePayload(Box<ClientP2PExchangePayload>),
}

impl ClientMessage {
    pub fn build_ping() -> Self {
        ClientMessage::Ping(Ping::new("peer"))
    }

    pub fn build_p2p_ack(fingerprint: Fingerprint) -> Self {
        ClientMessage::ClientP2PAck(ClientP2PAck::new(fingerprint))
    }

    pub fn build_p2p_payload(
        encrypted_message: EncryptedMessage,
        mac: MAC,
        peer_id: UserID,
    ) -> Self {
        ClientMessage::ClientP2PExchangePayload(Box::new(ClientP2PExchangePayload::new(
            encrypted_message,
            mac,
            peer_id,
        )))
    }

    pub fn serialize(msg: &ClientMessage) -> Result<AlignedVec, rancor::Error> {
        to_bytes::<rancor::Error>(msg)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<ClientMessage, Error> {
        let access_result: Result<&ArchivedClientMessage, rancor::Error> =
            access::<Archived<ClientMessage>, rancor::Error>(bytes);
        if access_result.is_err() {
            return Err(Error::new(InvalidData, "Could not access ClientMessage"));
        }
        let archived: &<ClientMessage as Archive>::Archived = access_result.unwrap();
        let deserialize: Result<ClientMessage, rancor::Error> = deserialize::<ClientMessage, rancor::Error>(archived);
        if deserialize.is_err() {
            return Err(Error::new(InvalidData, "Could not deserialize ClientMessage".to_string()))
        }

        Ok(deserialize.unwrap())
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
    udp_sock.send(&buffer).await?;

    Ok(udp_sock)
}
