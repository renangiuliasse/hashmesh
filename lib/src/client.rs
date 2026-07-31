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

use std::io::{
    Error,
    ErrorKind::{ConnectionRefused, InvalidData, Other},
};

use rkyv::{
    Archive, ArchiveUnsized, Archived, Deserialize, DeserializeUnsized, Serialize, access,
    deserialize, rancor, to_bytes, util::AlignedVec,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use uuid::Uuid;

use crate::{
    BUFFER_DEFAULT_SIZE, ClientPossibleArchitecture, Message, Ping, SoftwareVersion, USER_ID_SIZE,
    client::p2p::{
        ClientP2PAck, ClientP2PExchangePayload, EncryptedMessage, Fingerprint, MAC, UserID,
    },
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
pub struct ClientDefaultHandshake {
    version: SoftwareVersion,
    own_id: UserID,
    target_type: ClientPossibleArchitecture,
}
impl ClientDefaultHandshake {
    pub fn new(user: &User, target_type: ClientPossibleArchitecture) -> Self {
        Self {
            version: SoftwareVersion::project_version(),
            own_id: user.id,
            target_type,
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
    ClientP2PExchangePayload(Box<ClientP2PExchangePayload>),

    ClientDefaultHandshake(ClientDefaultHandshake),
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
        let deserialize: Result<ClientMessage, rancor::Error> =
            deserialize::<ClientMessage, rancor::Error>(archived);
        if deserialize.is_err() {
            return Err(Error::new(
                InvalidData,
                "Could not deserialize ClientMessage".to_string(),
            ));
        }

        Ok(deserialize.unwrap())
    }
}

/// Pings another device. It can either be a Client/Peer or a Node (Server). It **does not establish** a connection,
/// it serializes, and fires a single Ping UDP packet, and then either:
/// - returns the alive [UdpSocket]
/// - returns the [Error] object
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

/// Using the TCP Stream provided, initiates the default handshake protocol and returns:
/// - On sucess: a tuple of the current (used) mutable reference for [TcpStream] and the [Message] object received.
/// - On error: [Error]
pub async fn send_default_hanshake<'a>(
    stream: &'a mut TcpStream,
    user: &User,
    target_type: ClientPossibleArchitecture,
) -> Result<(&'a mut TcpStream, Message), Error> {
    let handshake =
        ClientMessage::ClientDefaultHandshake(ClientDefaultHandshake::new(user, target_type));
    let serialization = ClientMessage::serialize(&handshake);
    if serialization.is_err() {
        return Err(Error::new(
            InvalidData,
            "Could not serialize default handshake",
        ));
    }

    let payload = serialization.unwrap();
    let write_result = stream.write_all(&payload).await;
    let _ = stream.flush().await;
    if write_result.is_err() {
        return Err(Error::new(
            ConnectionRefused,
            "Could not send handshake payload",
        ));
    }

    let mut buf = [0u8; BUFFER_DEFAULT_SIZE];
    let recv_reading = stream.read(&mut buf).await;
    if recv_reading.is_err() {
        return Err(Error::new(Other, "Could not read received payload"));
    }

    let recv = &buf[..recv_reading.unwrap()];
    let message_res = Message::deserialize(recv);
    if message_res.is_err() {
        return Err(Error::new(
            InvalidData,
            "Could not deserialize received Message",
        ));
    }

    let message: Message = message_res.unwrap();

    Ok((stream, message))
}

/// Using the TCP Stream provided, resolves the incoming default handshake protocol and returns:
/// - On sucess: a tuple of the current (used) mutable reference for the [TcpStream] and the [Message] object received.
/// - On error: [Error]
pub async fn treat_incoming_default_handshake<'a>(
    stream: &'a mut TcpStream,
    incoming_handshake: ClientDefaultHandshake,
    user: &User,
    target_type: ClientPossibleArchitecture,
) -> Result<&'a mut TcpStream, Error> {
    let ver = SoftwareVersion::project_version();
    if ver != incoming_handshake.version {
        return Err(Error::new(
            Other,
            "Incoming software version is different from Client",
        ));
    }
    if target_type != incoming_handshake.target_type {
        return Err(Error::new(
            Other,
            "Incoming architecture type is different than the Client desired one",
        ));
    }

    let sending_handshake =
        ClientMessage::ClientDefaultHandshake(ClientDefaultHandshake::new(user, target_type));
    let payload_result = ClientMessage::serialize(&sending_handshake);
    if payload_result.is_err() {
        return Err(Error::new(
            InvalidData,
            "Could not serialize sending handshake payload",
        ));
    }

    let payload = payload_result.unwrap();
    let _ = stream.write_all(&payload).await;
    let _ = stream.flush().await;

    Ok(stream)
}

pub async fn listen_for_clientmessage(addr: String) -> Result<(TcpStream, ClientMessage), Error> {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Could not bind listener to address");
    match listener.accept().await {
        Err(e) => Err(e),
        Ok((mut stream, _socket)) => {
            let mut buf = [0u8; BUFFER_DEFAULT_SIZE];
            let read_bytes = stream.read(&mut buf).await?;

            let data = &buf[..read_bytes];
            let message = ClientMessage::deserialize(data)?;

            Ok((stream, message))
        }
    }
}
