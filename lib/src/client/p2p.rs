/*!
 * This module represents and secures a Peer-to-Peer connection between
 * two parties. It is responsible for initiating the P2P Handshake and securing
 * a stable TCP connection with a peer with the same software version.
 */

use rkyv::{Archive, Deserialize, Serialize};
use std::io::{Error, ErrorKind::Other, Read};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    BUFFER_DEFAULT_SIZE, ClientPossibleArchitecture, MAC_SIZE, MESSAGE_MAX_SIZE, SoftwareVersion,
    USER_ID_SIZE,
    client::{ClientMessage, User},
};

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
            if Read::read_to_string(&mut raw_bytes, &mut msg).is_ok() {
                return Some(msg);
            }
        }

        None
    }
}

pub async fn p2p_initiate_handshake(
    ip: String,
    fingerprint: Fingerprint,
) -> Result<(TcpStream, ClientP2PAck), Error> {
    let stream_result = TcpStream::connect(ip).await;
    if let Err(err) = stream_result {
        return Err(err);
    };

    let mut stream = stream_result.unwrap();

    let p2p_ack_message = ClientMessage::build_p2p_ack(fingerprint);
    let p2p_ack_bytes =
        ClientMessage::serialize(&p2p_ack_message).expect("Could not serialize P2P ACK");

    stream.write_all(&p2p_ack_bytes).await?;
    stream.flush().await?;

    let mut buf = [0; BUFFER_DEFAULT_SIZE];

    let ack_response = stream.read(&mut buf).await?;

    let buf_copy = &buf[..ack_response];
    let response_result = ClientMessage::deserialize(buf_copy);
    if response_result.is_err() {
        return Err(Error::new(
            Other,
            "Could not deserialize P2P handshake response buffer.".to_string(),
        ));
    }

    let response_message = response_result.unwrap();
    if let ClientMessage::ClientP2PAck(ack) = response_message {
        let v = SoftwareVersion::project_version();
        if v != ack.version {
            return Err(
                Error::other(
                    format!(
                        "Peer's software version is mismatched. Running {}.{}.{}, peer's running {}.{}.{}", 
                        v.major, v.minor, v.patch, ack.version.major, ack.version.minor, ack.version.patch
                    ).to_string()
                )
            );
        }

        return Ok((stream, ack));
    }

    Err(Error::other(
        "Did not receive a P2P ACK packet. Not pairing",
    ))
}

/// Resolves the rest of the P2P Handshake, verifies both the received ACK software version used and responds the handshake
pub async fn treat_incoming_p2p_handshake(
    stream: &mut TcpStream,
    ack: ClientP2PAck,
    fingerprint: Fingerprint,
) -> Result<&mut TcpStream, std::io::Error> {
    let v = SoftwareVersion::project_version();
    if v != ack.version {
        return Err(
            Error::other(
                format!(
                    "Peer's software version is mismatched. Running {}.{}.{}, peer's running {}.{}.{}", 
                    v.major, v.minor, v.patch, ack.version.major, ack.version.minor, ack.version.patch
                ).to_string()
            )
        );
    }

    let ack_response = ClientMessage::build_p2p_ack(fingerprint);
    let ack_response_bytes = ClientMessage::serialize(&ack_response)
        .expect("Could not serialize an ACK response.");

    let _byte_amount = stream.write(&ack_response_bytes).await?;

    Ok(stream)
}

/// Sends a encrypted message and resolves all serialization and payload through the current [TcpStream]
pub async fn p2p_send_encrypted_message(
    stream: &mut TcpStream,
    user: User,
    encrypted_message: EncryptedMessage,
    mac: MAC,
) -> Result<&mut TcpStream, Error> {
    let payload = ClientMessage::build_p2p_payload(encrypted_message, mac, user.id);
    let serialization = ClientMessage::serialize(&payload);
    if let Err(_err) = serialization {
        return Err(Error::other("Could not serialize P2P payload".to_string()));
    }

    let buf_aligned = serialization.unwrap();
    let buf = buf_aligned.as_slice();

    let res = stream.write_all(buf).await;
    match res {
        Err(err) => Err(err),
        Ok(()) => Ok(stream),
    }
}
