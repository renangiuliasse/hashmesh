use std::
    io::{Error, ErrorKind}
;

use rkyv::{
    Archive, Archived, Deserialize, Serialize, access, deserialize, rancor, to_bytes,
    util::AlignedVec,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, UdpSocket},
};

use crate::{Ping, SoftwareVersion};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientAck {
    version: SoftwareVersion,
    own_uid: String,
    target_type: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Fingerprint {
    key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ClientP2PAck {
    version: SoftwareVersion,
    fingerprint: Fingerprint,
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

pub async fn p2p_initiate_handshake(ip: String, fingerprint: Fingerprint) -> Result<(TcpStream, ClientP2PAck), Error> {
    let stream_result = TcpStream::connect(ip).await;
    if let Err(err) = stream_result {
        return Err(err);
    };

    let mut stream = stream_result.unwrap();

    let p2p_ack_message = ClientMessage::new_p2p_ack(fingerprint);
    let p2p_ack_bytes = ClientMessage::serialize(&p2p_ack_message).unwrap();

    let ack = stream.write(&p2p_ack_bytes).await;
    if let Err(err) = ack {
        return Err(err);
    }

    let mut buf: AlignedVec = AlignedVec::with_capacity(size_of::<ClientP2PAck>());
    let ack_response = stream.read(&mut buf).await;
    if let Err(err) = ack_response {
        return Err(err);
    }

    let response_result = ClientMessage::deserialize(&buf);
    if let Err(err) = response_result {
        return Err(err);
    }

    let response_message = response_result.unwrap();
    match response_message {
        ClientMessage::ClientAck => {
            return Err(Error::new(
                ErrorKind::Other,
                "Received Client default ACK as P2P response. Not pairing",
            ));
        }
        ClientMessage::Ping(_ping) => {
            return Err(Error::new(
                ErrorKind::Other,
                "Received Ping as ACK response. Not pairing",
            ));
        }
        ClientMessage::ClientP2PAck(ack) => {
            if ack.version != ack.version {
                let v = SoftwareVersion::project_version();
                return Err(
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "Peer's software version is mismatched. Running {}.{}.{}, peer's running {}.{}.{}", 
                            v.major, v.minor, v.patch, ack.version.major, ack.version.minor, ack.version.patch
                        ).to_string()
                    )
                );
            }

            return Ok((stream, ack))
        }
    }
}

pub async fn p2p_waitfor_handshake(local_addr: String, fingerprint: Fingerprint) -> Result<(TcpListener, ClientP2PAck), std::io::Error> {
    let stream_result = TcpListener::bind(local_addr).await;
    if let Err(err) = stream_result {
        return Err(err)
    }

    let stream = stream_result.unwrap();

    let mut buf: AlignedVec = AlignedVec::with_capacity(size_of::<ClientP2PAck>());

    match stream.accept().await {
        Err(err) => { return Err(err) }
        Ok((mut curr_stream, _socket)) => {
            let read_result = curr_stream.read(&mut buf).await;
            if let Err(err) = read_result {
                return Err(err)
            }

            let ack_data = ClientMessage::deserialize(&buf).unwrap();
            if let ClientMessage::ClientP2PAck(data) = ack_data {
                if data.version != data.version {
                    let v = SoftwareVersion::project_version();
                    return Err(
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "Peer's software version is mismatched. Running {}.{}.{}, peer's running {}.{}.{}", 
                                v.major, v.minor, v.patch, data.version.major, data.version.minor, data.version.patch
                            ).to_string()
                        )
                    );
                }
                return Ok((stream, data))
            } else {
                return Err(Error::new(ErrorKind::Other, format!("ACK Received is not supported or is broken")))
            }
        }
    }
}