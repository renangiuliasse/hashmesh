/*!
 * This module represents and secures a Peer-to-Peer connection between
 * two parties. It is responsible for initiating the P2P Handshake and securing
 * a stable TCP connection with a peer with the same software version.
 */

use std::io::{
    Error,
    ErrorKind::Other,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    BUFFER_DEFAULT_SIZE, SoftwareVersion,
    client::{ClientMessage, ClientP2PAck, EncryptedMessage, Fingerprint, MAC, User},
};

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

    let ack = stream.write(&p2p_ack_bytes).await;
    ack?;

    let mut buf = [0; BUFFER_DEFAULT_SIZE];

    let ack_response = stream.read(&mut buf).await;
    if let Err(err) = ack_response {
        return Err(err);
    }

    let buf_copy = &buf[..ack_response.unwrap()];
    let response_result = ClientMessage::deserialize(buf_copy);
    if response_result.is_err() {
        return Err(Error::new(
            Other,
            "Could not deserialize P2P handshake response buffer.".to_string(),
        ));
    }

    let response_message = response_result.unwrap();
    match response_message {
        ClientMessage::ClientAck => {}
        ClientMessage::Ping(_) => {}
        ClientMessage::ClientP2PExchangePayload(_) => {}
        ClientMessage::ClientP2PAck(ack) => {
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
    }

    Err(Error::other(
        "Did not receive a P2P ACK packet. Not pairing",
    ))
}

pub async fn p2p_waitfor_handshake(
    local_addr: String,
    fingerprint: Fingerprint,
) -> Result<(TcpStream, ClientP2PAck), std::io::Error> {
    let stream_result = TcpListener::bind(local_addr).await;
    if let Err(err) = stream_result {
        return Err(err);
    }

    let stream = stream_result.unwrap();

    match stream.accept().await {
        Err(err) => Err(err),
        Ok((mut curr_stream, _socket)) => {
            let mut buf = [0u8; BUFFER_DEFAULT_SIZE];

            let read_result = curr_stream.read(&mut buf).await;
            if let Err(err) = read_result {
                return Err(err);
            }

            let bytes_read = read_result.unwrap();
            let buf_read = &buf[..bytes_read];
            let ack_result = ClientMessage::deserialize(buf_read);
            if ack_result.is_err() {
                return Err(Error::new(
                    Other,
                    "Could not deserialize received packet listening for incoming P2P handshake".to_string(),
                ));
            }

            let ack_data = ack_result.unwrap();
            if let ClientMessage::ClientP2PAck(data) = ack_data {
                let v = SoftwareVersion::project_version();
                if v != data.version {
                    return Err(
                        Error::other(
                            format!(
                                "Peer's software version is mismatched. Running {}.{}.{}, peer's running {}.{}.{}", 
                                v.major, v.minor, v.patch, data.version.major, data.version.minor, data.version.patch
                            ).to_string()
                        )
                    );
                }

                let ack_response = ClientMessage::build_p2p_ack(fingerprint);
                let ack_response_bytes = ClientMessage::serialize(&ack_response)
                    .expect("Could not serialize an ACK response.");

                let response_result = curr_stream.write(&ack_response_bytes).await;
                response_result?;

                Ok((curr_stream, data))
            } else {
                Err(Error::other(
                    "ACK Received is not supported or is broken".to_string(),
                ))
            }
        }
    }
}

pub async fn p2p_send_encrypted_message(
    mut stream: TcpStream,
    user: User,
    encrypted_message: EncryptedMessage,
    mac: MAC,
) -> Result<TcpStream, Error> {
    let payload = ClientMessage::build_p2p_payload(encrypted_message, mac, user.id);
    let serialization = ClientMessage::serialize(&payload);
    if let Err(_err) = serialization {
        return Err(Error::other(
            "Could not serialize P2P payload".to_string(),
        ));
    }

    let buf_aligned = serialization.unwrap();
    let buf = buf_aligned.as_slice();

    let res = stream.write_all(buf).await;
    match res {
        Err(err) => Err(err),
        Ok(()) => Ok(stream),
    }
}
