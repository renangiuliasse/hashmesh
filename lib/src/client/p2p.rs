use std::io::{Error, ErrorKind};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    BUFFER_DEFAULT_SIZE, SoftwareVersion,
    client::{ClientMessage, ClientP2PAck, Fingerprint},
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

    let p2p_ack_message = ClientMessage::new_p2p_ack(fingerprint);
    let p2p_ack_bytes =
        ClientMessage::serialize(&p2p_ack_message).expect("Could not serialize P2P ACK");

    let ack = stream.write(&p2p_ack_bytes).await;
    if let Err(err) = ack {
        return Err(err);
    }

    let mut buf = [0; BUFFER_DEFAULT_SIZE];

    let ack_response = stream.read(&mut buf).await;
    if let Err(err) = ack_response {
        return Err(err);
    }

    let buf_copy = &buf[..ack_response.unwrap()];
    let response_result = ClientMessage::deserialize(buf_copy);
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

            return Ok((stream, ack));
        }
    }
}

pub async fn p2p_waitfor_handshake(
    local_addr: String,
    fingerprint: Fingerprint,
) -> Result<(TcpListener, ClientP2PAck), std::io::Error> {
    let stream_result = TcpListener::bind(local_addr).await;
    if let Err(err) = stream_result {
        return Err(err);
    }

    let stream = stream_result.unwrap();

    match stream.accept().await {
        Err(err) => return Err(err),
        Ok((mut curr_stream, _socket)) => {
            let mut buf = [0; BUFFER_DEFAULT_SIZE];

            let read_result = curr_stream.read(&mut buf).await;
            if let Err(err) = read_result {
                return Err(err);
            }

            let buf_read = &buf[..read_result.unwrap()];
            let ack_result = ClientMessage::deserialize(buf_read);
            if let Err(err) = ack_result {
                return Err(err);
            }

            let ack_data = ack_result.unwrap();
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

                let ack_response = ClientMessage::new_p2p_ack(fingerprint);
                let ack_response_bytes = ClientMessage::serialize(&ack_response)
                    .expect("Could not serialize an ACK response.");

                let response_result = curr_stream.write(&ack_response_bytes).await;
                if let Err(err) = response_result {
                    return Err(err);
                }

                return Ok((stream, data));
            } else {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("ACK Received is not supported or is broken"),
                ));
            }
        }
    }
}
