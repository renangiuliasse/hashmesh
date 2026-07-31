use std::io::{
    Error,
    ErrorKind::{InvalidData, Other},
};

use rkyv::{Archive, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    BUFFER_DEFAULT_SIZE, Message, NodePossibleArchitecture, Ping, SoftwareVersion,
    client::{ClientDefaultHandshake, ClientMessage},
};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NodeAckResponse {
    server_config: NodeSettings,
}
impl NodeAckResponse {
    pub fn new(server_config: NodeSettings) -> Self {
        NodeAckResponse { server_config }
    }

    pub fn get_config(&self) -> NodeSettings {
        self.server_config
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NodeSettings {
    pub architecture: NodePossibleArchitecture,
}
impl NodeSettings {
    pub fn new(architecture: NodePossibleArchitecture) -> Self {
        NodeSettings { architecture }
    }
}

/// Messages that the Node may send. You should NOT serialize this, instead, build this as a [Message]
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum NodeMessage {
    NodeAckResponse(NodeAckResponse),
    Ping(Ping),
}
impl NodeMessage {
    pub fn build_ack(server_config: NodeSettings) -> NodeAckResponse {
        NodeAckResponse { server_config }
    }
}

/// Handles the receiving handshake from a client and answers following protocol
pub async fn handle_user_handshake(
    stream: &mut TcpStream,
    server_info: NodeAckResponse,
) -> Result<(&mut TcpStream, ClientDefaultHandshake), Error> {
    let mut buf = [0u8; BUFFER_DEFAULT_SIZE];
    let read_bytes = stream.read(&mut buf).await?;

    let read_data = &buf[..read_bytes];
    let clientmessage = ClientMessage::deserialize(read_data)?;
    if let ClientMessage::ClientDefaultHandshake(handshake) = clientmessage {
        if handshake.get_version() != SoftwareVersion::project_version() {
            return Err(Error::new(
                Other,
                "Software version of this Node and connecting peer is mismatched",
            ));
        }

        let serversettings = Message::serialize(&Message::NodeMessage(
            NodeMessage::NodeAckResponse(server_info),
        ))
        .expect("Could not serialize Node ACK handshake response");

        stream.write_all(&serversettings).await?;
        stream.flush().await?;

        Ok((stream, handshake))
    } else {
        Err(Error::new(
            InvalidData,
            "Node received invalid/unknown ACK format",
        ))
    }
}
