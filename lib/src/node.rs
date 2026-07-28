pub struct NodeAckResponse { server_config: NodeConfig }

pub enum NodeMessage {
    NodeAckResponse,
    Ping
}

pub enum NodeConfig {  }