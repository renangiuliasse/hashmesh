pub struct NodeAckResponse {
    server_config: NodeConfig,
}

pub enum ArchitectureSelection {
    Shout,
    Neighbour,
}
pub struct NodeSettings {
    architecture: ArchitectureSelection,
}

pub enum NodeMessage {
    NodeAckResponse,
    Ping,
}

pub enum NodeConfig {}
