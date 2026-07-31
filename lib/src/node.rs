use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NodeAckResponse {
    server_config: NodeSettings,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ArchitectureSelection {
    Shout,
    Neighbour,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct NodeSettings {
    architecture: ArchitectureSelection,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum NodeMessage {
    NodeAckResponse,
    Ping,
}
