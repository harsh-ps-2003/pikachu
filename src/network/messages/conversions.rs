use crate::chord::types::{NodeId, Key, Value};
use crate::network::messages::chord::{NodeInfo, KeyValue};

impl From<NodeId> for NodeInfo {
    fn from(id: NodeId) -> Self {
        NodeInfo {
            node_id: id.to_bytes().to_vec(),
            address: "".to_string(), // Add address handling
        }
    }
}

impl From<NodeInfo> for NodeId {
    fn from(info: NodeInfo) -> Self {
        NodeId::from_bytes(&info.node_id)
    }
}

impl From<(Key, Value)> for KeyValue {
    fn from((key, value): (Key, Value)) -> Self {
        KeyValue {
            key: key.0,
            value: value.0,
        }
    }
}

impl From<KeyValue> for (Key, Value) {
    fn from(kv: KeyValue) -> Self {
        (Key(kv.key), Value(kv.value))
    }
} 