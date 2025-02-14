use crate::chord::types::ChordNode;
use crate::chord::types::{NodeId, ThreadConfig, KEY_SIZE};
use crate::error::ChordError;
use crate::network::grpc::client::ChordGrpcClient;
use crate::network::messages::chord::{
    GetPredecessorRequest, GetSuccessorListRequest, NodeInfo, NotifyRequest,
};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const STABILIZE_INTERVAL: Duration = Duration::from_secs(30);
const PREDECESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(15);
const FINGER_FIX_INTERVAL: Duration = Duration::from_secs(45);
const SUCCESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(20);
const MAX_HEARTBEAT_RETRIES: u32 = 3;
const SUCCESSOR_LIST_SIZE: usize = 3;
const MAX_SUCCESSOR_LIST_SIZE: usize = 3;
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

async fn connect_with_retry(addr: &str) -> Option<ChordGrpcClient> {
    for i in 0..MAX_RETRIES {
        match ChordGrpcClient::new(addr.to_string()).await {
            Ok(client) => return Some(client),
            Err(e) => {
                if i < MAX_RETRIES - 1 {
                    warn!(
                        "Failed to connect to {}, attempt {}/{}: {}",
                        addr,
                        i + 1,
                        MAX_RETRIES,
                        e
                    );
                    sleep(RETRY_DELAY).await;
                } else {
                    error!(
                        "Failed to connect to {} after {} attempts: {}",
                        addr, MAX_RETRIES, e
                    );
                }
            }
        }
    }
    None
}

pub async fn run_stabilize_worker(config: ThreadConfig) {
    info!("Starting stabilize worker...");

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        if let Err(e) = stabilize(&config).await {
            warn!("Stabilization failed: {}", e);
            continue;
        }
    }
}

async fn stabilize(config: &ThreadConfig) -> Result<(), ChordError> {
    // Get current successor
    let successor = {
        let successor_list = config.successor_list.lock().await;
        successor_list.first().cloned()
    };

    let successor = match successor {
        Some(s) => s,
        None => {
            debug!("No successor yet, using self as successor");
            config.local_node_id
        }
    };

    // Skip stabilization if we're our own successor
    if successor == config.local_node_id {
        return Ok(());
    }

    // Get successor's address
    let successor_addr = config.get_node_addr(&successor).await.ok_or_else(|| {
        ChordError::NodeNotFound(format!("Address not found for successor {}", successor))
    })?;

    // Create client for successor
    let mut successor_client = ChordGrpcClient::new(successor_addr.clone())
        .await
        .map_err(|e| {
            ChordError::StabilizationFailed(format!("Failed to connect to successor: {}", e))
        })?;

    // Get successor's predecessor
    let successor_pred = match successor_client.get_predecessor().await {
        Ok(resp) => match resp.predecessor {
            Some(pred_info) => {
                // Store the predecessor's address
                let pred_id = NodeId::from_bytes(&pred_info.node_id);
                let mut addresses = config.node_addresses.lock().await;
                addresses.insert(pred_id, pred_info.address);
                Some(pred_id)
            }
            None => None,
        },
        Err(e) => {
            return Err(ChordError::StabilizationFailed(format!(
                "Failed to get predecessor from successor: {}",
                e
            )))
        }
    };

    // Check if we should update our successor
    if let Some(x) = successor_pred {
        if x != config.local_node_id && x.is_between(&config.local_node_id, &successor) {
            // Update successor
            {
                let mut finger_table = config.finger_table.lock().await;
                finger_table.update_finger(0, x);
            }

            info!(
                "Updated successor from {} to {} during stabilization",
                successor, x
            );

            // Update successor list
            update_successor_list(config, x).await?;

            debug!("Successfully stabilized with immediate successor {}", x);
            return Ok(());
        }
    }

    // Notify successor about our existence
    let notify_request = NotifyRequest {
        predecessor: Some(NodeInfo {
            node_id: config.local_node_id.to_bytes().to_vec(),
            address: config.local_addr.clone(),
        }),
    };

    successor_client.notify(notify_request).await.map_err(|e| {
        ChordError::StabilizationFailed(format!("Failed to notify successor: {}", e))
    })?;

    // Update successor list with current successor
    update_successor_list(config, successor).await?;

    // Update finger table after successful stabilization
    update_finger_table_after_stabilize(config).await?;

    debug!(
        "Successfully stabilized with immediate successor {}",
        successor
    );
    Ok(())
}

async fn update_successor_list(config: &ThreadConfig, successor: NodeId) -> Result<(), ChordError> {
    let mut successor_list = config.successor_list.lock().await;

    // Clear and add new successor
    successor_list.clear();
    successor_list.push(successor);

    // Try to get successor's successor list
    if let Some(addr) = config.get_node_addr(&successor).await {
        if let Ok(mut client) = ChordGrpcClient::new(addr).await {
            match client.get_successor_list().await {
                Ok(list) => {
                    // Add successors from the received list
                    for node_info in list {
                        let node = NodeId::from_bytes(&node_info.node_id);
                        if !successor_list.contains(&node)
                            && successor_list.len() < MAX_SUCCESSOR_LIST_SIZE
                        {
                            successor_list.push(node);

                            // Store the address
                            let mut addresses = config.node_addresses.lock().await;
                            addresses.insert(node, node_info.address);
                        }
                    }
                    debug!("Updated successor list: {:?}", successor_list);
                }
                Err(e) => warn!("Failed to get successor list: {}", e),
            }
        }
    }

    Ok(())
}

async fn update_finger_table_after_stabilize(config: &ThreadConfig) -> Result<(), ChordError> {
    let successor = {
        let successor_list = config.successor_list.lock().await;
        successor_list.first().cloned()
    };

    if let Some(successor) = successor {
        let mut finger_table = config.finger_table.lock().await;

        // Update first finger (immediate successor)
        finger_table.update_finger(0, successor);

        // Update other fingers if they should point to the successor
        for i in 1..KEY_SIZE {
            let finger_id = config.local_node_id.get_finger_id(i);
            if finger_id.is_between(&config.local_node_id, &successor) {
                finger_table.update_finger(i, successor);
            }
        }
    }

    Ok(())
}

async fn maintain_backup_successors(config: &ThreadConfig) {
    let successor = {
        let successor_list = config.successor_list.lock().await;
        successor_list.first().cloned()
    };

    if let Some(successor) = successor {
        if let Some(addr) = config.get_node_addr(&successor).await {
            if let Ok(mut client) = ChordGrpcClient::new(addr).await {
                match client.get_successor_list().await {
                    Ok(successors) => {
                        let mut new_list = vec![successor];

                        // Add successors from the response, maintaining max size of 3
                        for succ_info in successors {
                            let succ_id = NodeId::from_bytes(&succ_info.node_id);
                            if !new_list.contains(&succ_id) && new_list.len() < SUCCESSOR_LIST_SIZE
                            {
                                new_list.push(succ_id);

                                // Store the address
                                let mut addresses = config.node_addresses.lock().await;
                                addresses.insert(succ_id, succ_info.address);
                            }

                            // Break if we've reached the desired size
                            if new_list.len() >= SUCCESSOR_LIST_SIZE {
                                break;
                            }
                        }

                        let mut successor_list = config.successor_list.lock().await;
                        let new_list_clone = new_list.clone(); // Clone before moving
                        *successor_list = new_list;

                        debug!("Updated successor list: {:?}", new_list_clone);
                    }
                    Err(e) => warn!("Failed to get successor list: {}", e),
                }
            }
        }
    }
}

async fn handle_successor_failure(config: &ThreadConfig, failed_successor: NodeId) {
    // Remove failed successor from addresses
    {
        let mut addresses = config.node_addresses.lock().await;
        addresses.remove(&failed_successor);
    }

    // Update successor list
    {
        let mut successor_list = config.successor_list.lock().await;
        successor_list.retain(|&x| x != failed_successor);

        // If list is empty after removal, try emergency recovery
        if successor_list.is_empty() {
            drop(successor_list); // Drop lock before recovery
            if let Err(e) = attempt_emergency_recovery(config).await {
                error!("Emergency recovery failed after successor failure: {}", e);
            }
        }
    }

    // Update finger table
    {
        let mut finger_table = config.finger_table.lock().await;
        for entry in &mut finger_table.entries {
            if entry.node == Some(failed_successor) {
                entry.node = None;
            }
        }
    }
}

async fn attempt_emergency_recovery(config: &ThreadConfig) -> Result<(), ChordError> {
    // Try to find any live node from finger table
    let potential_nodes = {
        let finger_table = config.finger_table.lock().await;
        finger_table
            .entries
            .iter()
            .filter_map(|entry| entry.node)
            .collect::<Vec<_>>()
    };

    for node in potential_nodes {
        if let Some(addr) = config.get_node_addr(&node).await {
            if let Ok(mut client) = ChordGrpcClient::new(addr).await {
                if let Ok(_) = client.heartbeat().await {
                    // Found a live node, use it to rebuild our state
                    let mut successor_list = config.successor_list.lock().await;
                    successor_list.clear();
                    successor_list.push(node);

                    let mut finger_table = config.finger_table.lock().await;
                    finger_table.update_finger(0, node);

                    return Ok(());
                }
            }
        }
    }

    Err(ChordError::StabilizationFailed(
        "No live nodes found for recovery".into(),
    ))
}

pub async fn run_predecessor_checker(config: ThreadConfig) {
    info!("Starting predecessor health checker");
    let mut interval = tokio::time::interval(PREDECESSOR_CHECK_INTERVAL);

    loop {
        interval.tick().await;
        check_predecessor(&config).await;
    }
}

pub async fn run_finger_maintainer(config: ThreadConfig) {
    info!("Starting finger table maintenance worker");

    let mut next_finger = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Only update one finger at a time, rotating through all fingers
        if let Err(e) = fix_finger(&config, next_finger).await {
            warn!("Failed to update finger {}: {}", next_finger, e);
        }

        // Move to next finger
        next_finger = (next_finger + 1) % KEY_SIZE;
    }
}

async fn fix_finger(config: &ThreadConfig, index: usize) -> Result<(), ChordError> {
    let node_id = config.local_node_id;
    let finger_start = node_id.get_finger_id(index);

    // First check if finger start is between us and our immediate successor
    let successor = {
        let successor_list = config.successor_list.lock().await;
        successor_list.first().cloned()
    };

    if let Some(succ) = successor {
        if finger_start.is_between(&node_id, &succ) {
            let mut finger_table = config.finger_table.lock().await;
            finger_table.update_finger(index, succ);
            return Ok(());
        }
    }

    // Find successor for this finger
    let successor = find_successor_for_finger(config, &finger_start).await?;

    // Update finger table
    let mut finger_table = config.finger_table.lock().await;
    finger_table.update_finger(index, successor);

    Ok(())
}

async fn find_successor_for_finger(
    config: &ThreadConfig,
    id: &NodeId,
) -> Result<NodeId, ChordError> {
    // First check if id is between us and our immediate successor
    let successor = {
        let finger_table = config.finger_table.lock().await;
        finger_table.get_successor()
    };

    if let Some(succ) = successor {
        if id.is_between(&config.local_node_id, &succ) {
            return Ok(succ);
        }
    }

    // Otherwise find closest preceding node and ask them
    let closest = {
        let finger_table = config.finger_table.lock().await;
        finger_table.find_closest_preceding_node(id)
    };

    if let Some(node) = closest {
        if node == config.local_node_id {
            // If we're the closest, return our successor
            let finger_table = config.finger_table.lock().await;
            return finger_table
                .get_successor()
                .ok_or_else(|| ChordError::FingerUpdateFailed("No successor found".to_string()));
        }

        // Get the node's address and create client
        let addr = config.get_node_addr(&node).await.ok_or_else(|| {
            ChordError::NodeNotFound(format!("Address not found for node {}", node))
        })?;

        let mut client = ChordGrpcClient::new(addr).await.map_err(|e| {
            ChordError::FingerUpdateFailed(format!("Failed to connect to node: {}", e))
        })?;

        // Ask that node to find the successor
        match client.find_successor(id.to_bytes().to_vec()).await {
            Ok(node_info) => Ok(NodeId::from_bytes(&node_info.node_id)),
            Err(e) => Err(ChordError::FingerUpdateFailed(format!(
                "Failed to find successor: {}",
                e
            ))),
        }
    } else {
        // If no closest node found, return our successor
        let finger_table = config.finger_table.lock().await;
        finger_table
            .get_successor()
            .ok_or_else(|| ChordError::FingerUpdateFailed("No successor found".to_string()))
    }
}

pub async fn run_successor_maintainer(config: ThreadConfig) {
    info!("Starting successor list maintainer");
    let mut interval = tokio::time::interval(SUCCESSOR_CHECK_INTERVAL);

    loop {
        interval.tick().await;
        maintain_backup_successors(&config).await;
    }
}

async fn check_predecessor(config: &ThreadConfig) {
    let pred_id = {
        let pred = config.predecessor.lock().await;
        match *pred {
            Some(id) => id,
            None => return,
        }
    };

    let mut is_alive = false;
    for _ in 0..3 {
        if let Some(pred_addr) = config.get_node_addr(&pred_id).await {
            match ChordGrpcClient::new(pred_addr).await {
                Ok(_) => {
                    is_alive = true;
                    break;
                }
                Err(_) => continue,
            }
        }
    }

    // If all retries failed, clear predecessor
    if !is_alive {
        warn!(
            "Predecessor {} failed all heartbeat attempts, marking as failed",
            pred_id
        );
        let mut predecessor = config.predecessor.lock().await;
        *predecessor = None;
    }
}
