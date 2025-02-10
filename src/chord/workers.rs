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
                    warn!("Failed to connect to {}, attempt {}/{}: {}", addr, i + 1, MAX_RETRIES, e);
                    sleep(RETRY_DELAY).await;
                } else {
                    error!("Failed to connect to {} after {} attempts: {}", addr, MAX_RETRIES, e);
                }
            }
        }
    }
    None
}

pub async fn run_stabilize_worker(config: ThreadConfig) {
    info!("Starting stabilize worker");
    let mut next_stabilize = tokio::time::Instant::now();

    loop {
        // Wait until next stabilize interval
        let now = tokio::time::Instant::now();
        if now < next_stabilize {
            sleep(next_stabilize - now).await;
            next_stabilize += STABILIZE_INTERVAL;
        }

        debug!("Running stabilize");

        // Get current successor
        let successor = {
            let successor_list = config.successor_list.lock().await;
            successor_list.first().cloned()
        };

        if let Some(successor) = successor {
            // Get successor's address
            let successor_addr = match config.get_node_addr(&successor).await {
                Some(addr) => addr,
                None => {
                    error!("No address found for successor {}", successor);
                    continue;
                }
            };

            // Connect to successor and get their predecessor
            if let Some(mut client) = connect_with_retry(&successor_addr).await {
                match client.get_predecessor().await {
                    Ok(pred_info) => {
                        if let Some(x) = pred_info.predecessor {
                            let x_id = NodeId::from_bytes(&x.node_id);

                            // Update successor if needed
                            let mut successor_list = config.successor_list.lock().await;
                            if x_id != successor {
                                successor_list.insert(0, x_id);
                                debug!("Updated successor to {}", x_id);
                            }
                        }
                    }
                    Err(e) => error!("Failed to get predecessor from successor: {}", e),
                }

                // Notify successor about us
                if let Err(e) = client.notify(config.local_node_id.clone()).await {
                    warn!("Failed to notify successor: {}", e);
                    // Remove failed successor and try next one
                    let mut successors = config.successor_list.lock().await;
                    if let Some(first) = successors.first() {
                        if first == &successor {
                            successors.remove(0);
                        }
                    }
                }
            }
        }
    }
}

/// Worker thread that periodically checks predecessor's health via heartbeat
pub async fn run_predecessor_checker(config: ThreadConfig) {
    info!("Starting predecessor health checker");
    let mut next_check = tokio::time::Instant::now();

    loop {
        // Wait until next check interval
        let now = tokio::time::Instant::now();
        if now < next_check {
            sleep(next_check - now).await;
            next_check += PREDECESSOR_CHECK_INTERVAL;
        }

        debug!("Checking predecessor health");

        // Get current predecessor
        let predecessor = {
            let guard = config.predecessor.lock().await;
            guard.clone()
        };

        // If we have a predecessor, check its health
        if let Some(pred_id) = predecessor {
            let mut retry_count = 0;
            let mut is_alive = false;

            // Try to get predecessor's address
            let pred_addr = match config.get_node_addr(&pred_id).await {
                Some(addr) => addr,
                None => {
                    warn!(
                        "No address found for predecessor {}, marking as failed",
                        pred_id
                    );
                    clear_predecessor(&config).await;
                    continue;
                }
            };

            // Attempt heartbeat with retries
            while retry_count < MAX_HEARTBEAT_RETRIES && !is_alive {
                match connect_with_retry(&pred_addr).await {
                    Some(mut client) => match client.heartbeat().await {
                        Ok(response) => {
                            if response.alive {
                                is_alive = true;
                                debug!("Predecessor {} is alive", pred_id);
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Heartbeat to predecessor failed (attempt {}): {}",
                                retry_count + 1,
                                e
                            );
                        }
                    },
                    None => {
                        warn!(
                            "Failed to connect to predecessor (attempt {}): {}",
                            retry_count + 1,
                            "No address found"
                        );
                    }
                }

                retry_count += 1;
                if retry_count < MAX_HEARTBEAT_RETRIES {
                    sleep(Duration::from_millis(500)).await;
                }
            }

            // If all retries failed, clear predecessor
            if !is_alive {
                warn!(
                    "Predecessor {} failed all heartbeat attempts, marking as failed",
                    pred_id
                );
                clear_predecessor(&config).await;

                // Trigger immediate stabilization to find new predecessor
                if let Err(e) = trigger_stabilization(&config).await {
                    error!(
                        "Failed to trigger stabilization after predecessor failure: {}",
                        e
                    );
                }
            }
        }
    }
}

/// Helper function to safely clear the predecessor
async fn clear_predecessor(config: &ThreadConfig) {
    let mut guard = config.predecessor.lock().await;
    *guard = None;
}

/// Helper function to trigger immediate stabilization
async fn trigger_stabilization(config: &ThreadConfig) -> Result<(), ChordError> {
    let local_addr = config.local_addr.clone();
    let mut client = ChordGrpcClient::new(local_addr)
        .await
        .map_err(|e| ChordError::StabilizationFailed(format!("Failed to create client: {}", e)))?;

    client.stabilize().await.map_err(|e| {
        ChordError::StabilizationFailed(format!("Failed to trigger stabilization: {}", e))
    })?;

    Ok(())
}

pub async fn run_finger_maintainer(config: ThreadConfig) {
    let finger_interval = Duration::from_secs(60);
    
    loop {
        for i in 0..256 {
            let target = config.local_node_id.add_power_of_two(i as u8);
            
            if let Some(successor) = config.successor_list.lock().await.first().cloned() {
                if let Some(addr) = config.get_node_addr(&successor).await {
                    if let Some(mut client) = connect_with_retry(&addr).await {
                        match client.find_successor(target.to_bytes().to_vec()).await {
                            Ok(node_info) => {
                                let mut finger_table = config.finger_table.lock().await;
                                finger_table.update_finger(i, NodeId::from_bytes(&node_info.node_id));
                            }
                            Err(e) => {
                                warn!("Failed to update finger table entry {}: {}", i, e);
                            }
                        }
                    }
                }
            }
        }
        sleep(finger_interval).await;
    }
}

/// Worker thread that maintains the successor list
pub async fn run_successor_maintainer(config: ThreadConfig) {
    info!("Starting successor list maintainer");
    let mut next_check = tokio::time::Instant::now();

    loop {
        // Wait until next check interval
        let now = tokio::time::Instant::now();
        if now < next_check {
            sleep(next_check - now).await;
            next_check += SUCCESSOR_CHECK_INTERVAL;
        }

        debug!("Updating successor list");

        // Get current successor list
        let current_successors = {
            let guard = config.successor_list.lock().await;
            guard.clone()
        };

        // If we have no successors, we can't update the list
        if current_successors.is_empty() {
            warn!("No successors in list, skipping update");
            continue;
        }

        // Get immediate successor's address
        let immediate_successor = current_successors[0];
        let successor_addr = match config.get_node_addr(&immediate_successor).await {
            Some(addr) => addr,
            None => {
                warn!(
                    "No address found for immediate successor {}",
                    immediate_successor
                );
                continue;
            }
        };

        // Get successor's successor list
        match connect_with_retry(&successor_addr).await {
            Some(mut client) => {
                match client.get_successor_list().await {
                    Ok(successor_list) => {
                        // Create new successor list starting with our immediate successor
                        let mut new_successors = vec![immediate_successor];

                        // Add successors from our successor's list until we reach SUCCESSOR_LIST_SIZE
                        for successor in successor_list {
                            let successor_id = NodeId::from_bytes(&successor.node_id);

                            // Don't add duplicates or ourselves
                            if !new_successors.contains(&successor_id)
                                && successor_id != config.local_node_id
                            {
                                new_successors.push(successor_id);

                                // Store the address for this successor
                                config.add_node_addr(successor_id, successor.address).await;

                                if new_successors.len() >= SUCCESSOR_LIST_SIZE {
                                    break;
                                }
                            }
                        }

                        // Update our successor list
                        let mut guard = config.successor_list.lock().await;
                        *guard = new_successors;
                        debug!("Updated successor list: {:?}", guard);
                    }
                    Err(e) => {
                        error!("Failed to get successor list from successor: {}", e);
                        handle_successor_failure(&config, &immediate_successor).await;
                    }
                }
            }
            None => {
                error!(
                    "Failed to connect to successor {}: {}",
                    immediate_successor, "No address found"
                );
                handle_successor_failure(&config, &immediate_successor).await;
            }
        }
    }
}

/// Helper function to handle successor failure
async fn handle_successor_failure(config: &ThreadConfig, failed_successor: &NodeId) {
    // Remove failed successor from list
    let mut guard = config.successor_list.lock().await;
    guard.retain(|&x| x != *failed_successor);

    // If we still have successors, try to stabilize with the next one
    if let Some(next_successor) = guard.first().cloned() {
        debug!(
            "Attempting to stabilize with next successor {}",
            next_successor
        );
        if let Err(e) = trigger_stabilization(config).await {
            error!("Failed to stabilize with next successor: {}", e);
        }
    } else {
        warn!("No more successors available after failure");
    }
}

/// Worker thread that periodically updates finger table entries
pub async fn run_fix_fingers_worker(config: ThreadConfig) {
    info!("Starting finger table maintenance worker");
    let mut next_fix = tokio::time::Instant::now();
    let mut next_finger = 0;

    loop {
        // Wait until next fix interval
        let now = tokio::time::Instant::now();
        if now < next_fix {
            sleep(next_fix - now).await;
            next_fix += FINGER_FIX_INTERVAL;
        }

        debug!("Fixing finger table entry {}", next_finger);

        // Calculate finger ID
        let finger_id = config.local_node_id.get_finger_id(next_finger);

        // Find successor for this finger
        match find_successor(&config, finger_id).await {
            Ok(successor) => {
                // Update finger table
                let mut finger_table = config.finger_table.lock().await;
                finger_table.update_finger(next_finger, successor);
                debug!("Updated finger {} to {}", next_finger, successor);
            }
            Err(e) => error!("Failed to find successor for finger {}: {}", next_finger, e),
        }

        // Move to next finger
        next_finger = (next_finger + 1) % KEY_SIZE;
    }
}

/// Helper function to find successor for a given ID
async fn find_successor(config: &ThreadConfig, id: NodeId) -> Result<NodeId, ChordError> {
    let successor_list = config.successor_list.lock().await;

    if let Some(successor) = successor_list.first() {
        if id.is_between(&config.local_node_id, successor) {
            return Ok(*successor);
        }

        // Drop the successor_list lock before acquiring finger_table lock
        drop(successor_list);

        // Find closest preceding node from finger table
        let finger_table = config.finger_table.lock().await;

        if let Some(closest) = finger_table.find_closest_preceding_node(&id) {
            // Drop the finger_table lock before proceeding with network operations
            drop(finger_table);

            // Forward the query to the closest preceding node
            let addr = {
                let node_addresses = config.node_addresses.lock().await;
                node_addresses
                    .get(&closest)
                    .ok_or_else(|| {
                        ChordError::NodeNotFound(format!("No address for node {}", closest))
                    })?
                    .clone()
            };

            let mut client = ChordGrpcClient::new(addr).await.map_err(|e| {
                ChordError::StabilizationFailed(format!("Failed to connect to node: {}", e))
            })?;

            let response = client
                .find_successor(id.to_bytes().to_vec())
                .await
                .map_err(|e| {
                    ChordError::StabilizationFailed(format!("Failed to find successor: {}", e))
                })?;

            return Ok(NodeId::from_bytes(&response.node_id));
        }
    }

    Err(ChordError::NodeNotFound(
        "No suitable successor found".into(),
    ))
}

pub async fn run_stabilize(node: Arc<ChordNode>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;

        // Get current successor list
        let mut successor_list = node.successor_list.lock().await;
        if successor_list.is_empty() {
            debug!("No successors to stabilize with, attempting emergency recovery");
            drop(successor_list); // Drop lock before recovery
            if let Err(e) = attempt_emergency_recovery(&node).await {
                error!("Emergency recovery failed: {}", e);
                // Add exponential backoff before next attempt
                tokio::time::sleep(interval * 2).await;
            }
            continue;
        }

        let immediate_successor = successor_list[0];
        let mut stabilized = false;
        let mut failed_nodes = Vec::new();

        // Drop the lock to avoid holding it during network operations
        drop(successor_list);

        // Try to stabilize with immediate successor with retries
        if let Some(addr) = node.get_node_address(&immediate_successor).await {
            let mut retry_count = 0;
            let max_retries = 3;
            let mut backoff = Duration::from_millis(100);

            while retry_count < max_retries && !stabilized {
                match stabilize_with_successor(&node, immediate_successor, addr.clone()).await {
                    Ok(true) => {
                        stabilized = true;
                        debug!(
                            "Successfully stabilized with immediate successor {}",
                            immediate_successor
                        );
                        break;
                    }
                    Ok(false) => {
                        // Successor is reachable but invalid
                        debug!(
                            "Invalid successor state detected for {}",
                            immediate_successor
                        );
                        failed_nodes.push(immediate_successor);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Failed to stabilize with successor {} (attempt {}): {}",
                            immediate_successor,
                            retry_count + 1,
                            e
                        );
                        retry_count += 1;
                        if retry_count < max_retries {
                            tokio::time::sleep(backoff).await;
                            backoff *= 2;
                        } else {
                            failed_nodes.push(immediate_successor);
                        }
                    }
                }
            }
        } else {
            error!("No address found for successor {}", immediate_successor);
            failed_nodes.push(immediate_successor);
        }

        // Only try backup successors if not stabilized
        if !stabilized {
            let backup_successors = {
                let successors = node.successor_list.lock().await;
                successors.iter().skip(1).cloned().collect::<Vec<_>>()
            };

            for backup_successor in backup_successors {
                if let Some(addr) = node.get_node_address(&backup_successor).await {
                    match validate_and_stabilize_with_backup(&node, backup_successor, addr).await {
                        Ok(true) => {
                            // Successfully validated and stabilized with backup
                            let mut successor_list = node.successor_list.lock().await;
                            successor_list.retain(|&x| !failed_nodes.contains(&x));
                            successor_list.insert(0, backup_successor);
                            successor_list.truncate(MAX_SUCCESSOR_LIST_SIZE);

                            // Update finger table atomically
                            update_finger_table(&node, 0, backup_successor).await;
                            stabilized = true;
                            break;
                        }
                        Ok(false) => {
                            // Backup is reachable but invalid
                            failed_nodes.push(backup_successor);
                        }
                        Err(_) => {
                            failed_nodes.push(backup_successor);
                        }
                    }
                } else {
                    failed_nodes.push(backup_successor);
                }
            }
        }

        // Handle failed nodes and cleanup
        if !failed_nodes.is_empty() {
            handle_failed_nodes(&node, &failed_nodes).await;
            let mut successor_list = node.successor_list.lock().await;
            successor_list.retain(|&x| !failed_nodes.contains(&x));

            // If list becomes empty after cleanup, trigger emergency recovery
            if successor_list.is_empty() {
                drop(successor_list);
                if let Err(e) = attempt_emergency_recovery(&node).await {
                    error!("Emergency recovery after cleanup failed: {}", e);
                }
            }
        }

        // Only maintain backup successors if we have a valid immediate successor
        if stabilized {
            if let Some(immediate_successor) = {
                let successors = node.successor_list.lock().await;
                successors.first().cloned()
            } {
                maintain_backup_successors(&node, immediate_successor).await;
            }
        }

        // Validate network state periodically
        validate_network_state(&node).await;
    }
}

// Helper function to stabilize with a successor
async fn stabilize_with_successor(
    node: &Arc<ChordNode>,
    successor: NodeId,
    addr: String,
) -> Result<bool, ChordError> {
    let mut client = ChordGrpcClient::new(addr)
        .await
        .map_err(|e| ChordError::StabilizationFailed(format!("Failed to create client: {}", e)))?;

    // First verify the successor is alive and valid
    if let Err(e) = client.heartbeat().await {
        return Err(ChordError::StabilizationFailed(format!(
            "Successor heartbeat failed: {}",
            e
        )));
    }

    client.stabilize().await.map_err(|e| {
        ChordError::StabilizationFailed(format!("Failed to stabilize with successor: {}", e))
    })?;

    // Get and validate predecessor info
    match client.get_predecessor().await {
        Ok(pred_resp) => {
            if let Some(pred_info) = pred_resp.predecessor {
                let pred_id = NodeId::from_bytes(&pred_info.node_id);
                if validate_predecessor(node, &pred_id, &successor).await {
                    // Update successor list with the new node
                    let mut successor_list = node.successor_list.lock().await;
                    successor_list.insert(0, pred_id);
                    successor_list.truncate(MAX_SUCCESSOR_LIST_SIZE);

                    // Update node address atomically
                    update_node_address(node, pred_id, pred_info.address).await;
                    return Ok(true);
                }
            }
            // No predecessor or invalid predecessor, but node is responsive
            Ok(true)
        }
        Err(e) => Err(ChordError::StabilizationFailed(format!(
            "Failed to get predecessor info: {}",
            e
        ))),
    }
}

// Helper function to validate and stabilize with a backup successor
async fn validate_and_stabilize_with_backup(
    node: &Arc<ChordNode>,
    successor: NodeId,
    addr: String,
) -> Result<bool, ChordError> {
    let mut client = ChordGrpcClient::new(addr)
        .await
        .map_err(|e| ChordError::StabilizationFailed(format!("Failed to create client: {}", e)))?;

    // First verify the backup is alive
    if let Err(e) = client.heartbeat().await {
        return Err(ChordError::StabilizationFailed(format!(
            "Backup successor heartbeat failed: {}",
            e
        )));
    }

    // Verify the backup's state is consistent
    match client.get_predecessor().await {
        Ok(pred_resp) => {
            if let Some(pred_info) = pred_resp.predecessor {
                let pred_id = NodeId::from_bytes(&pred_info.node_id);
                if !validate_predecessor(node, &pred_id, &successor).await {
                    return Ok(false);
                }
            }
        }
        Err(e) => {
            return Err(ChordError::StabilizationFailed(format!(
                "Failed to validate backup successor: {}",
                e
            )))
        }
    }

    // Try to stabilize
    client.stabilize().await.map_err(|e| {
        ChordError::StabilizationFailed(format!("Failed to stabilize with backup: {}", e))
    })?;

    Ok(true)
}

// Modified emergency recovery function
async fn attempt_emergency_recovery(node: &Arc<ChordNode>) -> Result<(), ChordError> {
    debug!("Attempting emergency recovery of successor list");

    // Try to rebuild from finger table
    let potential_successors = {
        let finger_table = node.finger_table.lock().await;
        let mut successors = Vec::new();
        for entry in &finger_table.entries {
            if let Some(n) = entry.node {
                if !successors.contains(&n) {
                    successors.push(n);
                }
            }
        }
        successors
    };

    let mut recovered = false;
    for &potential_successor in &potential_successors {
        if let Some(addr) = node.get_node_address(&potential_successor).await {
            match validate_and_stabilize_with_backup(node, potential_successor, addr).await {
                Ok(true) => {
                    let mut successor_list = node.successor_list.lock().await;
                    successor_list.push(potential_successor);
                    debug!(
                        "Emergency recovery: Added {} as successor",
                        potential_successor
                    );
                    recovered = true;
                    break;
                }
                _ => continue,
            }
        }
    }

    if !recovered {
        return Err(ChordError::StabilizationFailed(
            "Emergency recovery failed to find any valid successors".into(),
        ));
    }

    Ok(())
}

async fn validate_predecessor(node: &Arc<ChordNode>, pred_id: &NodeId, successor: &NodeId) -> bool {
    // Check if predecessor is between us and our successor
    if !pred_id.is_between(&node.node_id, successor) {
        return false;
    }

    // Verify predecessor is reachable
    if let Some(addr) = node.get_node_address(pred_id).await {
        if let Ok(mut client) = ChordGrpcClient::new(addr).await {
            return client.heartbeat().await.is_ok();
        }
    }
    false
}

async fn update_node_address(node: &Arc<ChordNode>, node_id: NodeId, addr: String) {
    let mut addresses = node.node_addresses.lock().await;
    addresses.insert(node_id, addr);
}

async fn update_finger_table(node: &Arc<ChordNode>, index: usize, new_node: NodeId) {
    let mut finger_table = node.finger_table.lock().await;
    finger_table.update_finger(index, new_node);
}

async fn handle_failed_nodes(node: &Arc<ChordNode>, failed_nodes: &[NodeId]) {
    let mut addresses = node.node_addresses.lock().await;
    for &failed_node in failed_nodes {
        addresses.remove(&failed_node);

        // Clean up finger table entries
        let mut finger_table = node.finger_table.lock().await;
        for i in 0..finger_table.entries.len() {
            if finger_table.entries[i].node == Some(failed_node) {
                finger_table.entries[i].node = None;
            }
        }
    }
}

async fn maintain_backup_successors(node: &Arc<ChordNode>, immediate_successor: NodeId) {
    if let Some(addr) = node.get_node_address(&immediate_successor).await {
        if let Ok(mut client) = ChordGrpcClient::new(addr).await {
            match client.get_successor_list().await {
                Ok(successors) => {
                    let mut successor_list = node.successor_list.lock().await;
                    for succ_info in successors {
                        let succ_id = NodeId::from_bytes(&succ_info.node_id);
                        if !successor_list.contains(&succ_id) {
                            successor_list.push(succ_id);
                            // Update node address
                            let mut addresses = node.node_addresses.lock().await;
                            addresses.insert(succ_id, succ_info.address);
                        }
                    }
                    successor_list.truncate(MAX_SUCCESSOR_LIST_SIZE);
                }
                Err(e) => error!(
                    "Failed to get successor list from {}: {}",
                    immediate_successor, e
                ),
            }
        }
    }
}

async fn validate_network_state(node: &Arc<ChordNode>) {
    let successor_list = node.successor_list.lock().await;
    if successor_list.is_empty() {
        error!("Invalid state: Empty successor list");
        drop(successor_list);
        attempt_emergency_recovery(node).await;
        return;
    }

    // Verify immediate successor is reachable
    if let Some(&immediate_successor) = successor_list.first() {
        if let Some(addr) = node.get_node_address(&immediate_successor).await {
            if let Err(e) = ChordGrpcClient::new(addr).await {
                error!("Invalid state: Immediate successor unreachable: {}", e);
                drop(successor_list);
                attempt_emergency_recovery(node).await;
            }
        }
    }
}

async fn check_predecessor(config: Arc<ChordNode>) {
    let pred_id = {
        let pred = config.predecessor.lock().await;
        match *pred {
            Some(id) => id,
            None => return,
        }
    };

    let mut is_alive = false;
    for _ in 0..3 {
        if let Some(pred_addr) = config.get_node_address(&pred_id).await {
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
