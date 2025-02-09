use crate::chord::types::{ThreadConfig, NodeId, KEY_SIZE};
use crate::network::grpc::client::ChordGrpcClient;
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::sleep;
use crate::error::ChordError;

const STABILIZE_INTERVAL: Duration = Duration::from_secs(30);
const PREDECESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(15);
const FINGER_FIX_INTERVAL: Duration = Duration::from_secs(45);
const SUCCESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(20);
const MAX_HEARTBEAT_RETRIES: u32 = 3;
const SUCCESSOR_LIST_SIZE: usize = 3;

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
            match ChordGrpcClient::new(successor_addr.to_string()).await {
                Ok(mut client) => {
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
                    if let Err(e) = client.notify(config.local_node_id).await {
                        error!("Failed to notify successor: {}", e);
                    }
                }
                Err(e) => error!("Failed to connect to successor: {}", e),
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
                    warn!("No address found for predecessor {}, marking as failed", pred_id);
                    clear_predecessor(&config).await;
                    continue;
                }
            };

            // Attempt heartbeat with retries
            while retry_count < MAX_HEARTBEAT_RETRIES && !is_alive {
                match ChordGrpcClient::new(pred_addr.clone()).await {
                    Ok(mut client) => {
                        match client.heartbeat().await {
                            Ok(response) => {
                                if response.alive {
                                    is_alive = true;
                                    debug!("Predecessor {} is alive", pred_id);
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("Heartbeat to predecessor failed (attempt {}): {}", 
                                    retry_count + 1, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to connect to predecessor (attempt {}): {}", 
                            retry_count + 1, e);
                    }
                }
                
                retry_count += 1;
                if retry_count < MAX_HEARTBEAT_RETRIES {
                    sleep(Duration::from_millis(500)).await;
                }
            }

            // If all retries failed, clear predecessor
            if !is_alive {
                warn!("Predecessor {} failed all heartbeat attempts, marking as failed", pred_id);
                clear_predecessor(&config).await;
                
                // Trigger immediate stabilization to find new predecessor
                if let Err(e) = trigger_stabilization(&config).await {
                    error!("Failed to trigger stabilization after predecessor failure: {}", e);
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

    client.stabilize()
        .await
        .map_err(|e| ChordError::StabilizationFailed(format!("Failed to trigger stabilization: {}", e)))?;

    Ok(())
}

pub async fn run_finger_maintainer(config: ThreadConfig) {
    let mut client = match ChordGrpcClient::new(config.local_addr).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create gRPC client for finger maintainer: {}", e);
            return;
        }
    };

    let mut next_finger = 0;

    loop {
        sleep(FINGER_FIX_INTERVAL).await;
        debug!("Fixing finger table entry {}", next_finger);

        if let Err(e) = client.fix_finger(next_finger).await {
            error!("Failed to fix finger {}: {}", next_finger, e);
        }

        next_finger = (next_finger + 1) % KEY_SIZE;
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
                warn!("No address found for immediate successor {}", immediate_successor);
                continue;
            }
        };

        // Get successor's successor list
        match ChordGrpcClient::new(successor_addr.clone()).await {
            Ok(mut client) => {
                match client.get_successor_list().await {
                    Ok(successor_list) => {
                        // Create new successor list starting with our immediate successor
                        let mut new_successors = vec![immediate_successor];
                        
                        // Add successors from our successor's list until we reach SUCCESSOR_LIST_SIZE
                        for successor in successor_list {
                            let successor_id = NodeId::from_bytes(&successor.node_id);
                            
                            // Don't add duplicates or ourselves
                            if !new_successors.contains(&successor_id) && 
                               successor_id != config.local_node_id {
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
            Err(e) => {
                error!("Failed to connect to successor {}: {}", immediate_successor, e);
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
        debug!("Attempting to stabilize with next successor {}", next_successor);
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
                node_addresses.get(&closest)
                    .ok_or_else(|| ChordError::NodeNotFound(format!("No address for node {}", closest)))?
                    .clone()
            };
            
            let mut client = ChordGrpcClient::new(addr)
                .await
                .map_err(|e| ChordError::StabilizationFailed(format!("Failed to connect to node: {}", e)))?;
            
            let response = client.find_successor(id.to_bytes().to_vec())
                .await
                .map_err(|e| ChordError::StabilizationFailed(format!("Failed to find successor: {}", e)))?;
            
            return Ok(NodeId::from_bytes(&response.node_id));
        }
    }
    
    Err(ChordError::NodeNotFound("No suitable successor found".into()))
} 