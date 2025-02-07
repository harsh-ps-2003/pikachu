use crate::chord::types::{ThreadConfig, NodeId, KEY_SIZE};
use crate::network::grpc::client::ChordGrpcClient;
use log::{debug, error, info};
use std::time::Duration;
use tokio::time::sleep;

const STABILIZE_INTERVAL: Duration = Duration::from_secs(30);
const PREDECESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(15);
const FINGER_FIX_INTERVAL: Duration = Duration::from_secs(45);
const SUCCESSOR_CHECK_INTERVAL: Duration = Duration::from_secs(20);

pub async fn run_stabilize_worker(config: ThreadConfig) {
    let mut client = match ChordGrpcClient::new(config.local_addr).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create gRPC client for stabilize worker: {}", e);
            return;
        }
    };

    loop {
        sleep(STABILIZE_INTERVAL).await;
        debug!("Running stabilization process");

        // Update successor list
        if let Err(e) = client.stabilize().await {
            error!("Stabilization failed: {}", e);
        }
    }
}

pub async fn run_predecessor_checker(config: ThreadConfig) {
    let mut client = match ChordGrpcClient::new(config.local_addr).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create gRPC client for predecessor checker: {}", e);
            return;
        }
    };

    loop {
        sleep(PREDECESSOR_CHECK_INTERVAL).await;
        debug!("Checking predecessor health");

        let pred = {
            let guard = config.predecessor.lock().unwrap();
            guard.clone()
        };

        if let Some(pred_id) = pred {
            if let Err(e) = client.check_predecessor(pred_id).await {
                error!("Predecessor check failed: {}", e);
                // Clear predecessor if unreachable
                let mut guard = config.predecessor.lock().unwrap();
                *guard = None;
            }
        }
    }
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

pub async fn run_successor_maintainer(config: ThreadConfig) {
    let mut client = match ChordGrpcClient::new(config.local_addr).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create gRPC client for successor maintainer: {}", e);
            return;
        }
    };

    loop {
        sleep(SUCCESSOR_CHECK_INTERVAL).await;
        debug!("Checking successor list health");

        let successors = {
            let guard = config.successor_list.lock().unwrap();
            guard.clone()
        };

        for successor in successors {
            if let Err(e) = client.check_successor(successor).await {
                error!("Successor check failed for {}: {}", successor, e);
                // Remove failed successor from list
                let mut guard = config.successor_list.lock().unwrap();
                guard.retain(|&x| x != successor);
            }
        }
    }
} 