pub mod actor;
pub mod routing;
pub mod types;

// Chord configuration
pub const SUCCESSOR_LIST_SIZE: usize = 8;
pub const STABILIZE_INTERVAL: u64 = 30;
pub const FIX_FINGERS_INTERVAL: u64 = 60; 
pub const FINGER_TABLE_SIZE: usize = 256;

// Custom protocol for Chord routing, essential for distinguishing the Chord protocol from other protocols that may be running on the same network.
pub const CHORD_PROTOCOL: &[u8] = b"/chord/1.0.0";