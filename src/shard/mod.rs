//! Time-based shard management for log storage
//!
//! This module organizes logs into time-based shards for efficient storage and retrieval.
//! Each shard represents a specific time period (day, week, month, or year) and contains
//! one or more segments of log data.

mod period;
mod info;
mod manager;
mod error;
mod util;

pub use period::ShardPeriod;
pub use info::{ShardInfo, ShardStats};
pub use manager::ShardManager;
pub use error::{ShardError, ShardResult};

/// Shard ID type
pub type ShardId = String;

/// Magic bytes and version for shard files
pub(crate) const SHARD_MAGIC: &[u8; 4] = b"CCNS"; // "Cocoon Shard"
pub(crate) const SHARD_VERSION: u32 = 1;