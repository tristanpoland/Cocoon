//! Shard information and statistics data structures
//!
//! Provides structures for storing and managing information about shards and their contents.

use chrono::{DateTime, Utc};
use crate::segment::SegmentId;
use crate::shard::{ShardId, ShardPeriod};

/// Information about a shard
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// Shard ID
    pub id: ShardId,
    /// Shard period
    pub period: ShardPeriod,
    /// Shard start time
    pub start_time: DateTime<Utc>,
    /// Shard end time
    pub end_time: DateTime<Utc>,
    /// List of segment IDs in this shard
    pub segments: Vec<SegmentId>,
    /// Total entries in the shard
    pub entry_count: usize,
    /// Total size of the shard in bytes
    pub size_bytes: usize,
    /// Whether the shard is active (accepting new entries)
    pub active: bool,
}

impl ShardInfo {
    /// Create a new shard info
    pub fn new(
        id: ShardId,
        period: ShardPeriod,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            period,
            start_time,
            end_time,
            segments: Vec::new(),
            entry_count: 0,
            size_bytes: 0,
            active: true,
        }
    }
    
    /// Add a segment to the shard
    pub fn add_segment(&mut self, segment_id: SegmentId, entry_count: usize, size_bytes: usize) {
        self.segments.push(segment_id);
        self.entry_count += entry_count;
        self.size_bytes += size_bytes;
    }
    
    /// Remove a segment from the shard
    pub fn remove_segment(&mut self, segment_id: SegmentId, entry_count: usize, size_bytes: usize) {
        self.segments.retain(|id| *id != segment_id);
        self.entry_count = self.entry_count.saturating_sub(entry_count);
        self.size_bytes = self.size_bytes.saturating_sub(size_bytes);
    }
    
    /// Check if a timestamp is in this shard's time range
    pub fn contains_timestamp(&self, timestamp: &DateTime<Utc>) -> bool {
        *timestamp >= self.start_time && *timestamp < self.end_time
    }
    
    /// Get the approximate compression ratio of the shard
    pub fn compression_ratio(&self, compressed_size: usize) -> f64 {
        if compressed_size == 0 || self.size_bytes == 0 {
            return 1.0;
        }
        
        self.size_bytes as f64 / compressed_size as f64
    }
    
    /// Check if the shard is empty
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty() || self.entry_count == 0
    }
    
    /// Mark the shard as inactive
    pub fn mark_inactive(&mut self) {
        self.active = false;
    }
    
    /// Mark the shard as active
    pub fn mark_active(&mut self) {
        self.active = true;
    }
    
    /// Get the time range of the shard
    pub fn time_range(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.start_time, self.end_time)
    }
    
    /// Calculate the age of the shard in days
    pub fn age_days(&self) -> f64 {
        let now = Utc::now();
        let duration = now.signed_duration_since(self.start_time);
        duration.num_seconds() as f64 / (24.0 * 60.0 * 60.0)
    }
}

/// Statistics about shards
#[derive(Debug, Clone, Default)]
pub struct ShardStats {
    /// Number of shards
    pub shard_count: usize,
    /// Number of segments
    pub segment_count: usize,
    /// Total number of log entries
    pub total_logs: usize,
    /// Total raw size of log data in bytes
    pub raw_size_bytes: usize,
    /// Total compressed size of log data in bytes
    pub compressed_size_bytes: usize,
    /// Number of active shards
    pub active_shards: usize,
    /// Number of inactive shards
    pub inactive_shards: usize,
    /// Number of empty shards
    pub empty_shards: usize,
    /// Oldest shard timestamp
    pub oldest_timestamp: Option<DateTime<Utc>>,
    /// Newest shard timestamp
    pub newest_timestamp: Option<DateTime<Utc>>,
}

impl ShardStats {
    /// Create new empty shard stats
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Calculate the overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size_bytes == 0 || self.raw_size_bytes == 0 {
            return 1.0;
        }
        
        self.raw_size_bytes as f64 / self.compressed_size_bytes as f64
    }
    
    /// Calculate the average number of logs per shard
    pub fn avg_logs_per_shard(&self) -> f64 {
        if self.shard_count == 0 {
            return 0.0;
        }
        
        self.total_logs as f64 / self.shard_count as f64
    }
    
    /// Calculate the average number of segments per shard
    pub fn avg_segments_per_shard(&self) -> f64 {
        if self.shard_count == 0 {
            return 0.0;
        }
        
        self.segment_count as f64 / self.shard_count as f64
    }
    
    /// Calculate the average log size in bytes
    pub fn avg_log_size(&self) -> f64 {
        if self.total_logs == 0 {
            return 0.0;
        }
        
        self.raw_size_bytes as f64 / self.total_logs as f64
    }
    
    /// Get the total size of all shard data in bytes (compressed)
    pub fn total_size_bytes(&self) -> usize {
        self.compressed_size_bytes
    }
    
    /// Check if there are any shards
    pub fn has_shards(&self) -> bool {
        self.shard_count > 0
    }
    
    /// Add another stats object to this one
    pub fn merge(&mut self, other: &Self) {
        self.shard_count += other.shard_count;
        self.segment_count += other.segment_count;
        self.total_logs += other.total_logs;
        self.raw_size_bytes += other.raw_size_bytes;
        self.compressed_size_bytes += other.compressed_size_bytes;
        self.active_shards += other.active_shards;
        self.inactive_shards += other.inactive_shards;
        self.empty_shards += other.empty_shards;
        
        // Update timestamp ranges
        if let Some(other_oldest) = other.oldest_timestamp {
            if let Some(ref mut self_oldest) = self.oldest_timestamp {
                if other_oldest < *self_oldest {
                    *self_oldest = other_oldest;
                }
            } else {
                self.oldest_timestamp = Some(other_oldest);
            }
        }
        
        if let Some(other_newest) = other.newest_timestamp {
            if let Some(ref mut self_newest) = self.newest_timestamp {
                if other_newest > *self_newest {
                    *self_newest = other_newest;
                }
            } else {
                self.newest_timestamp = Some(other_newest);
            }
        }
    }
    
    /// Create a human-readable report of statistics
    pub fn report(&self) -> String {
        let mut result = String::new();
        
        result.push_str("=== Shard Statistics ===\n\n");
        
        result.push_str(&format!("Total Shards: {}\n", self.shard_count));
        result.push_str(&format!("  - Active: {}\n", self.active_shards));
        result.push_str(&format!("  - Inactive: {}\n", self.inactive_shards));
        result.push_str(&format!("  - Empty: {}\n", self.empty_shards));
        
        result.push_str(&format!("Total Segments: {}\n", self.segment_count));
        result.push_str(&format!("Total Log Entries: {}\n", self.total_logs));
        
        result.push_str(&format!("Raw Data Size: {} bytes\n", self.raw_size_bytes));
        result.push_str(&format!("Compressed Size: {} bytes\n", self.compressed_size_bytes));
        result.push_str(&format!("Compression Ratio: {:.2}x\n", self.compression_ratio()));
        
        if self.has_shards() {
            result.push_str(&format!("Avg. Logs per Shard: {:.2}\n", self.avg_logs_per_shard()));
            result.push_str(&format!("Avg. Segments per Shard: {:.2}\n", self.avg_segments_per_shard()));
            result.push_str(&format!("Avg. Log Size: {:.2} bytes\n", self.avg_log_size()));
        }
        
        if let Some(oldest) = self.oldest_timestamp {
            result.push_str(&format!("Oldest Log: {}\n", oldest));
        }
        
        if let Some(newest) = self.newest_timestamp {
            result.push_str(&format!("Newest Log: {}\n", newest));
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    
    #[test]
    fn test_shard_info_basic() {
        let now = Utc::now();
        let tomorrow = now + Duration::days(1);
        
        let mut info = ShardInfo::new(
            "20230101".to_string(),
            ShardPeriod::Day,
            now,
            tomorrow,
        );
        
        // Test adding segments
        info.add_segment(1, 100, 1000);
        info.add_segment(2, 200, 2000);
        
        assert_eq!(info.segments.len(), 2);
        assert_eq!(info.entry_count, 300);
        assert_eq!(info.size_bytes, 3000);
        
        // Test removing segments
        info.remove_segment(1, 100, 1000);
        
        assert_eq!(info.segments.len(), 1);
        assert_eq!(info.entry_count, 200);
        assert_eq!(info.size_bytes, 2000);
        
        // Test contains_timestamp
        assert!(info.contains_timestamp(&now));
        assert!(!info.contains_timestamp(&tomorrow));
        
        // Test active status
        assert!(info.active);
        info.mark_inactive();
        assert!(!info.active);
        info.mark_active();
        assert!(info.active);
    }
    
    #[test]
    fn test_shard_stats() {
        let mut stats = ShardStats::new();
        
        // Test empty stats
        assert_eq!(stats.shard_count, 0);
        assert_eq!(stats.avg_logs_per_shard(), 0.0);
        assert!(!stats.has_shards());
        
        // Update stats
        stats.shard_count = 10;
        stats.segment_count = 20;
        stats.total_logs = 1000;
        stats.raw_size_bytes = 10000;
        stats.compressed_size_bytes = 2000;
        stats.active_shards = 8;
        stats.inactive_shards = 2;
        
        // Test calculations
        assert_eq!(stats.compression_ratio(), 5.0);
        assert_eq!(stats.avg_logs_per_shard(), 100.0);
        assert_eq!(stats.avg_segments_per_shard(), 2.0);
        assert_eq!(stats.avg_log_size(), 10.0);
        assert!(stats.has_shards());
        
        // Test merging
        let mut other_stats = ShardStats::new();
        other_stats.shard_count = 5;
        other_stats.segment_count = 10;
        other_stats.total_logs = 500;
        other_stats.raw_size_bytes = 5000;
        other_stats.compressed_size_bytes = 1000;
        other_stats.active_shards = 4;
        other_stats.inactive_shards = 1;
        
        stats.merge(&other_stats);
        
        assert_eq!(stats.shard_count, 15);
        assert_eq!(stats.segment_count, 30);
        assert_eq!(stats.total_logs, 1500);
        assert_eq!(stats.raw_size_bytes, 15000);
        assert_eq!(stats.compressed_size_bytes, 3000);
        assert_eq!(stats.active_shards, 12);
        assert_eq!(stats.inactive_shards, 3);
        
        // Test report
        let report = stats.report();
        assert!(report.contains("Total Shards: 15"));
        assert!(report.contains("Compression Ratio: 5.00x"));
    }
}