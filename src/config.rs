//! Configuration for Cocoon
//!
//! This module provides configuration options for the Cocoon log database.

use std::path::{Path, PathBuf};
use std::time::Duration;
use serde::{Serialize, Deserialize};

use crate::error::{Result, Error};
use crate::shard::ShardPeriod;

/// Compression algorithms supported by Cocoon
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CompressionAlgorithm {
    /// No compression, fastest but largest storage requirements
    None,
    /// LZ4 compression, good balance of speed and compression ratio
    Lz4,
    /// Zstandard compression, better compression than LZ4 but slower
    Zstd,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::Zstd
    }
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
        }
    }
}

impl CompressionAlgorithm {
    /// Parse a compression algorithm from a string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(Error::config(format!("Unknown compression algorithm: {}", s))),
        }
    }
    
    /// Get the name of the compression algorithm
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
    
    /// Check if compression is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Recovery modes for the store
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RecoveryMode {
    /// Always attempt to recover, potentially losing some data
    BestEffort,
    /// Only recover if consistent state can be achieved
    Consistent,
    /// Attempt to recover as much data as possible, may be slower
    Conservative,
}

impl Default for RecoveryMode {
    fn default() -> Self {
        Self::Consistent
    }
}

impl std::fmt::Display for RecoveryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BestEffort => write!(f, "best_effort"),
            Self::Consistent => write!(f, "consistent"),
            Self::Conservative => write!(f, "conservative"),
        }
    }
}

impl RecoveryMode {
    /// Parse a recovery mode from a string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "best_effort" => Ok(Self::BestEffort),
            "consistent" => Ok(Self::Consistent),
            "conservative" => Ok(Self::Conservative),
            _ => Err(Error::config(format!("Unknown recovery mode: {}", s))),
        }
    }
    
    /// Get the name of the recovery mode
    pub fn name(&self) -> &'static str {
        match self {
            Self::BestEffort => "best_effort",
            Self::Consistent => "consistent",
            Self::Conservative => "conservative",
        }
    }
}

/// Configuration options for a Cocoon store
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StoreConfig {
    // Storage configuration
    /// Maximum size of a segment in bytes
    pub max_segment_size: usize,
    /// Maximum number of segments per shard
    pub max_segments_per_shard: usize,
    /// Number of bits for Bloom filters
    pub bloom_filter_bits: usize,
    /// Base directory for storage
    pub directory: Option<PathBuf>,
    /// Shard period to use (default: Day)
    pub shard_period: Option<ShardPeriod>,
    
    // Performance tuning
    /// Size of the write buffer in bytes
    pub write_buffer_size: usize,
    /// Compression algorithm to use
    pub compression_algorithm: CompressionAlgorithm,
    /// Compression level (0-9, higher = better compression)
    pub compression_level: i32,
    /// Size of the cache in megabytes
    pub cache_size_mb: usize,
    /// Number of threads to use for compaction
    pub compaction_threads: usize,
    
    // Reliability settings
    /// Whether to sync writes to disk immediately
    pub sync_writes: bool,
    /// Whether to verify checksums on read
    pub checksum_verification: bool,
    /// Recovery mode to use after crash
    pub recovery_mode: RecoveryMode,
    
    // Retention policy
    /// Number of days to retain logs
    pub retention_days: u32,
    /// Interval in hours between retention checks
    pub retention_check_interval_hours: u32,
    
    // Query settings
    /// Default maximum results per query
    pub default_query_limit: usize,
    /// Whether to use parallel query execution by default
    pub parallel_query_execution: bool,
    
    // Additional settings
    /// Optional encryption key (feature dependent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<String>,
    /// Enable metrics collection
    pub collect_metrics: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            // Storage configuration
            max_segment_size: 256 * 1024 * 1024, // 256MB
            max_segments_per_shard: 16,
            bloom_filter_bits: 10,
            directory: None,
            shard_period: Some(ShardPeriod::Day),
            
            // Performance tuning
            write_buffer_size: 32 * 1024 * 1024, // 32MB
            compression_algorithm: CompressionAlgorithm::Zstd,
            compression_level: 3,
            cache_size_mb: 512, // 512MB
            compaction_threads: 1,
            
            // Reliability settings
            sync_writes: false,
            checksum_verification: true,
            recovery_mode: RecoveryMode::default(),
            
            // Retention policy
            retention_days: 30,
            retention_check_interval_hours: 24,
            
            // Query settings
            default_query_limit: 1000,
            parallel_query_execution: true,
            
            // Additional settings
            encryption_key: None,
            collect_metrics: true,
        }
    }
}

impl StoreConfig {
    /// Create a new store configuration with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the maximum segment size
    pub fn with_max_segment_size(mut self, size: usize) -> Self {
        self.max_segment_size = size;
        self
    }
    
    /// Set the maximum number of segments per shard
    pub fn with_max_segments_per_shard(mut self, count: usize) -> Self {
        self.max_segments_per_shard = count;
        self
    }
    
    /// Set the number of bits for Bloom filters
    pub fn with_bloom_filter_bits(mut self, bits: usize) -> Self {
        self.bloom_filter_bits = bits;
        self
    }
    
    /// Set the base directory for storage
    pub fn with_directory<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.directory = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Set the shard period to use
    pub fn with_shard_period(mut self, period: ShardPeriod) -> Self {
        self.shard_period = Some(period);
        self
    }
    
    /// Set the write buffer size
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }
    
    /// Set the compression algorithm
    pub fn with_compression_algorithm(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.compression_algorithm = algorithm;
        self
    }
    
    /// Set the compression level
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }
    
    /// Set the cache size in megabytes
    pub fn with_cache_size_mb(mut self, size: usize) -> Self {
        self.cache_size_mb = size;
        self
    }
    
    /// Set the number of threads to use for compaction
    pub fn with_compaction_threads(mut self, threads: usize) -> Self {
        self.compaction_threads = threads;
        self
    }
    
    /// Set whether to sync writes to disk immediately
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }
    
    /// Set whether to verify checksums on read
    pub fn with_checksum_verification(mut self, verify: bool) -> Self {
        self.checksum_verification = verify;
        self
    }
    
    /// Set the recovery mode
    pub fn with_recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }
    
    /// Set the number of days to retain logs
    pub fn with_retention_days(mut self, days: u32) -> Self {
        self.retention_days = days;
        self
    }
    
    /// Set the interval in hours between retention checks
    pub fn with_retention_check_interval_hours(mut self, hours: u32) -> Self {
        self.retention_check_interval_hours = hours;
        self
    }
    
    /// Set the default maximum results per query
    pub fn with_default_query_limit(mut self, limit: usize) -> Self {
        self.default_query_limit = limit;
        self
    }
    
    /// Set whether to use parallel query execution by default
    pub fn with_parallel_query_execution(mut self, parallel: bool) -> Self {
        self.parallel_query_execution = parallel;
        self
    }
    
    /// Set the encryption key (requires encryption feature)
    #[cfg(feature = "encryption")]
    pub fn with_encryption_key(mut self, key: impl Into<String>) -> Self {
        self.encryption_key = Some(key.into());
        self
    }
    
    /// Set whether to collect metrics
    pub fn with_collect_metrics(mut self, collect: bool) -> Self {
        self.collect_metrics = collect;
        self
    }
    
    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.max_segment_size < 1024 * 1024 {
            return Err(Error::config(
                "Maximum segment size must be at least 1MB"
            ));
        }
        
        if self.max_segments_per_shard < 2 {
            return Err(Error::config(
                "Maximum segments per shard must be at least 2"
            ));
        }
        
        if self.bloom_filter_bits < 4 || self.bloom_filter_bits > 20 {
            return Err(Error::config(
                "Bloom filter bits must be between 4 and 20"
            ));
        }
        
        if self.write_buffer_size < 1024 * 1024 {
            return Err(Error::config(
                "Write buffer size must be at least 1MB"
            ));
        }
        
        if self.compression_level < 0 || self.compression_level > 9 {
            return Err(Error::config(
                "Compression level must be between 0 and 9"
            ));
        }
        
        if self.cache_size_mb < 16 {
            return Err(Error::config(
                "Cache size must be at least 16MB"
            ));
        }
        
        if self.compaction_threads < 1 {
            return Err(Error::config(
                "Compaction threads must be at least 1"
            ));
        }
        
        if self.retention_days < 1 {
            return Err(Error::config(
                "Retention days must be at least 1"
            ));
        }
        
        if self.retention_check_interval_hours < 1 {
            return Err(Error::config(
                "Retention check interval must be at least 1 hour"
            ));
        }
        
        if self.default_query_limit < 1 {
            return Err(Error::config(
                "Default query limit must be at least 1"
            ));
        }
        
        Ok(())
    }
    
    /// Check if the configuration is using encryption
    pub fn is_using_encryption(&self) -> bool {
        #[cfg(feature = "encryption")]
        return self.encryption_key.is_some();
        
        #[cfg(not(feature = "encryption"))]
        return false;
    }
    
    /// Get the recovery mode as a string
    pub fn recovery_mode_str(&self) -> &'static str {
        self.recovery_mode.name()
    }
    
    /// Get the compression algorithm as a string
    pub fn compression_algorithm_str(&self) -> &'static str {
        self.compression_algorithm.name()
    }
    
    /// Get retention period as a Duration
    pub fn retention_period(&self) -> Duration {
        Duration::from_secs(self.retention_days as u64 * 24 * 60 * 60)
    }
    
    /// Get retention check interval as a Duration
    pub fn retention_check_interval(&self) -> Duration {
        Duration::from_secs(self.retention_check_interval_hours as u64 * 60 * 60)
    }
    
    /// Get cache size in bytes
    pub fn cache_size_bytes(&self) -> usize {
        self.cache_size_mb * 1024 * 1024
    }
    
    /// Create a human-readable string representation of the configuration
    pub fn to_string_pretty(&self) -> String {
        let mut result = String::new();
        
        result.push_str("=== Cocoon Configuration ===\n\n");
        
        // Storage configuration
        result.push_str("Storage Configuration:\n");
        result.push_str(&format!("  Max Segment Size: {} MB\n", self.max_segment_size / 1024 / 1024));
        result.push_str(&format!("  Max Segments per Shard: {}\n", self.max_segments_per_shard));
        result.push_str(&format!("  Bloom Filter Bits: {}\n", self.bloom_filter_bits));
        if let Some(ref dir) = self.directory {
            result.push_str(&format!("  Directory: {:?}\n", dir));
        }
        if let Some(period) = self.shard_period {
            result.push_str(&format!("  Shard Period: {}\n", period));
        }
        
        // Performance tuning
        result.push_str("\nPerformance Tuning:\n");
        result.push_str(&format!("  Write Buffer Size: {} MB\n", self.write_buffer_size / 1024 / 1024));
        result.push_str(&format!("  Compression Algorithm: {}\n", self.compression_algorithm));
        result.push_str(&format!("  Compression Level: {}\n", self.compression_level));
        result.push_str(&format!("  Cache Size: {} MB\n", self.cache_size_mb));
        result.push_str(&format!("  Compaction Threads: {}\n", self.compaction_threads));
        
        // Reliability settings
        result.push_str("\nReliability Settings:\n");
        result.push_str(&format!("  Sync Writes: {}\n", self.sync_writes));
        result.push_str(&format!("  Checksum Verification: {}\n", self.checksum_verification));
        result.push_str(&format!("  Recovery Mode: {}\n", self.recovery_mode));
        
        // Retention policy
        result.push_str("\nRetention Policy:\n");
        result.push_str(&format!("  Retention Days: {}\n", self.retention_days));
        result.push_str(&format!("  Retention Check Interval: {} hours\n", self.retention_check_interval_hours));
        
        // Query settings
        result.push_str("\nQuery Settings:\n");
        result.push_str(&format!("  Default Query Limit: {}\n", self.default_query_limit));
        result.push_str(&format!("  Parallel Query Execution: {}\n", self.parallel_query_execution));
        
        // Additional settings
        result.push_str("\nAdditional Settings:\n");
        if self.is_using_encryption() {
            result.push_str("  Encryption: Enabled\n");
        } else {
            result.push_str("  Encryption: Disabled\n");
        }
        result.push_str(&format!("  Collect Metrics: {}\n", self.collect_metrics));
        
        result
    }
    
    /// Load configuration from a TOML file
    #[cfg(feature = "toml")]
    pub fn from_toml_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        use std::fs::read_to_string;
        use toml::from_str;
        
        let content = read_to_string(path)?;
        let config = from_str(&content)
            .map_err(|e| Error::config(format!("Failed to parse TOML: {}", e)))?;
        
        Ok(config)
    }
    
    /// Save configuration to a TOML file
    #[cfg(feature = "toml")]
    pub fn to_toml_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use std::fs::write;
        use toml::to_string_pretty;
        
        let content = to_string_pretty(self)
            .map_err(|e| Error::config(format!("Failed to serialize to TOML: {}", e)))?;
        
        write(path, content)?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = StoreConfig::default();
        
        // Check default values
        assert_eq!(config.max_segment_size, 256 * 1024 * 1024);
        assert_eq!(config.max_segments_per_shard, 16);
        assert_eq!(config.bloom_filter_bits, 10);
        assert_eq!(config.write_buffer_size, 32 * 1024 * 1024);
        assert_eq!(config.compression_algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(config.compression_level, 3);
        assert_eq!(config.cache_size_mb, 512);
        assert_eq!(config.compaction_threads, 1);
        assert_eq!(config.sync_writes, false);
        assert_eq!(config.checksum_verification, true);
        assert_eq!(config.recovery_mode, RecoveryMode::Consistent);
        assert_eq!(config.retention_days, 30);
        assert_eq!(config.retention_check_interval_hours, 24);
        assert_eq!(config.default_query_limit, 1000);
        assert_eq!(config.parallel_query_execution, true);
        assert_eq!(config.collect_metrics, true);
        
        // Validate the default config
        assert!(config.validate().is_ok());
    }
    
    #[test]
    fn test_config_builder() {
        let config = StoreConfig::new()
            .with_max_segment_size(512 * 1024 * 1024)
            .with_compression_algorithm(CompressionAlgorithm::Lz4)
            .with_compression_level(6)
            .with_write_buffer_size(64 * 1024 * 1024)
            .with_cache_size_mb(1024)
            .with_retention_days(90)
            .with_shard_period(ShardPeriod::Month);
        
        // Check custom values
        assert_eq!(config.max_segment_size, 512 * 1024 * 1024);
        assert_eq!(config.compression_algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(config.compression_level, 6);
        assert_eq!(config.write_buffer_size, 64 * 1024 * 1024);
        assert_eq!(config.cache_size_mb, 1024);
        assert_eq!(config.retention_days, 90);
        assert_eq!(config.shard_period, Some(ShardPeriod::Month));
        
        // Validate the custom config
        assert!(config.validate().is_ok());
    }
    
    #[test]
    fn test_config_validation() {
        // Test invalid values
        let invalid_configs = vec![
            StoreConfig::new().with_max_segment_size(1024), // Too small
            StoreConfig::new().with_max_segments_per_shard(1), // Too few
            StoreConfig::new().with_bloom_filter_bits(2), // Too few bits
            StoreConfig::new().with_bloom_filter_bits(30), // Too many bits
            StoreConfig::new().with_write_buffer_size(1000), // Too small
            StoreConfig::new().with_compression_level(-1), // Out of range
            StoreConfig::new().with_compression_level(10), // Out of range
            StoreConfig::new().with_cache_size_mb(8), // Too small
            StoreConfig::new().with_compaction_threads(0), // Too few
            StoreConfig::new().with_retention_days(0), // Invalid
            StoreConfig::new().with_retention_check_interval_hours(0), // Invalid
            StoreConfig::new().with_default_query_limit(0), // Invalid
        ];
        
        for config in invalid_configs {
            assert!(config.validate().is_err());
        }
    }
    
    #[test]
    fn test_compression_algorithm() {
        // Test string conversion
        assert_eq!(CompressionAlgorithm::None.to_string(), "none");
        assert_eq!(CompressionAlgorithm::Lz4.to_string(), "lz4");
        assert_eq!(CompressionAlgorithm::Zstd.to_string(), "zstd");
        
        // Test parsing
        assert_eq!(CompressionAlgorithm::from_str("none").unwrap(), CompressionAlgorithm::None);
        assert_eq!(CompressionAlgorithm::from_str("lz4").unwrap(), CompressionAlgorithm::Lz4);
        assert_eq!(CompressionAlgorithm::from_str("zstd").unwrap(), CompressionAlgorithm::Zstd);
        assert!(CompressionAlgorithm::from_str("invalid").is_err());
        
        // Test name
        assert_eq!(CompressionAlgorithm::None.name(), "none");
        assert_eq!(CompressionAlgorithm::Lz4.name(), "lz4");
        assert_eq!(CompressionAlgorithm::Zstd.name(), "zstd");
        
        // Test is_enabled
        assert!(!CompressionAlgorithm::None.is_enabled());
        assert!(CompressionAlgorithm::Lz4.is_enabled());
        assert!(CompressionAlgorithm::Zstd.is_enabled());
    }
    
    #[test]
    fn test_recovery_mode() {
        // Test string conversion
        assert_eq!(RecoveryMode::BestEffort.to_string(), "best_effort");
        assert_eq!(RecoveryMode::Consistent.to_string(), "consistent");
        assert_eq!(RecoveryMode::Conservative.to_string(), "conservative");
        
        // Test parsing
        assert_eq!(RecoveryMode::from_str("best_effort").unwrap(), RecoveryMode::BestEffort);
        assert_eq!(RecoveryMode::from_str("consistent").unwrap(), RecoveryMode::Consistent);
        assert_eq!(RecoveryMode::from_str("conservative").unwrap(), RecoveryMode::Conservative);
        assert!(RecoveryMode::from_str("invalid").is_err());
        
        // Test name
        assert_eq!(RecoveryMode::BestEffort.name(), "best_effort");
        assert_eq!(RecoveryMode::Consistent.name(), "consistent");
        assert_eq!(RecoveryMode::Conservative.name(), "conservative");
    }
    
    #[test]
    fn test_config_pretty_string() {
        let config = StoreConfig::new();
        let pretty = config.to_string_pretty();
        
        // Basic check that it contains section headers
        assert!(pretty.contains("Storage Configuration:"));
        assert!(pretty.contains("Performance Tuning:"));
        assert!(pretty.contains("Reliability Settings:"));
        assert!(pretty.contains("Retention Policy:"));
        assert!(pretty.contains("Query Settings:"));
        assert!(pretty.contains("Additional Settings:"));
        
        // Check some specific values
        assert!(pretty.contains(&format!("Max Segment Size: {}", 256)));
        assert!(pretty.contains(&format!("Compression Algorithm: {}", CompressionAlgorithm::Zstd)));
        assert!(pretty.contains(&format!("Recovery Mode: {}", RecoveryMode::Consistent)));
    }
}