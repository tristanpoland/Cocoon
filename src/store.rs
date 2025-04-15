use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::collections::HashMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};

use chrysalis_rs::{LogEntry, Serializable};
use crate::error::{Result, Error};
use crate::config::{StoreConfig, RecoveryMode};
use crate::segment::SegmentManager;
use crate::shard::ShardManager;
use crate::index::IndexManager;
use crate::wal::WriteAheadLog;
use crate::query::{Query, QueryBuilder, TimeRange};
use crate::transaction::Transaction;
use crate::retention::RetentionManager;
use crate::metrics::MetricsCollector;
use crate::bloom::BloomFilterManager;

/// Statistics about a Cocoon store
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of logs stored
    pub total_logs: usize,
    /// Total size in bytes of the raw log data
    pub raw_size_bytes: usize,
    /// Total size in bytes of the compressed log data
    pub compressed_size_bytes: usize,
    /// Total size in bytes of the indices
    pub index_size_bytes: usize,
    /// Number of shards in the store
    pub shard_count: usize,
    /// Number of segments in the store
    pub segment_count: usize,
    /// Oldest log timestamp in the store
    pub oldest_timestamp: Option<DateTime<Utc>>,
    /// Newest log timestamp in the store
    pub newest_timestamp: Option<DateTime<Utc>>,
    /// Compression ratio (raw size / compressed size)
    pub compression_ratio: f64,
    /// Write operations count
    pub write_ops: usize,
    /// Read operations count
    pub read_ops: usize,
    /// Query operations count
    pub query_ops: usize,
    /// Compaction operations count
    pub compaction_ops: usize,
    /// Latest compaction duration in milliseconds
    pub last_compaction_duration_ms: u64,
    /// Cache hit rate (percentage)
    pub cache_hit_rate: f64,
}

impl StorageStats {
    /// Calculate the compression ratio
    pub fn calculate_compression_ratio(&mut self) {
        if self.compressed_size_bytes > 0 {
            self.compression_ratio = self.raw_size_bytes as f64 / self.compressed_size_bytes as f64;
        } else {
            self.compression_ratio = 1.0;
        }
    }
    
    /// Get the total storage size in bytes
    pub fn total_size_bytes(&self) -> usize {
        self.compressed_size_bytes + self.index_size_bytes
    }
}

/// The main storage engine for Cocoon
pub struct CocoonStore {
    /// Base directory for storage
    base_dir: PathBuf,
    /// Store configuration
    config: StoreConfig,
    /// Shard manager
    shard_manager: Arc<ShardManager>,
    /// Index manager
    index_manager: Arc<IndexManager>,
    /// Segment manager
    segment_manager: Arc<SegmentManager>,
    /// Write-ahead log
    wal: Arc<WriteAheadLog>,
    /// Bloom filter manager
    bloom_manager: Arc<BloomFilterManager>,
    /// Write buffer
    write_buffer: Arc<Mutex<Vec<LogEntry>>>,
    /// Write buffer size in bytes
    write_buffer_size: AtomicUsize,
    /// Retention manager
    retention_manager: Arc<RetentionManager>,
    /// Store is open flag
    is_open: Arc<RwLock<bool>>,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Background tasks handle
    #[allow(dead_code)]
    background_tasks: Option<BackgroundTasks>,
}

/// Background tasks handles
struct BackgroundTasks {
    compaction_handle: Option<std::thread::JoinHandle<()>>,
    retention_handle: Option<std::thread::JoinHandle<()>>,
    flush_handle: Option<std::thread::JoinHandle<()>>,
}

impl CocoonStore {
    /// Create a new store with default configuration
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let config = StoreConfig::new().with_directory(dir.as_ref());
        Self::with_config(dir, config)
    }
    
    /// Create a new store with custom configuration
    pub fn with_config<P: AsRef<Path>>(dir: P, config: StoreConfig) -> Result<Self> {
        // Validate configuration
        config.validate()?;
        
        // Create base directory if it doesn't exist
        let base_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).map_err(|e| {
            Error::Storage(format!("Failed to create directory {}: {}", base_dir.display(), e))
        })?;
        
        // Create store directories
        let wal_dir = base_dir.join("wal");
        let segments_dir = base_dir.join("segments");
        let index_dir = base_dir.join("index");
        let meta_dir = base_dir.join("meta");
        
        for dir in [&wal_dir, &segments_dir, &index_dir, &meta_dir] {
            fs::create_dir_all(dir).map_err(|e| {
                Error::Storage(format!("Failed to create directory {}: {}", dir.display(), e))
            })?;
        }
        
        // Initialize components
        let metrics = Arc::new(MetricsCollector::new());
        
        let segment_manager = Arc::new(SegmentManager::new(
            segments_dir,
            &config,
            metrics.clone(),
        )?);
        
        let index_manager = Arc::new(IndexManager::new(
            index_dir,
            segment_manager.clone(),
            &config,
            metrics.clone(),
        )?);
        
        let shard_manager = Arc::new(ShardManager::new(
            base_dir.clone(),
            segment_manager.clone(),
            index_manager.clone(),
            &config,
            metrics.clone(),
        )?);
        
        let wal = Arc::new(WriteAheadLog::new(
            wal_dir,
            &config,
            metrics.clone(),
        )?);
        
        let bloom_manager = Arc::new(BloomFilterManager::new(
            meta_dir.join("bloom"),
            &config,
            metrics.clone(),
        )?);
        
        let retention_manager = Arc::new(RetentionManager::new(
            shard_manager.clone(),
            &config,
            metrics.clone(),
        ));
        
        // Create store
        let mut store = Self {
            base_dir,
            config,
            shard_manager,
            index_manager,
            segment_manager,
            wal,
            bloom_manager,
            write_buffer: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            write_buffer_size: AtomicUsize::new(0),
            retention_manager,
            is_open: Arc::new(RwLock::new(true)),
            metrics,
            background_tasks: None,
        };
        
        // Recover if necessary
        store.recover()?;
        
        // Start background tasks
        store.start_background_tasks();
        
        Ok(store)
    }
    
    /// Store a log entry
    pub fn store<T: Serializable>(&self, entry: &T) -> Result<()> {
        self.ensure_open()?;
        
        // Serialize entry to LogEntry
        let log_entry = self.to_log_entry(entry)?;
        
        // Add to write buffer
        let entry_size = self.add_to_write_buffer(log_entry)?;
        
        // Update metrics
        self.metrics.increment_writes();
        self.metrics.add_bytes_written(entry_size);
        
        // Check if buffer should be flushed
        if self.write_buffer_size.load(Ordering::Relaxed) >= self.config.write_buffer_size {
            self.flush_internal(false)?;
        }
        
        Ok(())
    }
    
    /// Store multiple log entries in a batch
    pub fn store_batch<T: Serializable>(&self, entries: &[T]) -> Result<()> {
        self.ensure_open()?;
        
        if entries.is_empty() {
            return Ok(());
        }
        
        // Start a transaction for the batch
        let mut transaction = self.begin_transaction()?;
        
        for entry in entries {
            let log_entry = self.to_log_entry(entry)?;
            transaction.store(log_entry)?;
        }
        
        // Commit the transaction
        transaction.commit()
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<Transaction> {
        self.ensure_open()?;
        
        Ok(Transaction::new(
            self.wal.clone(),
            self.shard_manager.clone(),
            self.index_manager.clone(),
            self.bloom_manager.clone(),
            self.config.clone(),
            self.metrics.clone(),
        ))
    }
    
    /// Create a query builder
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new(
            self.shard_manager.clone(),
            self.index_manager.clone(),
            self.bloom_manager.clone(),
            self.metrics.clone(),
        )
    }
    
    /// Execute a query
    pub fn execute_query(&self, query: Query) -> Result<Vec<LogEntry>> {
        self.ensure_open()?;
        
        // Flush before querying to ensure all data is visible
        self.flush_internal(false)?;
        
        // Execute the query
        let start = Instant::now();
        let results = query.execute()?;
        let duration = start.elapsed();
        
        // Update metrics
        self.metrics.increment_queries();
        self.metrics.record_query_duration(duration);
        
        Ok(results)
    }
    
    /// Flush the write buffer to disk
    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        self.flush_internal(true)
    }
    
    /// Get statistics about the store
    pub fn stats(&self) -> Result<StorageStats> {
        self.ensure_open()?;
        
        let mut stats = StorageStats::default();
        
        // Get shard stats
        let shard_stats = self.shard_manager.stats()?;
        stats.shard_count = shard_stats.shard_count;
        stats.segment_count = shard_stats.segment_count;
        stats.total_logs = shard_stats.total_logs;
        stats.raw_size_bytes = shard_stats.raw_size_bytes;
        stats.compressed_size_bytes = shard_stats.compressed_size_bytes;
        
        // Get index stats
        let index_stats = self.index_manager.stats()?;
        stats.index_size_bytes = index_stats.size_bytes;
        
        // Get timestamp range
        let timestamp_range = self.shard_manager.timestamp_range()?;
        stats.oldest_timestamp = timestamp_range.0;
        stats.newest_timestamp = timestamp_range.1;
        
        // Get metrics
        stats.write_ops = self.metrics.get_write_count();
        stats.read_ops = self.metrics.get_read_count();
        stats.query_ops = self.metrics.get_query_count();
        stats.compaction_ops = self.metrics.get_compaction_count();
        stats.last_compaction_duration_ms = self.metrics.get_last_compaction_duration().as_millis() as u64;
        stats.cache_hit_rate = self.metrics.get_cache_hit_rate();
        
        // Calculate compression ratio
        stats.calculate_compression_ratio();
        
        Ok(stats)
    }
    
    /// Run compaction on all shards
    pub fn compact(&self) -> Result<()> {
        self.ensure_open()?;
        
        // Flush before compaction
        self.flush_internal(true)?;
        
        // Perform compaction
        let start = Instant::now();
        self.shard_manager.compact_all()?;
        let duration = start.elapsed();
        
        // Update metrics
        self.metrics.increment_compactions();
        self.metrics.record_compaction_duration(duration);
        
        Ok(())
    }
    
    /// Delete logs before a specific time
    pub fn delete_before(&self, timestamp: DateTime<Utc>) -> Result<usize> {
        self.ensure_open()?;
        
        // Flush before deletion
        self.flush_internal(true)?;
        
        // Perform deletion
        let count = self.shard_manager.delete_before(timestamp)?;
        
        // Update metrics if logs were deleted
        if count > 0 {
            self.metrics.add_deleted_logs(count);
        }
        
        Ok(count)
    }
    
    /// Close the store gracefully
    pub fn close(&self) -> Result<()> {
        // Check if already closed
        {
            let mut is_open = self.is_open.write();
            if !*is_open {
                return Ok(());
            }
            *is_open = false;
        }
        
        // Flush any pending writes
        self.flush_internal(true)?;
        
        // Close components
        self.wal.close()?;
        self.segment_manager.close()?;
        self.index_manager.close()?;
        self.shard_manager.close()?;
        
        Ok(())
    }
    
    /// Check if the store is open
    pub fn is_open(&self) -> bool {
        *self.is_open.read()
    }
    
    /// Get the store's base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
    
    /// Get the store's configuration
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }
    
    // Internal methods
    
    /// Ensure the store is open
    fn ensure_open(&self) -> Result<()> {
        if !self.is_open() {
            return Err(Error::Storage("Store is closed".to_string()));
        }
        Ok(())
    }
    
    /// Add a log entry to the write buffer
    fn add_to_write_buffer(&self, entry: LogEntry) -> Result<usize> {
        // Serialize the entry to calculate its size
        let serialized = serde_json::to_vec(&entry).map_err(Error::Serialization)?;
        let entry_size = serialized.len();
        
        // Add to write buffer
        {
            let mut buffer = self.write_buffer.lock();
            buffer.push(entry);
        }
        
        // Update buffer size
        self.write_buffer_size.fetch_add(entry_size, Ordering::Relaxed);
        
        Ok(entry_size)
    }
    
    /// Flush the write buffer to disk
    fn flush_internal(&self, force: bool) -> Result<()> {
        // Check if there's anything to flush
        if !force && self.write_buffer_size.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }
        
        // Take the current buffer
        let entries = {
            let mut buffer = self.write_buffer.lock();
            if buffer.is_empty() {
                return Ok(());
            }
            
            // Reset buffer
            let entries = std::mem::take(&mut *buffer);
            self.write_buffer_size.store(0, Ordering::Relaxed);
            entries
        };
        
        // Begin transaction
        let mut transaction = self.begin_transaction()?;
        
        // Add entries to transaction
        for entry in entries {
            transaction.store(entry)?;
        }
        
        // Commit transaction
        transaction.commit()
    }
    
    /// Convert a serializable entry to a LogEntry
    fn to_log_entry<T: Serializable>(&self, entry: &T) -> Result<LogEntry> {
        // If it's already a LogEntry, just clone it
        if let Some(log_entry) = self.try_as_log_entry(entry) {
            return Ok(log_entry);
        }
        
        // Otherwise, deserialize from JSON
        let json = entry.to_json()?;
        serde_json::from_str(&json).map_err(Error::Serialization)
    }
    
    /// Try to cast a serializable to a LogEntry
    fn try_as_log_entry<T: Serializable>(&self, entry: &T) -> Option<LogEntry> {
        if let Some(log_entry) = entry.downcast_ref::<LogEntry>() {
            return Some(log_entry.clone());
        }
        None
    }
    
    /// Recover the store from WAL after a crash
    fn recover(&self) -> Result<()> {
        // Check if recovery is needed
        if !self.wal.needs_recovery() {
            return Ok(());
        }
        
        println!("Recovering store from WAL...");
        let start = Instant::now();
        
        // Perform recovery based on configured mode
        let recovered = match self.config.recovery_mode {
            RecoveryMode::BestEffort => self.wal.recover_best_effort()?,
            RecoveryMode::Consistent => self.wal.recover_consistent()?,
            RecoveryMode::Conservative => self.wal.recover_conservative()?,
        };
        
        let duration = start.elapsed();
        println!("Recovery completed in {:?}, recovered {} entries", duration, recovered.len());
        
        // Re-apply recovered entries
        if !recovered.is_empty() {
            let mut transaction = self.begin_transaction()?;
            
            for entry in recovered {
                transaction.store(entry)?;
            }
            
            transaction.commit()?;
        }
        
        // Clean up WAL
        self.wal.clear()?;
        
        Ok(())
    }
    
    /// Start background tasks for maintenance
    fn start_background_tasks(&mut self) {
        let is_open = self.is_open.clone();
        let shard_manager = self.shard_manager.clone();
        let retention_manager = self.retention_manager.clone();
        let store_arc = Arc::new(self.clone_internal());
        
        // Start compaction task
        let compaction_handle = Some(std::thread::spawn(move || {
            let interval = Duration::from_secs(3600); // 1 hour
            let mut last_run = Instant::now();
            
            while *is_open.read() {
                std::thread::sleep(Duration::from_secs(10));
                
                if last_run.elapsed() >= interval && *is_open.read() {
                    if let Err(e) = shard_manager.compact_all() {
                        eprintln!("Background compaction error: {}", e);
                    }
                    last_run = Instant::now();
                }
            }
        }));
        
        // Start retention task
        let is_open_clone = self.is_open.clone();
        let retention_handle = Some(std::thread::spawn(move || {
            let interval = Duration::from_secs(
                retention_manager.config().retention_check_interval_hours as u64 * 3600
            );
            let mut last_run = Instant::now();
            
            while *is_open_clone.read() {
                std::thread::sleep(Duration::from_secs(10));
                
                if last_run.elapsed() >= interval && *is_open_clone.read() {
                    if let Err(e) = retention_manager.enforce_retention_policy() {
                        eprintln!("Background retention error: {}", e);
                    }
                    last_run = Instant::now();
                }
            }
        }));
        
        // Start periodic flush task
        let store = store_arc.clone();
        let is_open_clone = self.is_open.clone();
        let flush_handle = Some(std::thread::spawn(move || {
            let interval = Duration::from_secs(30); // 30 seconds
            
            while *is_open_clone.read() {
                std::thread::sleep(interval);
                
                if *is_open_clone.read() {
                    if let Err(e) = store.flush_internal(false) {
                        eprintln!("Background flush error: {}", e);
                    }
                }
            }
        }));
        
        self.background_tasks = Some(BackgroundTasks {
            compaction_handle,
            retention_handle,
            flush_handle,
        });
    }
    
    /// Internal clone method to enable background tasks
    fn clone_internal(&self) -> Self {
        Self {
            base_dir: self.base_dir.clone(),
            config: self.config.clone(),
            shard_manager: self.shard_manager.clone(),
            index_manager: self.index_manager.clone(),
            segment_manager: self.segment_manager.clone(),
            wal: self.wal.clone(),
            bloom_manager: self.bloom_manager.clone(),
            write_buffer: self.write_buffer.clone(),
            write_buffer_size: AtomicUsize::new(self.write_buffer_size.load(Ordering::Relaxed)),
            retention_manager: self.retention_manager.clone(),
            is_open: self.is_open.clone(),
            metrics: self.metrics.clone(),
            background_tasks: None,
        }
    }
}

impl Drop for CocoonStore {
    fn drop(&mut self) {
        if self.is_open() {
            if let Err(e) = self.close() {
                eprintln!("Error closing store: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use chrysalis_rs::{LogEntry, LogLevel};
    use std::thread;
    
    #[test]
    fn test_store_and_query() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        
        // Create a store
        let store = CocoonStore::new(temp_dir.path())?;
        
        // Create some log entries
        let mut entries = Vec::new();
        for i in 0..100 {
            let mut entry = LogEntry::new(format!("Test log {}", i), LogLevel::Info);
            entry.add_context("index", i)?;
            entries.push(entry);
        }
        
        // Store entries
        store.store_batch(&entries)?;
        
        // Query all logs
        let results = store.query().execute()?;
        assert_eq!(results.len(), 100);
        
        // Query logs with specific context
        let results = store.query()
            .with_context_value("index", 42)
            .execute()?;
        assert_eq!(results.len(), 1);
        
        Ok(())
    }
    
    #[test]
    fn test_concurrent_access() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        
        // Create a store
        let store = Arc::new(CocoonStore::new(temp_dir.path())?);
        
        // Spawn writer threads
        let mut handles = Vec::new();
        for t in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || -> Result<()> {
                for i in 0..25 {
                    let mut entry = LogEntry::new(
                        format!("Test log {} from thread {}", i, t), 
                        LogLevel::Info
                    );
                    entry.add_context("thread", t)?;
                    entry.add_context("index", i)?;
                    store.store(&entry)?;
                }
                Ok(())
            }));
        }
        
        // Wait for writers to finish
        for handle in handles {
            handle.join().unwrap()?;
        }
        
        // Flush to ensure all data is written
        store.flush()?;
        
        // Query all logs
        let results = store.query().execute()?;
        assert_eq!(results.len(), 100);
        
        Ok(())
    }
    
    #[test]
    fn test_recovery() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        
        // Create and populate a store
        {
            let store = CocoonStore::new(temp_dir.path())?;
            
            for i in 0..50 {
                let mut entry = LogEntry::new(format!("Test log {}", i), LogLevel::Info);
                entry.add_context("index", i)?;
                store.store(&entry)?;
            }
            
            // Do NOT flush - we want to simulate a crash
            
            // Force WAL to be written but don't complete the process
            let wal_path = temp_dir.path().join("wal");
            fs::create_dir_all(&wal_path)?;
            let wal_file = wal_path.join("current.wal");
            fs::write(wal_file, "DUMMY WAL DATA")?;
        }
        
        // Re-open the store, which should trigger recovery
        let store = CocoonStore::new(temp_dir.path())?;
        
        // Check that logs were recovered
        let results = store.query().execute()?;
        assert!(results.len() > 0);
        
        Ok(())
    }
}