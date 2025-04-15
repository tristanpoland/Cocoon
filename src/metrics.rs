use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use parking_lot::Mutex;

/// Performance metrics collector for Cocoon
#[derive(Debug)]
pub struct MetricsCollector {
    // Operation counts
    /// Number of write operations
    write_count: AtomicUsize,
    /// Number of read operations
    read_count: AtomicUsize,
    /// Number of query operations
    query_count: AtomicUsize,
    /// Number of compaction operations
    compaction_count: AtomicUsize,
    /// Number of flush operations
    flush_count: AtomicUsize,
    
    // Data metrics
    /// Total bytes written
    bytes_written: AtomicUsize,
    /// Total bytes read
    bytes_read: AtomicUsize,
    /// Number of log entries in storage
    entry_count: AtomicUsize,
    /// Number of log entries deleted
    deleted_entries: AtomicUsize,
    /// Number of index entries
    index_entry_count: AtomicUsize,
    
    // Timing metrics
    /// Total write duration in nanoseconds
    write_duration_ns: AtomicU64,
    /// Total read duration in nanoseconds
    read_duration_ns: AtomicU64,
    /// Total query duration in nanoseconds
    query_duration_ns: AtomicU64,
    /// Total compaction duration in nanoseconds
    compaction_duration_ns: AtomicU64,
    /// Last compaction duration
    last_compaction_duration: Mutex<Duration>,
    /// Total flush duration in nanoseconds
    flush_duration_ns: AtomicU64,
    /// Total WAL write duration in nanoseconds
    wal_write_duration_ns: AtomicU64,
    /// Total WAL sync duration in nanoseconds
    wal_sync_duration_ns: AtomicU64,
    /// Total index access duration in nanoseconds
    index_query_duration_ns: AtomicU64,
    
    // Cache metrics
    /// Number of cache hits
    cache_hits: AtomicUsize,
    /// Number of cache misses
    cache_misses: AtomicUsize,
    
    // Query metrics
    /// Number of queries that used bloom filters
    bloom_filter_queries: AtomicUsize,
    /// Number of queries that benefited from bloom filters
    bloom_filter_hits: AtomicUsize,
    /// Total number of query results returned
    query_result_count: AtomicUsize,
    
    // WAL metrics
    /// Total bytes written to WAL
    wal_bytes_written: AtomicUsize,
    
    // Internal state
    /// Start time of the metrics collector
    start_time: Instant,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            // Operation counts
            write_count: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
            query_count: AtomicUsize::new(0),
            compaction_count: AtomicUsize::new(0),
            flush_count: AtomicUsize::new(0),
            
            // Data metrics
            bytes_written: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
            entry_count: AtomicUsize::new(0),
            deleted_entries: AtomicUsize::new(0),
            index_entry_count: AtomicUsize::new(0),
            
            // Timing metrics
            write_duration_ns: AtomicU64::new(0),
            read_duration_ns: AtomicU64::new(0),
            query_duration_ns: AtomicU64::new(0),
            compaction_duration_ns: AtomicU64::new(0),
            last_compaction_duration: Mutex::new(Duration::from_secs(0)),
            flush_duration_ns: AtomicU64::new(0),
            wal_write_duration_ns: AtomicU64::new(0),
            wal_sync_duration_ns: AtomicU64::new(0),
            index_query_duration_ns: AtomicU64::new(0),
            
            // Cache metrics
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            
            // Query metrics
            bloom_filter_queries: AtomicUsize::new(0),
            bloom_filter_hits: AtomicUsize::new(0),
            query_result_count: AtomicUsize::new(0),
            
            // WAL metrics
            wal_bytes_written: AtomicUsize::new(0),
            
            // Internal state
            start_time: Instant::now(),
        }
    }
    
    // Operation count methods
    
    /// Increment write count
    pub fn increment_writes(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment read count
    pub fn increment_reads(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment query count
    pub fn increment_queries(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment compaction count
    pub fn increment_compactions(&self) {
        self.compaction_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment flush count
    pub fn increment_flushes(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }
    
    // Data metrics methods
    
    /// Add bytes written
    pub fn add_bytes_written(&self, bytes: usize) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Add bytes read
    pub fn add_bytes_read(&self, bytes: usize) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Update entry count
    pub fn set_entry_count(&self, count: usize) {
        self.entry_count.store(count, Ordering::Relaxed);
    }
    
    /// Add entries
    pub fn add_entries(&self, count: usize) {
        self.entry_count.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Add deleted entries
    pub fn add_deleted_logs(&self, count: usize) {
        self.deleted_entries.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment index entries
    pub fn increment_index_entries(&self) {
        self.index_entry_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Remove index entries
    pub fn remove_index_entries(&self, count: usize) {
        self.index_entry_count.fetch_sub(count, Ordering::Relaxed);
    }
    
    // Timing metrics methods
    
    /// Record a write operation duration
    pub fn record_write_duration(&self, duration: Duration) {
        self.write_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a read operation duration
    pub fn record_read_duration(&self, duration: Duration) {
        self.read_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a query operation duration
    pub fn record_query_duration(&self, duration: Duration) {
        self.query_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a compaction operation duration
    pub fn record_compaction_duration(&self, duration: Duration) {
        self.compaction_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        *self.last_compaction_duration.lock() = duration;
    }
    
    /// Record a flush operation duration
    pub fn record_flush_duration(&self, duration: Duration) {
        self.flush_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a WAL write operation duration
    pub fn record_wal_write_duration(&self, duration: Duration) {
        self.wal_write_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a WAL sync operation duration
    pub fn record_wal_sync_duration(&self, duration: Duration) {
        self.wal_sync_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record an index query operation duration
    pub fn record_index_query_duration(&self, duration: Duration) {
        self.index_query_duration_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    // Cache metrics methods
    
    /// Increment cache hits
    pub fn increment_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment cache misses
    pub fn increment_cache_misses(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    // Query metrics methods
    
    /// Record bloom filter query
    pub fn record_bloom_filter_query(&self) {
        self.bloom_filter_queries.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record bloom filter hit
    pub fn record_bloom_filter_hit(&self) {
        self.bloom_filter_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record query results
    pub fn record_query_results(&self, count: usize) {
        self.query_result_count.fetch_add(count, Ordering::Relaxed);
    }
    
    // WAL metrics methods
    
    /// Add WAL bytes written
    pub fn add_wal_bytes_written(&self, bytes: usize) {
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // Getters
    
    /// Get number of write operations
    pub fn get_write_count(&self) -> usize {
        self.write_count.load(Ordering::Relaxed)
    }
    
    /// Get number of read operations
    pub fn get_read_count(&self) -> usize {
        self.read_count.load(Ordering::Relaxed)
    }
    
    /// Get number of query operations
    pub fn get_query_count(&self) -> usize {
        self.query_count.load(Ordering::Relaxed)
    }
    
    /// Get number of compaction operations
    pub fn get_compaction_count(&self) -> usize {
        self.compaction_count.load(Ordering::Relaxed)
    }
    
    /// Get number of flush operations
    pub fn get_flush_count(&self) -> usize {
        self.flush_count.load(Ordering::Relaxed)
    }
    
    /// Get total bytes written
    pub fn get_bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }
    
    /// Get total bytes read
    pub fn get_bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }
    
    /// Get entry count
    pub fn get_entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }
    
    /// Get deleted entries count
    pub fn get_deleted_count(&self) -> usize {
        self.deleted_entries.load(Ordering::Relaxed)
    }
    
    /// Get index entry count
    pub fn get_index_entry_count(&self) -> usize {
        self.index_entry_count.load(Ordering::Relaxed)
    }
    
    /// Get total write duration
    pub fn get_write_duration(&self) -> Duration {
        Duration::from_nanos(self.write_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total read duration
    pub fn get_read_duration(&self) -> Duration {
        Duration::from_nanos(self.read_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total query duration
    pub fn get_query_duration(&self) -> Duration {
        Duration::from_nanos(self.query_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total compaction duration
    pub fn get_compaction_duration(&self) -> Duration {
        Duration::from_nanos(self.compaction_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get last compaction duration
    pub fn get_last_compaction_duration(&self) -> Duration {
        *self.last_compaction_duration.lock()
    }
    
    /// Get total flush duration
    pub fn get_flush_duration(&self) -> Duration {
        Duration::from_nanos(self.flush_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total WAL write duration
    pub fn get_wal_write_duration(&self) -> Duration {
        Duration::from_nanos(self.wal_write_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total WAL sync duration
    pub fn get_wal_sync_duration(&self) -> Duration {
        Duration::from_nanos(self.wal_sync_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get total index query duration
    pub fn get_index_query_duration(&self) -> Duration {
        Duration::from_nanos(self.index_query_duration_ns.load(Ordering::Relaxed))
    }
    
    /// Get cache hit rate (0.0 - 1.0)
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        
        if hits + misses == 0 {
            return 0.0;
        }
        
        hits as f64 / (hits + misses) as f64
    }
    
    /// Get bloom filter hit rate (0.0 - 1.0)
    pub fn get_bloom_filter_hit_rate(&self) -> f64 {
        let queries = self.bloom_filter_queries.load(Ordering::Relaxed);
        let hits = self.bloom_filter_hits.load(Ordering::Relaxed);
        
        if queries == 0 {
            return 0.0;
        }
        
        hits as f64 / queries as f64
    }
    
    /// Get average query result count
    pub fn get_avg_query_results(&self) -> f64 {
        let queries = self.query_count.load(Ordering::Relaxed);
        let results = self.query_result_count.load(Ordering::Relaxed);
        
        if queries == 0 {
            return 0.0;
        }
        
        results as f64 / queries as f64
    }
    
    /// Get total WAL bytes written
    pub fn get_wal_bytes_written(&self) -> usize {
        self.wal_bytes_written.load(Ordering::Relaxed)
    }
    
    /// Get uptime of the metrics collector
    pub fn get_uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    /// Reset all metrics
    pub fn reset(&self) {
        self.write_count.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.query_count.store(0, Ordering::Relaxed);
        self.compaction_count.store(0, Ordering::Relaxed);
        self.flush_count.store(0, Ordering::Relaxed);
        
        self.bytes_written.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        // Don't reset entry_count - it's a current state, not a metric
        self.deleted_entries.store(0, Ordering::Relaxed);
        // Don't reset index_entry_count - it's a current state, not a metric
        
        self.write_duration_ns.store(0, Ordering::Relaxed);
        self.read_duration_ns.store(0, Ordering::Relaxed);
        self.query_duration_ns.store(0, Ordering::Relaxed);
        self.compaction_duration_ns.store(0, Ordering::Relaxed);
        *self.last_compaction_duration.lock() = Duration::from_secs(0);
        self.flush_duration_ns.store(0, Ordering::Relaxed);
        self.wal_write_duration_ns.store(0, Ordering::Relaxed);
        self.wal_sync_duration_ns.store(0, Ordering::Relaxed);
        self.index_query_duration_ns.store(0, Ordering::Relaxed);
        
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        
        self.bloom_filter_queries.store(0, Ordering::Relaxed);
        self.bloom_filter_hits.store(0, Ordering::Relaxed);
        self.query_result_count.store(0, Ordering::Relaxed);
        
        self.wal_bytes_written.store(0, Ordering::Relaxed);
    }
    
    /// Get a report of all metrics
    pub fn get_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("=== Cocoon Metrics Report ===\n\n");
        
        // Uptime
        let uptime = self.get_uptime();
        report.push_str(&format!("Uptime: {:?}\n\n", uptime));
        
        // Operation counts
        report.push_str("Operation Counts:\n");
        report.push_str(&format!("  Writes: {}\n", self.get_write_count()));
        report.push_str(&format!("  Reads: {}\n", self.get_read_count()));
        report.push_str(&format!("  Queries: {}\n", self.get_query_count()));
        report.push_str(&format!("  Compactions: {}\n", self.get_compaction_count()));
        report.push_str(&format!("  Flushes: {}\n\n", self.get_flush_count()));
        
        // Data metrics
        report.push_str("Data Metrics:\n");
        report.push_str(&format!("  Bytes Written: {}\n", self.get_bytes_written()));
        report.push_str(&format!("  Bytes Read: {}\n", self.get_bytes_read()));
        report.push_str(&format!("  Entries: {}\n", self.get_entry_count()));
        report.push_str(&format!("  Deleted Entries: {}\n", self.get_deleted_count()));
        report.push_str(&format!("  Index Entries: {}\n\n", self.get_index_entry_count()));
        
        // Performance metrics
        report.push_str("Performance Metrics:\n");
        if self.get_write_count() > 0 {
            let avg_write = self.get_write_duration().as_micros() / self.get_write_count() as u128;
            report.push_str(&format!("  Avg. Write Time: {}µs\n", avg_write));
        }
        if self.get_read_count() > 0 {
            let avg_read = self.get_read_duration().as_micros() / self.get_read_count() as u128;
            report.push_str(&format!("  Avg. Read Time: {}µs\n", avg_read));
        }
        if self.get_query_count() > 0 {
            let avg_query = self.get_query_duration().as_micros() / self.get_query_count() as u128;
            report.push_str(&format!("  Avg. Query Time: {}µs\n", avg_query));
        }
        if self.get_compaction_count() > 0 {
            let avg_compaction = self.get_compaction_duration().as_micros() / self.get_compaction_count() as u128;
            report.push_str(&format!("  Avg. Compaction Time: {}µs\n", avg_compaction));
        }
        report.push_str(&format!("  Last Compaction Time: {:?}\n", self.get_last_compaction_duration()));
        report.push_str(&format!("  Cache Hit Rate: {:.2}%\n", self.get_cache_hit_rate() * 100.0));
        report.push_str(&format!("  Bloom Filter Hit Rate: {:.2}%\n\n", self.get_bloom_filter_hit_rate() * 100.0));
        
        // Query metrics
        report.push_str("Query Metrics:\n");
        report.push_str(&format!("  Avg. Results per Query: {:.2}\n\n", self.get_avg_query_results()));
        
        // WAL metrics
        report.push_str("WAL Metrics:\n");
        report.push_str(&format!("  Bytes Written: {}\n", self.get_wal_bytes_written()));
        
        // Throughput metrics
        let uptime_secs = uptime.as_secs_f64();
        if uptime_secs > 0.0 {
            let write_throughput = self.get_write_count() as f64 / uptime_secs;
            let read_throughput = self.get_read_count() as f64 / uptime_secs;
            let write_bytes_throughput = self.get_bytes_written() as f64 / uptime_secs;
            let read_bytes_throughput = self.get_bytes_read() as f64 / uptime_secs;
            
            report.push_str("\nThroughput Metrics:\n");
            report.push_str(&format!("  Writes/sec: {:.2}\n", write_throughput));
            report.push_str(&format!("  Reads/sec: {:.2}\n", read_throughput));
            report.push_str(&format!("  Write Bytes/sec: {:.2}\n", write_bytes_throughput));
            report.push_str(&format!("  Read Bytes/sec: {:.2}\n", read_bytes_throughput));
        }
        
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[test]
    fn test_metrics_basic_recording() {
        let metrics = MetricsCollector::new();
        
        // Record operations
        metrics.increment_writes();
        metrics.increment_reads();
        metrics.increment_queries();
        metrics.increment_compactions();
        metrics.increment_flushes();
        
        // Verify counts
        assert_eq!(metrics.get_write_count(), 1);
        assert_eq!(metrics.get_read_count(), 1);
        assert_eq!(metrics.get_query_count(), 1);
        assert_eq!(metrics.get_compaction_count(), 1);
        assert_eq!(metrics.get_flush_count(), 1);
    }
    
    #[test]
    fn test_metrics_data_recording() {
        let metrics = MetricsCollector::new();
        
        // Record data metrics
        metrics.add_bytes_written(1000);
        metrics.add_bytes_read(500);
        metrics.set_entry_count(100);
        metrics.add_deleted_logs(10);
        metrics.increment_index_entries();
        metrics.increment_index_entries();
        
        // Verify data metrics
        assert_eq!(metrics.get_bytes_written(), 1000);
        assert_eq!(metrics.get_bytes_read(), 500);
        assert_eq!(metrics.get_entry_count(), 100);
        assert_eq!(metrics.get_deleted_count(), 10);
        assert_eq!(metrics.get_index_entry_count(), 2);
    }
    
    #[test]
    fn test_metrics_timing_recording() {
        let metrics = MetricsCollector::new();
        
        // Record timing metrics
        let duration = Duration::from_millis(100);
        metrics.record_write_duration(duration);
        metrics.record_read_duration(duration);
        metrics.record_query_duration(duration);
        metrics.record_compaction_duration(duration);
        metrics.record_flush_duration(duration);
        
        // Verify timing metrics
        assert_eq!(metrics.get_write_duration(), duration);
        assert_eq!(metrics.get_read_duration(), duration);
        assert_eq!(metrics.get_query_duration(), duration);
        assert_eq!(metrics.get_compaction_duration(), duration);
        assert_eq!(metrics.get_last_compaction_duration(), duration);
        assert_eq!(metrics.get_flush_duration(), duration);
    }
    
    #[test]
    fn test_metrics_cache_recording() {
        let metrics = MetricsCollector::new();
        
        // Record cache metrics
        for _ in 0..75 {
            metrics.increment_cache_hits();
        }
        
        for _ in 0..25 {
            metrics.increment_cache_misses();
        }
        
        // Verify cache hit rate
        assert_eq!(metrics.get_cache_hit_rate(), 0.75);
    }
    
    #[test]
    fn test_metrics_bloom_filter_recording() {
        let metrics = MetricsCollector::new();
        
        // Record bloom filter metrics
        for _ in 0..100 {
            metrics.record_bloom_filter_query();
        }
        
        for _ in 0..80 {
            metrics.record_bloom_filter_hit();
        }
        
        // Verify bloom filter hit rate
        assert_eq!(metrics.get_bloom_filter_hit_rate(), 0.8);
    }
    
    #[test]
    fn test_metrics_query_results_recording() {
        let metrics = MetricsCollector::new();
        
        // Record query metrics
        metrics.increment_queries();
        metrics.record_query_results(10);
        
        metrics.increment_queries();
        metrics.record_query_results(20);
        
        // Verify average query results
        assert_eq!(metrics.get_avg_query_results(), 15.0);
    }
    
    #[test]
    fn test_metrics_wal_recording() {
        let metrics = MetricsCollector::new();
        
        // Record WAL metrics
        metrics.add_wal_bytes_written(1000);
        metrics.record_wal_write_duration(Duration::from_millis(50));
        metrics.record_wal_sync_duration(Duration::from_millis(20));
        
        // Verify WAL metrics
        assert_eq!(metrics.get_wal_bytes_written(), 1000);
        assert_eq!(metrics.get_wal_write_duration(), Duration::from_millis(50));
        assert_eq!(metrics.get_wal_sync_duration(), Duration::from_millis(20));
    }
    
    #[test]
    fn test_metrics_report() {
        let metrics = MetricsCollector::new();
        
        // Record some metrics
        metrics.increment_writes();
        metrics.add_bytes_written(1000);
        metrics.increment_cache_hits();
        metrics.increment_cache_misses();
        
        // Get report
        let report = metrics.get_report();
        
        // Verify report is non-empty and contains the section headings
        assert!(!report.is_empty());
        assert!(report.contains("Operation Counts:"));
        assert!(report.contains("Data Metrics:"));
        assert!(report.contains("Performance Metrics:"));
    }
    
    #[test]
    fn test_metrics_reset() {
        let metrics = MetricsCollector::new();
        
        // Record some metrics
        metrics.increment_writes();
        metrics.add_bytes_written(1000);
        metrics.set_entry_count(100);
        
        // Reset metrics
        metrics.reset();
        
        // Verify reset
        assert_eq!(metrics.get_write_count(), 0);
        assert_eq!(metrics.get_bytes_written(), 0);
        // Entry count should not be reset
        assert_eq!(metrics.get_entry_count(), 100);
    }
    
    #[test]
    fn test_metrics_thread_safety() {
        let metrics = Arc::new(MetricsCollector::new());
        
        // Spawn threads to increment writes
        let mut handles = Vec::new();
        
        for _ in 0..10 {
            let metrics_clone = metrics.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    metrics_clone.increment_writes();
                    metrics_clone.add_bytes_written(10);
                }
            }));
        }
        
        // Wait for threads to finish
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify counts
        assert_eq!(metrics.get_write_count(), 1000);
        assert_eq!(metrics.get_bytes_written(), 10000);
    }
}