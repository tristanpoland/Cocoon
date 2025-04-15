use std::sync::Arc;
use std::time::Instant;
use std::collections::HashSet;
use chrono::{DateTime, Utc, Duration};
use rayon::prelude::*;

use chrysalis_rs::{LogEntry, LogLevel};
use crate::error::{Result, Error};
use crate::index::{IndexManager, IndexEntry};
use crate::shard::ShardManager;
use crate::bloom::BloomFilterManager;
use crate::metrics::MetricsCollector;

/// Sort order for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending order (oldest first)
    Ascending,
    /// Descending order (newest first)
    Descending,
}

/// Time range for querying logs
#[derive(Debug, Clone)]
pub struct TimeRange {
    /// Start time (inclusive)
    pub start: DateTime<Utc>,
    /// End time (exclusive)
    pub end: DateTime<Utc>,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }
    
    /// Create a time range for the last N minutes
    pub fn last_minutes(minutes: i64) -> Self {
        let end = Utc::now();
        let start = end - Duration::minutes(minutes);
        Self { start, end }
    }
    
    /// Create a time range for the last N hours
    pub fn last_hours(hours: i64) -> Self {
        let end = Utc::now();
        let start = end - Duration::hours(hours);
        Self { start, end }
    }
    
    /// Create a time range for the last N days
    pub fn last_days(days: i64) -> Self {
        let end = Utc::now();
        let start = end - Duration::days(days);
        Self { start, end }
    }
    
    /// Create a time range for today
    pub fn today() -> Self {
        let now = Utc::now();
        let start = Utc.with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single().unwrap_or(now);
        Self { start, end: now }
    }
    
    /// Create a time range for yesterday
    pub fn yesterday() -> Self {
        let now = Utc::now();
        let today = Utc.with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single().unwrap_or(now);
        let yesterday = today - Duration::days(1);
        Self { start: yesterday, end: today }
    }
    
    /// Create a time range for the last 24 hours
    pub fn last_24_hours() -> Self {
        Self::last_hours(24)
    }
    
    /// Create a time range for the last 7 days
    pub fn last_7_days() -> Self {
        Self::last_days(7)
    }
    
    /// Create a time range for the last 30 days
    pub fn last_30_days() -> Self {
        Self::last_days(30)
    }
}

/// A query for searching logs
#[derive(Debug, Clone)]
pub struct Query {
    /// Time range to search
    pub time_range: Option<TimeRange>,
    /// Log level to filter by
    pub level: Option<LogLevel>,
    /// Minimum log level to filter by
    pub min_level: Option<LogLevel>,
    /// Text to search for in the message
    pub message_text: Option<String>,
    /// Context keys that must be present
    pub context_keys: Vec<String>,
    /// Context key-value pairs to match
    pub context_values: Vec<(String, String)>,
    /// Sort order for results
    pub sort_order: SortOrder,
    /// Maximum number of results to return
    pub limit: Option<usize>,
    /// Number of results to skip
    pub offset: Option<usize>,
    /// Whether to use Bloom filters for optimization
    pub use_bloom_filters: bool,
    /// Whether to use parallel processing
    pub parallel: bool,
    
    // Internal fields
    shard_manager: Arc<ShardManager>,
    index_manager: Arc<IndexManager>,
    bloom_manager: Arc<BloomFilterManager>,
    metrics: Arc<MetricsCollector>,
}

impl Query {
    /// Create a new query
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        time_range: Option<TimeRange>,
        level: Option<LogLevel>,
        min_level: Option<LogLevel>,
        message_text: Option<String>,
        context_keys: Vec<String>,
        context_values: Vec<(String, String)>,
        sort_order: SortOrder,
        limit: Option<usize>,
        offset: Option<usize>,
        use_bloom_filters: bool,
        parallel: bool,
        shard_manager: Arc<ShardManager>,
        index_manager: Arc<IndexManager>,
        bloom_manager: Arc<BloomFilterManager>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            time_range,
            level,
            min_level,
            message_text,
            context_keys,
            context_values,
            sort_order,
            limit,
            offset,
            use_bloom_filters,
            parallel,
            shard_manager,
            index_manager,
            bloom_manager,
            metrics,
        }
    }
    
    /// Execute the query and return the results
    pub fn execute(&self) -> Result<Vec<LogEntry>> {
        let query_start = Instant::now();
        
        // First, find matching index entries based on time range
        let index_entries = if let Some(ref time_range) = self.time_range {
            self.index_manager.find_in_time_range(&time_range.start, &time_range.end)?
        } else {
            // Without a time range, we'd need to scan all indices
            // This could be very expensive, so we'll just return an empty result
            return Err(Error::Query("Time range is required for queries".to_string()));
        };
        
        if index_entries.is_empty() {
            // No matching entries, return empty result
            return Ok(Vec::new());
        }
        
        // Group entries by segment for efficient retrieval
        let mut segment_entries: HashMap<u64, Vec<IndexEntry>> = HashMap::new();
        
        for entry in index_entries {
            segment_entries.entry(entry.segment_id)
                .or_insert_with(Vec::new)
                .push(entry);
        }
        
        // Optimize with Bloom filters if enabled
        if self.use_bloom_filters {
            // Filter segments that definitely don't contain matching entries
            if let Some(ref text) = self.message_text {
                segment_entries.retain(|&segment_id, _| {
                    self.bloom_manager.may_contain_text(segment_id, text)
                });
            }
            
            for (key, _) in &self.context_values {
                segment_entries.retain(|&segment_id, _| {
                    self.bloom_manager.may_contain_context_key(segment_id, key)
                });
            }
        }
        
        // Process segments in parallel or serially based on configuration
        let mut all_results = Vec::new();
        
        if self.parallel && segment_entries.len() > 1 {
            // Process segments in parallel
            let segment_results: Vec<Result<Vec<LogEntry>>> = segment_entries
                .par_iter()
                .map(|(&segment_id, entries)| {
                    self.process_segment(segment_id, entries)
                })
                .collect();
            
            // Combine results, checking for errors
            for result in segment_results {
                match result {
                    Ok(entries) => all_results.extend(entries),
                    Err(e) => return Err(e),
                }
            }
        } else {
            // Process segments serially
            for (&segment_id, entries) in &segment_entries {
                let segment_results = self.process_segment(segment_id, entries)?;
                all_results.extend(segment_results);
            }
        }
        
        // Apply sort order
        match self.sort_order {
            SortOrder::Ascending => {
                all_results.sort_by(|a, b| a.metadata.timestamp.cmp(&b.metadata.timestamp));
            },
            SortOrder::Descending => {
                all_results.sort_by(|a, b| b.metadata.timestamp.cmp(&a.metadata.timestamp));
            },
        }
        
        // Apply offset and limit
        let offset = self.offset.unwrap_or(0);
        let results = if offset < all_results.len() {
            if let Some(limit) = self.limit {
                all_results[offset..].iter()
                    .take(limit)
                    .cloned()
                    .collect()
            } else {
                all_results[offset..].to_vec()
            }
        } else {
            Vec::new()
        };
        
        // Update metrics
        let query_duration = query_start.elapsed();
        self.metrics.record_query_duration(query_duration);
        self.metrics.record_query_results(results.len());
        
        Ok(results)
    }
    
    /// Process a single segment and return matching entries
    fn process_segment(&self, segment_id: u64, entries: &[IndexEntry]) -> Result<Vec<LogEntry>> {
        // Read all the log entries from the segment
        let logs = self.shard_manager.read_segment_entries(segment_id, entries)?;
        
        // Filter logs based on query criteria
        let filtered: Vec<LogEntry> = logs.into_iter()
            .filter(|log| self.matches_filters(log))
            .collect();
        
        Ok(filtered)
    }
    
    /// Check if a log entry matches all the query filters
    fn matches_filters(&self, log: &LogEntry) -> bool {
        // Check log level
        if let Some(level) = self.level {
            if log.level != level {
                return false;
            }
        }
        
        // Check minimum log level
        if let Some(min_level) = self.min_level {
            if (log.level as u8) < (min_level as u8) {
                return false;
            }
        }
        
        // Check message text
        if let Some(ref text) = self.message_text {
            if !log.message.to_lowercase().contains(&text.to_lowercase()) {
                return false;
            }
        }
        
        // Check context keys
        for key in &self.context_keys {
            if !log.context.contains_key(key) {
                return false;
            }
        }
        
        // Check context values
        for (key, value) in &self.context_values {
            match log.context.get(key) {
                Some(val) => {
                    // Convert both to lowercase strings for case-insensitive comparison
                    let val_str = val.to_string().to_lowercase();
                    if !val_str.contains(&value.to_lowercase()) {
                        return false;
                    }
                },
                None => return false,
            }
        }
        
        // All filters passed
        true
    }
}

/// Helper struct to use standard HashMap in Query
use std::collections::HashMap;

/// Builder for creating queries
pub struct QueryBuilder {
    /// Time range to search
    time_range: Option<TimeRange>,
    /// Log level to filter by
    level: Option<LogLevel>,
    /// Minimum log level to filter by
    min_level: Option<LogLevel>,
    /// Text to search for in the message
    message_text: Option<String>,
    /// Context keys that must be present
    context_keys: Vec<String>,
    /// Context key-value pairs to match
    context_values: Vec<(String, String)>,
    /// Sort order for results
    sort_order: SortOrder,
    /// Maximum number of results to return
    limit: Option<usize>,
    /// Number of results to skip
    offset: Option<usize>,
    /// Whether to use Bloom filters for optimization
    use_bloom_filters: bool,
    /// Whether to use parallel processing
    parallel: bool,
    
    // Internal fields
    shard_manager: Arc<ShardManager>,
    index_manager: Arc<IndexManager>,
    bloom_manager: Arc<BloomFilterManager>,
    metrics: Arc<MetricsCollector>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub(crate) fn new(
        shard_manager: Arc<ShardManager>,
        index_manager: Arc<IndexManager>,
        bloom_manager: Arc<BloomFilterManager>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            time_range: None,
            level: None,
            min_level: None,
            message_text: None,
            context_keys: Vec::new(),
            context_values: Vec::new(),
            sort_order: SortOrder::Descending,
            limit: Some(100), // Default limit
            offset: None,
            use_bloom_filters: true,
            parallel: true,
            shard_manager,
            index_manager,
            bloom_manager,
            metrics,
        }
    }
    
    /// Set the time range
    pub fn in_time_range(mut self, range: TimeRange) -> Self {
        self.time_range = Some(range);
        self
    }
    
    /// Set time range to last N minutes
    pub fn in_last_minutes(mut self, minutes: i64) -> Self {
        self.time_range = Some(TimeRange::last_minutes(minutes));
        self
    }
    
    /// Set time range to last N hours
    pub fn in_last_hours(mut self, hours: i64) -> Self {
        self.time_range = Some(TimeRange::last_hours(hours));
        self
    }
    
    /// Set time range to last N days
    pub fn in_last_days(mut self, days: i64) -> Self {
        self.time_range = Some(TimeRange::last_days(days));
        self
    }
    
    /// Set time range to today
    pub fn today(mut self) -> Self {
        self.time_range = Some(TimeRange::today());
        self
    }
    
    /// Set time range to yesterday
    pub fn yesterday(mut self) -> Self {
        self.time_range = Some(TimeRange::yesterday());
        self
    }
    
    /// Set time range to last 24 hours
    pub fn in_last_24_hours(mut self) -> Self {
        self.time_range = Some(TimeRange::last_24_hours());
        self
    }
    
    /// Set time range to last 7 days
    pub fn in_last_7_days(mut self) -> Self {
        self.time_range = Some(TimeRange::last_7_days());
        self
    }
    
    /// Set time range to last 30 days
    pub fn in_last_30_days(mut self) -> Self {
        self.time_range = Some(TimeRange::last_30_days());
        self
    }
    
    /// Filter by exact log level
    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.level = Some(level);
        self
    }
    
    /// Filter by minimum log level (inclusive)
    pub fn with_min_level(mut self, level: LogLevel) -> Self {
        self.min_level = Some(level);
        self
    }
    
    /// Filter by error level logs (Error, Critical, Fatal)
    pub fn errors_only(mut self) -> Self {
        self.min_level = Some(LogLevel::Error);
        self
    }
    
    /// Filter by warning level or higher
    pub fn warnings_and_errors(mut self) -> Self {
        self.min_level = Some(LogLevel::Warn);
        self
    }
    
    /// Filter by text in message (case-insensitive)
    pub fn containing_text(mut self, text: impl Into<String>) -> Self {
        self.message_text = Some(text.into());
        self
    }
    
    /// Filter by presence of a context key
    pub fn with_context_key(mut self, key: impl Into<String>) -> Self {
        self.context_keys.push(key.into());
        self
    }
    
    /// Filter by context key-value pair (case-insensitive)
    pub fn with_context_value(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context_values.push((key.into(), value.into()));
        self
    }
    
    /// Set ascending sort order (oldest first)
    pub fn order_ascending(mut self) -> Self {
        self.sort_order = SortOrder::Ascending;
        self
    }
    
    /// Set descending sort order (newest first)
    pub fn order_descending(mut self) -> Self {
        self.sort_order = SortOrder::Descending;
        self
    }
    
    /// Set maximum number of results
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    
    /// Set number of results to skip
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
    
    /// Disable Bloom filter optimization
    pub fn disable_bloom_filters(mut self) -> Self {
        self.use_bloom_filters = false;
        self
    }
    
    /// Disable parallel processing
    pub fn disable_parallel(mut self) -> Self {
        self.parallel = false;
        self
    }
    
    /// Build the query
    pub fn build(self) -> Query {
        Query::new(
            self.time_range,
            self.level,
            self.min_level,
            self.message_text,
            self.context_keys,
            self.context_values,
            self.sort_order,
            self.limit,
            self.offset,
            self.use_bloom_filters,
            self.parallel,
            self.shard_manager,
            self.index_manager,
            self.bloom_manager,
            self.metrics,
        )
    }
    
    /// Execute the query
    pub fn execute(self) -> Result<Vec<LogEntry>> {
        self.build().execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrysalis_rs::{LogEntry, LogLevel};
    use tempfile::tempdir;
    use chrono::{TimeZone, Duration};
    use crate::config::StoreConfig;
    use crate::segment::SegmentManager;
    use crate::index::IndexManager;
    use crate::bloom::BloomFilterManager;
    use crate::metrics::MetricsCollector;
    
    // Helper function to create a log entry with specified timestamp and level
    fn create_test_log(message: &str, timestamp: DateTime<Utc>, level: LogLevel) -> Result<LogEntry> {
        let mut entry = LogEntry::new(message, level);
        entry.metadata.timestamp = timestamp;
        Ok(entry)
    }
    
    // Helper function to set up test components
    fn setup_test_environment() -> Result<(tempfile::TempDir, Arc<ShardManager>, Arc<IndexManager>, Arc<BloomFilterManager>, Arc<MetricsCollector>)> {
        let temp_dir = tempdir()?;
        let segments_dir = temp_dir.path().join("segments");
        let index_dir = temp_dir.path().join("index");
        let bloom_dir = temp_dir.path().join("bloom");
        
        std::fs::create_dir_all(&segments_dir)?;
        std::fs::create_dir_all(&index_dir)?;
        std::fs::create_dir_all(&bloom_dir)?;
        
        let config = StoreConfig::default();
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
        
        let bloom_manager = Arc::new(BloomFilterManager::new(
            bloom_dir,
            &config,
            metrics.clone(),
        )?);
        
        Ok((temp_dir, segment_manager, index_manager, bloom_manager, metrics))
    }
    
    #[test]
    fn test_time_range_creation() {
        let now = Utc::now();
        
        // Test last minutes
        let range = TimeRange::last_minutes(5);
        assert!(range.end >= now - Duration::seconds(1)); // Allow for slight test delay
        assert!((range.end - range.start).num_minutes() == 5);
        
        // Test last hours
        let range = TimeRange::last_hours(2);
        assert!(range.end >= now - Duration::seconds(1));
        assert!((range.end - range.start).num_hours() == 2);
        
        // Test last days
        let range = TimeRange::last_days(3);
        assert!(range.end >= now - Duration::seconds(1));
        assert!((range.end - range.start).num_days() == 3);
        
        // Test today
        let range = TimeRange::today();
        let today_start = Utc.with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single().unwrap();
        assert_eq!(range.start, today_start);
        assert!(range.end >= now - Duration::seconds(1));
        
        // Test yesterday
        let range = TimeRange::yesterday();
        let today_start = Utc.with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single().unwrap();
        let yesterday_start = today_start - Duration::days(1);
        assert_eq!(range.start, yesterday_start);
        assert_eq!(range.end, today_start);
    }
    
    #[test]
    fn test_query_builder() -> Result<()> {
        let (_temp_dir, shard_manager, index_manager, bloom_manager, metrics) = setup_test_environment()?;
        
        // Create a query builder
        let builder = QueryBuilder::new(
            shard_manager.clone(),
            index_manager.clone(),
            bloom_manager.clone(),
            metrics.clone(),
        );
        
        // Test builder methods
        let query = builder
            .in_last_24_hours()
            .with_level(LogLevel::Error)
            .containing_text("error")
            .with_context_key("user_id")
            .with_context_value("status", "failed")
            .limit(10)
            .offset(5)
            .order_ascending()
            .build();
        
        // Verify query configuration
        assert!(query.time_range.is_some());
        assert_eq!(query.level, Some(LogLevel::Error));
        assert_eq!(query.message_text, Some("error".to_string()));
        assert_eq!(query.context_keys, vec!["user_id".to_string()]);
        assert_eq!(query.context_values, vec![("status".to_string(), "failed".to_string())]);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(5));
        assert_eq!(query.sort_order, SortOrder::Ascending);
        
        Ok(())
    }
    
    #[test]
    fn test_query_filters() {
        // Create sample log entries
        let now = Utc::now();
        let log1 = LogEntry::new("Error occurred", LogLevel::Error);
        let log2 = LogEntry::new("Warning message", LogLevel::Warn);
        let log3 = LogEntry::new("Info message", LogLevel::Info);
        
        // Create a query with filters
        let query = Query::new(
            Some(TimeRange::new(now - Duration::hours(1), now + Duration::hours(1))),
            Some(LogLevel::Error),
            None,
            Some("error".to_string()),
            vec![],
            vec![],
            SortOrder::Descending,
            None,
            None,
            true,
            true,
            Arc::new(ShardManager::default()),
            Arc::new(IndexManager::default()),
            Arc::new(BloomFilterManager::default()),
            Arc::new(MetricsCollector::new()),
        );
        
        // Test filter matching
        assert!(query.matches_filters(&log1)); // Should match
        assert!(!query.matches_filters(&log2)); // Wrong level
        assert!(!query.matches_filters(&log3)); // Wrong level and text
        
        // Test with min_level filter
        let query = Query::new(
            Some(TimeRange::new(now - Duration::hours(1), now + Duration::hours(1))),
            None,
            Some(LogLevel::Warn),
            None,
            vec![],
            vec![],
            SortOrder::Descending,
            None,
            None,
            true,
            true,
            Arc::new(ShardManager::default()),
            Arc::new(IndexManager::default()),
            Arc::new(BloomFilterManager::default()),
            Arc::new(MetricsCollector::new()),
        );
        
        assert!(query.matches_filters(&log1)); // Error >= Warn
        assert!(query.matches_filters(&log2)); // Warn >= Warn
        assert!(!query.matches_filters(&log3)); // Info < Warn
    }
}