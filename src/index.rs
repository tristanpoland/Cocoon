use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc, TimeZone, Datelike, Timelike};
use parking_lot::{Mutex, RwLock};
use bincode::{serialize, deserialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use uuid::Uuid;
use rayon::prelude::*;

use chrysalis_rs::LogEntry;
use crate::error::{Result, Error};
use crate::config::StoreConfig;
use crate::segment::SegmentManager;
use crate::metrics::MetricsCollector;

/// Magic bytes to identify index files
const INDEX_MAGIC: &[u8; 4] = b"CCNI"; // "Cocoon Index"
const INDEX_VERSION: u32 = 1;

/// Index granularity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexGranularity {
    /// Year-level index (YYYY)
    Year,
    /// Month-level index (YYYY-MM)
    Month,
    /// Day-level index (YYYY-MM-DD)
    Day,
    /// Hour-level index (YYYY-MM-DD HH)
    Hour,
    /// Minute-level index (YYYY-MM-DD HH:MM)
    Minute,
}

impl IndexGranularity {
    /// Get the next finer granularity level
    fn finer(&self) -> Option<Self> {
        match self {
            Self::Year => Some(Self::Month),
            Self::Month => Some(Self::Day),
            Self::Day => Some(Self::Hour),
            Self::Hour => Some(Self::Minute),
            Self::Minute => None,
        }
    }
    
    /// Get the next coarser granularity level
    fn coarser(&self) -> Option<Self> {
        match self {
            Self::Year => None,
            Self::Month => Some(Self::Year),
            Self::Day => Some(Self::Month),
            Self::Hour => Some(Self::Day),
            Self::Minute => Some(Self::Hour),
        }
    }
    
    /// Format a timestamp according to this granularity
    fn format(&self, timestamp: &DateTime<Utc>) -> String {
        match self {
            Self::Year => format!("{:04}", timestamp.year()),
            Self::Month => format!("{:04}-{:02}", timestamp.year(), timestamp.month()),
            Self::Day => format!("{:04}-{:02}-{:02}", timestamp.year(), timestamp.month(), timestamp.day()),
            Self::Hour => format!("{:04}-{:02}-{:02}_{:02}", 
                timestamp.year(), timestamp.month(), timestamp.day(), timestamp.hour()),
            Self::Minute => format!("{:04}-{:02}-{:02}_{:02}:{:02}", 
                timestamp.year(), timestamp.month(), timestamp.day(), timestamp.hour(), timestamp.minute()),
        }
    }
    
    /// Parse a formatted string back to a timestamp range
    fn parse(&self, formatted: &str) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        match self {
            Self::Year => {
                let year = formatted.parse::<i32>().map_err(|_| {
                    Error::Index(format!("Invalid year format: {}", formatted))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid year: {}", year))
                })?;
                
                let end = Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid year: {}", year + 1))
                })?;
                
                Ok((start, end))
            },
            Self::Month => {
                let parts: Vec<&str> = formatted.split('-').collect();
                if parts.len() != 2 {
                    return Err(Error::Index(format!("Invalid month format: {}", formatted)));
                }
                
                let year = parts[0].parse::<i32>().map_err(|_| {
                    Error::Index(format!("Invalid year in month format: {}", parts[0]))
                })?;
                
                let month = parts[1].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid month: {}", parts[1]))
                })?;
                
                if month < 1 || month > 12 {
                    return Err(Error::Index(format!("Month out of range: {}", month)));
                }
                
                let start = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid date: {}-{}", year, month))
                })?;
                
                let end_month = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };
                
                let end = Utc.with_ymd_and_hms(end_month.0, end_month.1, 1, 0, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid date: {}-{}", end_month.0, end_month.1))
                })?;
                
                Ok((start, end))
            },
            Self::Day => {
                let parts: Vec<&str> = formatted.split('-').collect();
                if parts.len() != 3 {
                    return Err(Error::Index(format!("Invalid day format: {}", formatted)));
                }
                
                let year = parts[0].parse::<i32>().map_err(|_| {
                    Error::Index(format!("Invalid year in day format: {}", parts[0]))
                })?;
                
                let month = parts[1].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid month in day format: {}", parts[1]))
                })?;
                
                let day = parts[2].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid day: {}", parts[2]))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid date: {}-{}-{}", year, month, day))
                })?;
                
                // Calculate end (next day)
                let end = start + chrono::Duration::days(1);
                
                Ok((start, end))
            },
            Self::Hour => {
                let parts: Vec<&str> = formatted.split(&['_', '-'][..]).collect();
                if parts.len() != 4 {
                    return Err(Error::Index(format!("Invalid hour format: {}", formatted)));
                }
                
                let year = parts[0].parse::<i32>().map_err(|_| {
                    Error::Index(format!("Invalid year in hour format: {}", parts[0]))
                })?;
                
                let month = parts[1].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid month in hour format: {}", parts[1]))
                })?;
                
                let day = parts[2].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid day in hour format: {}", parts[2]))
                })?;
                
                let hour = parts[3].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid hour: {}", parts[3]))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).single().ok_or_else(|| {
                    Error::Index(format!("Invalid datetime: {}-{}-{} {}", year, month, day, hour))
                })?;
                
                // Calculate end (next hour)
                let end = start + chrono::Duration::hours(1);
                
                Ok((start, end))
            },
            Self::Minute => {
                let main_parts: Vec<&str> = formatted.split('_').collect();
                if main_parts.len() != 2 {
                    return Err(Error::Index(format!("Invalid minute format: {}", formatted)));
                }
                
                let date_parts: Vec<&str> = main_parts[0].split('-').collect();
                if date_parts.len() != 3 {
                    return Err(Error::Index(format!("Invalid date in minute format: {}", main_parts[0])));
                }
                
                let time_parts: Vec<&str> = main_parts[1].split(':').collect();
                if time_parts.len() != 2 {
                    return Err(Error::Index(format!("Invalid time in minute format: {}", main_parts[1])));
                }
                
                let year = date_parts[0].parse::<i32>().map_err(|_| {
                    Error::Index(format!("Invalid year in minute format: {}", date_parts[0]))
                })?;
                
                let month = date_parts[1].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid month in minute format: {}", date_parts[1]))
                })?;
                
                let day = date_parts[2].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid day in minute format: {}", date_parts[2]))
                })?;
                
                let hour = time_parts[0].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid hour in minute format: {}", time_parts[0]))
                })?;
                
                let minute = time_parts[1].parse::<u32>().map_err(|_| {
                    Error::Index(format!("Invalid minute: {}", time_parts[1]))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, day, hour, minute, 0).single().ok_or_else(|| {
                    Error::Index(format!(
                        "Invalid datetime: {}-{}-{} {}:{}", year, month, day, hour, minute
                    ))
                })?;
                
                // Calculate end (next minute)
                let end = start + chrono::Duration::minutes(1);
                
                Ok((start, end))
            },
        }
    }
}

/// Index entry pointing to a log in a segment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexEntry {
    /// Segment ID containing the log
    pub segment_id: u64,
    /// Offset within the segment
    pub offset: u64,
    /// Size of the entry in bytes
    pub size: u32,
    /// Timestamp of the log entry
    pub timestamp: DateTime<Utc>,
}

impl IndexEntry {
    /// Create a new index entry
    pub fn new(segment_id: u64, offset: u64, size: u32, timestamp: DateTime<Utc>) -> Self {
        Self {
            segment_id,
            offset,
            size,
            timestamp,
        }
    }
}

/// Statistics about an index
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total size of all indices in bytes
    pub size_bytes: usize,
    /// Number of index entries
    pub entry_count: usize,
    /// Number of index files
    pub file_count: usize,
}

/// Time-based index tree
struct TimeIndex {
    /// Index granularity
    granularity: IndexGranularity,
    /// Time period formatted as string
    period: String,
    /// Index entries
    entries: Vec<IndexEntry>,
    /// Child indices for finer granularity
    children: HashMap<String, TimeIndex>,
    /// Modified flag
    modified: bool,
}

impl TimeIndex {
    /// Create a new time index
    fn new(granularity: IndexGranularity, period: String) -> Self {
        Self {
            granularity,
            period,
            entries: Vec::new(),
            children: HashMap::new(),
            modified: false,
        }
    }
    
    /// Add an index entry
    fn add_entry(&mut self, entry: IndexEntry) {
        // Add to this level
        self.entries.push(entry);
        self.modified = true;
        
        // If there's a finer granularity, add to the appropriate child
        if let Some(finer) = self.granularity.finer() {
            let child_period = finer.format(&entry.timestamp);
            let child = self.children.entry(child_period.clone())
                .or_insert_with(|| TimeIndex::new(finer, child_period));
            
            child.add_entry(entry);
        }
    }
    
    /// Get entries in a time range
    fn get_entries_in_range(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Vec<IndexEntry> {
        let mut result = Vec::new();
        
        // Check if this index's time range overlaps with the query range
        if let Ok((index_start, index_end)) = self.granularity.parse(&self.period) {
            if index_end <= *start || index_start >= *end {
                // No overlap
                return result;
            }
            
            // This index overlaps with the query range
            // Try to find matching children for more precise results
            if !self.children.is_empty() {
                for child in self.children.values() {
                    result.extend(child.get_entries_in_range(start, end));
                }
            } else {
                // No children, filter entries directly
                for entry in &self.entries {
                    if entry.timestamp >= *start && entry.timestamp < *end {
                        result.push(*entry);
                    }
                }
            }
        }
        
        result
    }
    
    /// Get all entries in this index and its children
    fn get_all_entries(&self) -> Vec<IndexEntry> {
        let mut result = self.entries.clone();
        
        for child in self.children.values() {
            result.extend(child.get_all_entries());
        }
        
        result
    }
    
    /// Remove entries for a segment
    fn remove_segment_entries(&mut self, segment_id: u64) -> bool {
        // Remove entries from this level
        let original_len = self.entries.len();
        self.entries.retain(|e| e.segment_id != segment_id);
        
        let removed = self.entries.len() < original_len;
        if removed {
            self.modified = true;
        }
        
        // Remove from children
        let mut empty_children = Vec::new();
        for (key, child) in &mut self.children {
            let child_removed = child.remove_segment_entries(segment_id);
            
            if child_removed {
                self.modified = true;
                
                // Check if child is now empty
                if child.entries.is_empty() && child.children.is_empty() {
                    empty_children.push(key.clone());
                }
            }
        }
        
        // Remove empty children
        for key in empty_children {
            self.children.remove(&key);
        }
        
        removed
    }
    
    /// Count entries in this index and its children
    fn count_entries(&self) -> usize {
        let mut count = self.entries.len();
        
        for child in self.children.values() {
            count += child.count_entries();
        }
        
        count
    }
    
    /// Serialize the index to bytes
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        
        // Write granularity
        buffer.write_u8(match self.granularity {
            IndexGranularity::Year => 0,
            IndexGranularity::Month => 1,
            IndexGranularity::Day => 2,
            IndexGranularity::Hour => 3,
            IndexGranularity::Minute => 4,
        })?;
        
        // Write period
        buffer.write_u16::<LittleEndian>(self.period.len() as u16)?;
        buffer.write_all(self.period.as_bytes())?;
        
        // Write entries
        buffer.write_u32::<LittleEndian>(self.entries.len() as u32)?;
        for entry in &self.entries {
            // Segment ID
            buffer.write_u64::<LittleEndian>(entry.segment_id)?;
            // Offset
            buffer.write_u64::<LittleEndian>(entry.offset)?;
            // Size
            buffer.write_u32::<LittleEndian>(entry.size)?;
            // Timestamp
            buffer.write_i64::<LittleEndian>(entry.timestamp.timestamp())?;
            buffer.write_u32::<LittleEndian>(entry.timestamp.nanosecond())?;
        }
        
        // Write children count
        buffer.write_u32::<LittleEndian>(self.children.len() as u32)?;
        
        // Write children
        for (_, child) in &self.children {
            let child_data = child.serialize()?;
            buffer.write_u32::<LittleEndian>(child_data.len() as u32)?;
            buffer.write_all(&child_data)?;
        }
        
        Ok(buffer)
    }
    
    /// Deserialize from bytes
    fn deserialize(data: &[u8]) -> Result<Self> {
        let mut cursor = io::Cursor::new(data);
        
        // Read granularity
        let granularity_byte = cursor.read_u8()?;
        let granularity = match granularity_byte {
            0 => IndexGranularity::Year,
            1 => IndexGranularity::Month,
            2 => IndexGranularity::Day,
            3 => IndexGranularity::Hour,
            4 => IndexGranularity::Minute,
            _ => return Err(Error::Index(format!("Invalid granularity byte: {}", granularity_byte))),
        };
        
        // Read period
        let period_len = cursor.read_u16::<LittleEndian>()? as usize;
        let mut period_bytes = vec![0u8; period_len];
        cursor.read_exact(&mut period_bytes)?;
        let period = String::from_utf8(period_bytes).map_err(|e| {
            Error::Index(format!("Invalid period string: {}", e))
        })?;
        
        // Read entries
        let entry_count = cursor.read_u32::<LittleEndian>()? as usize;
        let mut entries = Vec::with_capacity(entry_count);
        
        for _ in 0..entry_count {
            // Segment ID
            let segment_id = cursor.read_u64::<LittleEndian>()?;
            // Offset
            let offset = cursor.read_u64::<LittleEndian>()?;
            // Size
            let size = cursor.read_u32::<LittleEndian>()?;
            // Timestamp
            let timestamp_secs = cursor.read_i64::<LittleEndian>()?;
            let timestamp_nanos = cursor.read_u32::<LittleEndian>()?;
            
            let timestamp = match DateTime::from_timestamp(timestamp_secs, timestamp_nanos) {
                Some(dt) => dt,
                None => return Err(Error::Index(format!(
                    "Invalid timestamp: {}s {}ns", timestamp_secs, timestamp_nanos
                ))),
            };
            
            entries.push(IndexEntry {
                segment_id,
                offset,
                size,
                timestamp,
            });
        }
        
        // Read children
        let children_count = cursor.read_u32::<LittleEndian>()? as usize;
        let mut children = HashMap::with_capacity(children_count);
        
        for _ in 0..children_count {
            let child_size = cursor.read_u32::<LittleEndian>()? as usize;
            let mut child_data = vec![0u8; child_size];
            cursor.read_exact(&mut child_data)?;
            
            let child = Self::deserialize(&child_data)?;
            children.insert(child.period.clone(), child);
        }
        
        Ok(Self {
            granularity,
            period,
            entries,
            children,
            modified: false,
        })
    }
}

/// Main index manager
pub struct IndexManager {
    /// Base directory for index files
    base_dir: PathBuf,
    /// Segment manager
    segment_manager: Arc<SegmentManager>,
    /// Time-based indices
    time_indices: RwLock<HashMap<String, TimeIndex>>,
    /// Store configuration
    config: StoreConfig,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Index is open
    is_open: Arc<RwLock<bool>>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new<P: AsRef<Path>>(
        dir: P,
        segment_manager: Arc<SegmentManager>,
        config: &StoreConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Result<Self> {
        let base_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).map_err(|e| {
            Error::Index(format!("Failed to create index directory {}: {}", base_dir.display(), e))
        })?;
        
        let mut manager = Self {
            base_dir,
            segment_manager,
            time_indices: RwLock::new(HashMap::new()),
            config: config.clone(),
            metrics,
            is_open: Arc::new(RwLock::new(true)),
        };
        
        // Load existing indices
        manager.load_indices()?;
        
        Ok(manager)
    }
    
    /// Add a log entry to the index
    pub fn add_entry(&self, 
        entry: &LogEntry, 
        segment_id: u64, 
        offset: u64, 
        size: u32
    ) -> Result<()> {
        self.ensure_open()?;
        
        let timestamp = entry.metadata.timestamp;
        let idx_entry = IndexEntry::new(segment_id, offset, size, timestamp);
        
        // Add to year index first
        let year_key = IndexGranularity::Year.format(&timestamp);
        
        let mut indices = self.time_indices.write();
        let year_index = indices.entry(year_key.clone())
            .or_insert_with(|| TimeIndex::new(IndexGranularity::Year, year_key));
        
        // Add entry to index tree
        year_index.add_entry(idx_entry);
        
        // Update metrics
        self.metrics.increment_index_entries();
        
        Ok(())
    }
    
    /// Find log entries in a time range
    pub fn find_in_time_range(
        &self,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
    ) -> Result<Vec<IndexEntry>> {
        self.ensure_open()?;
        
        let query_start = Instant::now();
        let mut results = Vec::new();
        
        // Find relevant year indices
        let start_year = start.year();
        let end_year = end.year();
        
        let indices = self.time_indices.read();
        
        for year in start_year..=end_year {
            let year_key = format!("{:04}", year);
            
            if let Some(year_index) = indices.get(&year_key) {
                results.extend(year_index.get_entries_in_range(start, end));
            }
        }
        
        // Sort results by timestamp
        results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        
        // Update metrics
        let duration = query_start.elapsed();
        self.metrics.record_index_query_duration(duration);
        self.metrics.record_index_query_results(results.len());
        
        Ok(results)
    }
    
    /// Find all entries for a specific segment
    pub fn find_by_segment(&self, segment_id: u64) -> Result<Vec<IndexEntry>> {
        self.ensure_open()?;
        
        let mut results = Vec::new();
        let indices = self.time_indices.read();
        
        for index in indices.values() {
            for entry in &index.entries {
                if entry.segment_id == segment_id {
                    results.push(*entry);
                }
            }
            
            // Also check children recursively
            for child in index.children.values() {
                Self::find_segment_entries_recursive(child, segment_id, &mut results);
            }
        }
        
        // Sort results by timestamp
        results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        
        Ok(results)
    }
    
    /// Remove all entries for a segment
    pub fn remove_segment(&self, segment_id: u64) -> Result<usize> {
        self.ensure_open()?;
        
        let mut removed_count = 0;
        let mut indices = self.time_indices.write();
        
        // Find and remove entries
        for index in indices.values_mut() {
            let before_count = index.count_entries();
            index.remove_segment_entries(segment_id);
            let after_count = index.count_entries();
            
            removed_count += before_count - after_count;
        }
        
        // Update metrics
        if removed_count > 0 {
            self.metrics.remove_index_entries(removed_count);
        }
        
        Ok(removed_count)
    }
    
    /// Flush modified indices to disk
    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        
        let indices = self.time_indices.read();
        
        for (key, index) in indices.iter() {
            if index.modified {
                self.save_index(key, index)?;
            }
        }
        
        Ok(())
    }
    
    /// Get statistics about the index
    pub fn stats(&self) -> Result<IndexStats> {
        self.ensure_open()?;
        
        let mut stats = IndexStats::default();
        let indices = self.time_indices.read();
        
        stats.file_count = indices.len();
        
        for index in indices.values() {
            stats.entry_count += index.count_entries();
        }
        
        // Calculate size by checking index files
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "idx") {
                if let Ok(metadata) = fs::metadata(&path) {
                    stats.size_bytes += metadata.len() as usize;
                }
            }
        }
        
        Ok(stats)
    }
    
    /// Close the index manager
    pub fn close(&self) -> Result<()> {
        // Check if already closed
        {
            let mut is_open = self.is_open.write();
            if !*is_open {
                return Ok(());
            }
            *is_open = false;
        }
        
        // Flush any modified indices
        self.flush()?;
        
        Ok(())
    }
    
    // Helper methods
    
    /// Ensure the index manager is open
    fn ensure_open(&self) -> Result<()> {
        if !*self.is_open.read() {
            return Err(Error::Index("Index manager is closed".to_string()));
        }
        Ok(())
    }
    
    /// Load existing indices from disk
    fn load_indices(&self) -> Result<()> {
        let mut loaded = 0;
        
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "idx") {
                if let Some(stem) = path.file_stem() {
                    if let Some(key) = stem.to_str() {
                        if let Ok(index) = self.load_index(key) {
                            let mut indices = self.time_indices.write();
                            indices.insert(key.to_string(), index);
                            loaded += 1;
                        }
                    }
                }
            }
        }
        
        println!("Loaded {} index files", loaded);
        
        Ok(())
    }
    
    /// Load an index from disk
    fn load_index(&self, key: &str) -> Result<TimeIndex> {
        let path = self.base_dir.join(format!("{}.idx", key));
        
        let file = File::open(&path).map_err(|e| {
            Error::Index(format!("Failed to open index file {}: {}", path.display(), e))
        })?;
        
        let mut reader = BufReader::new(file);
        
        // Read and validate header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        
        if magic != *INDEX_MAGIC {
            return Err(Error::Index(format!("Invalid index file header: {:?}", magic)));
        }
        
        let version = reader.read_u32::<LittleEndian>()?;
        if version != INDEX_VERSION {
            return Err(Error::Index(format!("Unsupported index version: {}", version)));
        }
        
        // Read index data
        let data_size = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = vec![0u8; data_size];
        reader.read_exact(&mut data)?;
        
        // Deserialize index
        let index = TimeIndex::deserialize(&data)?;
        
        Ok(index)
    }
    
    /// Save an index to disk
    fn save_index(&self, key: &str, index: &TimeIndex) -> Result<()> {
        let path = self.base_dir.join(format!("{}.idx", key));
        
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                Error::Index(format!("Failed to create index file {}: {}", path.display(), e))
            })?;
        
        let mut writer = BufWriter::new(file);
        
        // Write header
        writer.write_all(INDEX_MAGIC)?;
        writer.write_u32::<LittleEndian>(INDEX_VERSION)?;
        
        // Serialize index
        let data = index.serialize()?;
        
        // Write data size and data
        writer.write_u64::<LittleEndian>(data.len() as u64)?;
        writer.write_all(&data)?;
        
        writer.flush()?;
        
        Ok(())
    }
    
    /// Recursively find entries for a segment in an index
    fn find_segment_entries_recursive(
        index: &TimeIndex, 
        segment_id: u64, 
        results: &mut Vec<IndexEntry>
    ) {
        // Check entries at this level
        for entry in &index.entries {
            if entry.segment_id == segment_id {
                results.push(*entry);
            }
        }
        
        // Check children recursively
        for child in index.children.values() {
            Self::find_segment_entries_recursive(child, segment_id, results);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use chrysalis_rs::{LogEntry, LogLevel};
    use crate::segment::SegmentManager;
    
    #[test]
    fn test_index_granularity_formatting() {
        let timestamp = Utc.with_ymd_and_hms(2023, 5, 15, 14, 30, 0).unwrap();
        
        assert_eq!(IndexGranularity::Year.format(&timestamp), "2023");
        assert_eq!(IndexGranularity::Month.format(&timestamp), "2023-05");
        assert_eq!(IndexGranularity::Day.format(&timestamp), "2023-05-15");
        assert_eq!(IndexGranularity::Hour.format(&timestamp), "2023-05-15_14");
        assert_eq!(IndexGranularity::Minute.format(&timestamp), "2023-05-15_14:30");
    }
    
    #[test]
    fn test_index_granularity_parsing() {
        // Year
        let (start, end) = IndexGranularity::Year.parse("2023").unwrap();
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());
        
        // Month
        let (start, end) = IndexGranularity::Month.parse("2023-05").unwrap();
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 1, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap());
        
        // Day
        let (start, end) = IndexGranularity::Day.parse("2023-05-15").unwrap();
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 15, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 5, 16, 0, 0, 0).unwrap());
        
        // Hour
        let (start, end) = IndexGranularity::Hour.parse("2023-05-15_14").unwrap();
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 15, 14, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 5, 15, 15, 0, 0).unwrap());
        
        // Minute
        let (start, end) = IndexGranularity::Minute.parse("2023-05-15_14:30").unwrap();
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 15, 14, 30, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 5, 15, 14, 31, 0).unwrap());
    }
    
    #[test]
    fn test_time_index_serialization() {
        // Create a sample index
        let mut index = TimeIndex::new(IndexGranularity::Year, "2023".to_string());
        
        // Add some entries
        for i in 0..5 {
            let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, i, 0, 0).unwrap();
            index.add_entry(IndexEntry::new(1, i * 100, 50, timestamp));
        }
        
        // Add a child index
        let month_key = "2023-01".to_string();
        let mut month_index = TimeIndex::new(IndexGranularity::Month, month_key.clone());
        
        for i in 0..3 {
            let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, i, 30, 0).unwrap();
            month_index.add_entry(IndexEntry::new(1, i * 200, 75, timestamp));
        }
        
        index.children.insert(month_key, month_index);
        
        // Serialize
        let serialized = index.serialize().unwrap();
        
        // Deserialize
        let deserialized = TimeIndex::deserialize(&serialized).unwrap();
        
        // Verify
        assert_eq!(deserialized.granularity, IndexGranularity::Year);
        assert_eq!(deserialized.period, "2023");
        assert_eq!(deserialized.entries.len(), 5);
        assert_eq!(deserialized.children.len(), 1);
        
        let child = deserialized.children.get("2023-01").unwrap();
        assert_eq!(child.granularity, IndexGranularity::Month);
        assert_eq!(child.period, "2023-01");
        assert_eq!(child.entries.len(), 3);
    }
    
    // Create a test helper that sets up a mock segment manager and index manager
    fn setup_test_environment() -> Result<(tempfile::TempDir, Arc<IndexManager>)> {
        let temp_dir = tempdir()?;
        let segments_dir = temp_dir.path().join("segments");
        let index_dir = temp_dir.path().join("index");
        
        fs::create_dir_all(&segments_dir)?;
        fs::create_dir_all(&index_dir)?;
        
        let config = StoreConfig::default();
        let metrics = Arc::new(MetricsCollector::new());
        
        let segment_manager = Arc::new(SegmentManager::new(
            segments_dir,
            &config,
            metrics.clone(),
        )?);
        
        let index_manager = Arc::new(IndexManager::new(
            index_dir,
            segment_manager,
            &config,
            metrics,
        )?);
        
        Ok((temp_dir, index_manager))
    }
    
    #[test]
    fn test_index_manager_add_and_find() -> Result<()> {
        let (_temp_dir, index_manager) = setup_test_environment()?;
        
        // Create test log entries with different timestamps
        let mut entries = Vec::new();
        
        for i in 0..10 {
            let timestamp = Utc.with_ymd_and_hms(2023, 1, i + 1, 12, 0, 0).unwrap();
            let mut entry = LogEntry::new(format!("Test log {}", i), LogLevel::Info);
            entry.metadata.timestamp = timestamp;
            entries.push(entry);
        }
        
        // Add entries to index
        for (i, entry) in entries.iter().enumerate() {
            index_manager.add_entry(entry, 1, i as u64 * 100, 50)?;
        }
        
        // Test finding by time range
        let start = Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 7, 0, 0, 0).unwrap();
        
        let results = index_manager.find_in_time_range(&start, &end)?;
        assert_eq!(results.len(), 4); // Days 3, 4, 5, 6
        
        // Test finding by segment
        let segment_results = index_manager.find_by_segment(1)?;
        assert_eq!(segment_results.len(), 10);
        
        // Test removing segment
        let removed = index_manager.remove_segment(1)?;
        assert_eq!(removed, 10);
        
        // Verify segment was removed
        let results_after_remove = index_manager.find_by_segment(1)?;
        assert_eq!(results_after_remove.len(), 0);
        
        Ok(())
    }
}