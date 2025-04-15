use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bincode::{serialize, deserialize};
use bloomfilter::Bloom;

use chrysalis_rs::LogEntry;
use crate::error::{Result, Error};
use crate::config::StoreConfig;
use crate::metrics::MetricsCollector;

/// Magic bytes to identify Bloom filter files
const BLOOM_MAGIC: &[u8; 4] = b"CCNB"; // "Cocoon Bloom"
const BLOOM_VERSION: u32 = 1;

/// Default false positive rate for Bloom filters
const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Default initial capacity for Bloom filters
const DEFAULT_INITIAL_CAPACITY: usize = 10_000;

/// Types of Bloom filters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum BloomFilterType {
    /// Filter for message text
    MessageText,
    /// Filter for context keys
    ContextKey,
    /// Filter for context values
    ContextValue,
}

/// A Bloom filter for a specific segment
struct SegmentBloomFilters {
    /// Filter for message text
    message_text: Bloom<String>,
    /// Filter for context keys
    context_keys: Bloom<String>,
    /// Filter for context values
    context_values: Bloom<String>,
    /// Whether the filters have been modified
    modified: bool,
}

impl SegmentBloomFilters {
    /// Create new Bloom filters for a segment
    fn new(bits: usize) -> Self {
        // Calculate capacity and FP rate based on bits
        let capacity = DEFAULT_INITIAL_CAPACITY;
        let fp_rate = DEFAULT_FALSE_POSITIVE_RATE;
        
        Self {
            message_text: Bloom::new_for_fp_rate(capacity, fp_rate),
            context_keys: Bloom::new_for_fp_rate(capacity, fp_rate),
            context_values: Bloom::new_for_fp_rate(capacity, fp_rate),
            modified: false,
        }
    }
    
    /// Add words from a message to the message text filter
    fn add_message_text(&mut self, message: &str) {
        // Split message into words and add each word
        for word in message.to_lowercase().split_whitespace() {
            self.message_text.set(&word.to_string());
        }
        self.modified = true;
    }
    
    /// Add a context key to the context keys filter
    fn add_context_key(&mut self, key: &str) {
        self.context_keys.set(&key.to_lowercase());
        self.modified = true;
    }
    
    /// Add a context value to the context values filter
    fn add_context_value(&mut self, value: &str) {
        self.context_values.set(&value.to_lowercase());
        self.modified = true;
    }
    
    /// Check if the message text filter may contain a word
    fn may_contain_text(&self, text: &str) -> bool {
        // Split text into words and check if any word might be in the filter
        for word in text.to_lowercase().split_whitespace() {
            if self.message_text.check(&word.to_string()) {
                return true;
            }
        }
        false
    }
    
    /// Check if the context keys filter may contain a key
    fn may_contain_context_key(&self, key: &str) -> bool {
        self.context_keys.check(&key.to_lowercase())
    }
    
    /// Check if the context values filter may contain a value
    fn may_contain_context_value(&self, value: &str) -> bool {
        self.context_values.check(&value.to_lowercase())
    }
    
    /// Serialize all filters to bytes
    fn serialize(&self) -> Result<Vec<u8>> {
        // Create a buffer for serialized data
        let mut buffer = Vec::new();
        
        // Serialize message text filter
        let message_bits = self.message_text.bitmap();
        let message_k = self.message_text.number_of_hash_functions();
        let message_bits_per_key = self.message_text.bits_per_key();
        
        buffer.write_u64::<LittleEndian>(message_bits.len() as u64)?;
        for bit in message_bits {
            buffer.write_u64::<LittleEndian>(*bit)?;
        }
        buffer.write_u32::<LittleEndian>(message_k)?;
        buffer.write_f64::<LittleEndian>(message_bits_per_key)?;
        
        // Serialize context keys filter
        let keys_bits = self.context_keys.bitmap();
        let keys_k = self.context_keys.number_of_hash_functions();
        let keys_bits_per_key = self.context_keys.bits_per_key();
        
        buffer.write_u64::<LittleEndian>(keys_bits.len() as u64)?;
        for bit in keys_bits {
            buffer.write_u64::<LittleEndian>(*bit)?;
        }
        buffer.write_u32::<LittleEndian>(keys_k)?;
        buffer.write_f64::<LittleEndian>(keys_bits_per_key)?;
        
        // Serialize context values filter
        let values_bits = self.context_values.bitmap();
        let values_k = self.context_values.number_of_hash_functions();
        let values_bits_per_key = self.context_values.bits_per_key();
        
        buffer.write_u64::<LittleEndian>(values_bits.len() as u64)?;
        for bit in values_bits {
            buffer.write_u64::<LittleEndian>(*bit)?;
        }
        buffer.write_u32::<LittleEndian>(values_k)?;
        buffer.write_f64::<LittleEndian>(values_bits_per_key)?;
        
        Ok(buffer)
    }
    
    /// Deserialize from bytes
    fn deserialize(data: &[u8]) -> Result<Self> {
        let mut cursor = io::Cursor::new(data);
        
        // Deserialize message text filter
        let message_bits_len = cursor.read_u64::<LittleEndian>()? as usize;
        let mut message_bits = Vec::with_capacity(message_bits_len);
        for _ in 0..message_bits_len {
            message_bits.push(cursor.read_u64::<LittleEndian>()?);
        }
        let message_k = cursor.read_u32::<LittleEndian>()?;
        let message_bits_per_key = cursor.read_f64::<LittleEndian>()?;
        
        let message_text = Bloom::from_existing(
            &message_bits,
            message_bits.len() * 64,
            message_k as usize,
            message_bits_per_key,
        );
        
        // Deserialize context keys filter
        let keys_bits_len = cursor.read_u64::<LittleEndian>()? as usize;
        let mut keys_bits = Vec::with_capacity(keys_bits_len);
        for _ in 0..keys_bits_len {
            keys_bits.push(cursor.read_u64::<LittleEndian>()?);
        }
        let keys_k = cursor.read_u32::<LittleEndian>()?;
        let keys_bits_per_key = cursor.read_f64::<LittleEndian>()?;
        
        let context_keys = Bloom::from_existing(
            &keys_bits,
            keys_bits.len() * 64,
            keys_k as usize,
            keys_bits_per_key,
        );
        
        // Deserialize context values filter
        let values_bits_len = cursor.read_u64::<LittleEndian>()? as usize;
        let mut values_bits = Vec::with_capacity(values_bits_len);
        for _ in 0..values_bits_len {
            values_bits.push(cursor.read_u64::<LittleEndian>()?);
        }
        let values_k = cursor.read_u32::<LittleEndian>()?;
        let values_bits_per_key = cursor.read_f64::<LittleEndian>()?;
        
        let context_values = Bloom::from_existing(
            &values_bits,
            values_bits.len() * 64,
            values_k as usize,
            values_bits_per_key,
        );
        
        Ok(Self {
            message_text,
            context_keys,
            context_values,
            modified: false,
        })
    }
}

/// Manager for segment Bloom filters
pub struct BloomFilterManager {
    /// Base directory for Bloom filter files
    base_dir: PathBuf,
    /// Bloom filters by segment ID
    filters: RwLock<HashMap<u64, SegmentBloomFilters>>,
    /// Store configuration
    config: StoreConfig,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Filter manager is open
    is_open: Arc<RwLock<bool>>,
}

impl Default for BloomFilterManager {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("bloom"),
            filters: RwLock::new(HashMap::new()),
            config: StoreConfig::default(),
            metrics: Arc::new(MetricsCollector::new()),
            is_open: Arc::new(RwLock::new(true)),
        }
    }
}

impl BloomFilterManager {
    /// Create a new Bloom filter manager
    pub fn new<P: AsRef<Path>>(
        dir: P,
        config: &StoreConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Result<Self> {
        let base_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).map_err(|e| {
            Error::Storage(format!("Failed to create Bloom filter directory {}: {}", base_dir.display(), e))
        })?;
        
        let mut manager = Self {
            base_dir,
            filters: RwLock::new(HashMap::new()),
            config: config.clone(),
            metrics,
            is_open: Arc::new(RwLock::new(true)),
        };
        
        // Load existing filters
        manager.load_filters()?;
        
        Ok(manager)
    }
    
    /// Index a log entry in Bloom filters
    pub fn index_entry(&self, segment_id: u64, entry: &LogEntry) -> Result<()> {
        self.ensure_open()?;
        
        let mut filters = self.filters.write();
        let segment_filters = filters.entry(segment_id)
            .or_insert_with(|| SegmentBloomFilters::new(self.config.bloom_filter_bits));
        
        // Add message text
        segment_filters.add_message_text(&entry.message);
        
        // Add context keys and values
        for (key, value) in &entry.context {
            segment_filters.add_context_key(key);
            segment_filters.add_context_value(&value.to_string());
        }
        
        Ok(())
    }
    
    /// Check if a segment may contain text
    pub fn may_contain_text(&self, segment_id: u64, text: &str) -> bool {
        if let Ok(true) = self.ensure_open() {
            let filters = self.filters.read();
            if let Some(segment_filters) = filters.get(&segment_id) {
                return segment_filters.may_contain_text(text);
            }
        }
        
        // If filters don't exist or aren't open, return true to be safe
        true
    }
    
    /// Check if a segment may contain a context key
    pub fn may_contain_context_key(&self, segment_id: u64, key: &str) -> bool {
        if let Ok(true) = self.ensure_open() {
            let filters = self.filters.read();
            if let Some(segment_filters) = filters.get(&segment_id) {
                return segment_filters.may_contain_context_key(key);
            }
        }
        
        // If filters don't exist or aren't open, return true to be safe
        true
    }
    
    /// Check if a segment may contain a context value
    pub fn may_contain_context_value(&self, segment_id: u64, value: &str) -> bool {
        if let Ok(true) = self.ensure_open() {
            let filters = self.filters.read();
            if let Some(segment_filters) = filters.get(&segment_id) {
                return segment_filters.may_contain_context_value(value);
            }
        }
        
        // If filters don't exist or aren't open, return true to be safe
        true
    }
    
    /// Remove filters for a segment
    pub fn remove_segment(&self, segment_id: u64) -> Result<()> {
        self.ensure_open()?;
        
        // Remove from memory
        {
            let mut filters = self.filters.write();
            filters.remove(&segment_id);
        }
        
        // Remove from disk
        let filter_path = self.filter_path(segment_id);
        if filter_path.exists() {
            fs::remove_file(&filter_path).map_err(|e| {
                Error::Storage(format!(
                    "Failed to remove Bloom filter file {}: {}", 
                    filter_path.display(), e
                ))
            })?;
        }
        
        Ok(())
    }
    
    /// Flush modified filters to disk
    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        
        let filters = self.filters.read();
        
        for (&segment_id, filter) in filters.iter() {
            if filter.modified {
                self.save_filter(segment_id, filter)?;
            }
        }
        
        Ok(())
    }
    
    /// Close the Bloom filter manager
    pub fn close(&self) -> Result<()> {
        // Check if already closed
        {
            let mut is_open = self.is_open.write();
            if !*is_open {
                return Ok(());
            }
            *is_open = false;
        }
        
        // Flush any modified filters
        self.flush()?;
        
        Ok(())
    }
    
    // Internal helper methods
    
    /// Ensure the filter manager is open
    fn ensure_open(&self) -> Result<bool> {
        if !*self.is_open.read() {
            return Err(Error::Storage("Bloom filter manager is closed".to_string()));
        }
        Ok(true)
    }
    
    /// Load existing filters from disk
    fn load_filters(&self) -> Result<()> {
        let mut loaded = 0;
        
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "bloom") {
                if let Some(stem) = path.file_stem() {
                    if let Some(segment_id_str) = stem.to_str() {
                        if let Ok(segment_id) = segment_id_str.parse::<u64>() {
                            match self.load_filter(segment_id) {
                                Ok(filter) => {
                                    let mut filters = self.filters.write();
                                    filters.insert(segment_id, filter);
                                    loaded += 1;
                                },
                                Err(e) => {
                                    // Log error but continue loading other filters
                                    eprintln!("Error loading Bloom filter for segment {}: {}", segment_id, e);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        println!("Loaded {} Bloom filter files", loaded);
        
        Ok(())
    }
    
    /// Load a Bloom filter from disk
    fn load_filter(&self, segment_id: u64) -> Result<SegmentBloomFilters> {
        let path = self.filter_path(segment_id);
        
        let file = File::open(&path).map_err(|e| {
            Error::Storage(format!("Failed to open Bloom filter file {}: {}", path.display(), e))
        })?;
        
        let mut reader = BufReader::new(file);
        
        // Read and validate header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        
        if magic != *BLOOM_MAGIC {
            return Err(Error::Storage(format!("Invalid Bloom filter file header: {:?}", magic)));
        }
        
        let version = reader.read_u32::<LittleEndian>()?;
        if version != BLOOM_VERSION {
            return Err(Error::Storage(format!("Unsupported Bloom filter version: {}", version)));
        }
        
        // Read filter data
        let data_size = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = vec![0u8; data_size];
        reader.read_exact(&mut data)?;
        
        // Deserialize filter
        let filter = SegmentBloomFilters::deserialize(&data)?;
        
        Ok(filter)
    }
    
    /// Save a Bloom filter to disk
    fn save_filter(&self, segment_id: u64, filter: &SegmentBloomFilters) -> Result<()> {
        let path = self.filter_path(segment_id);
        
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                Error::Storage(format!("Failed to create Bloom filter file {}: {}", path.display(), e))
            })?;
        
        let mut writer = BufWriter::new(file);
        
        // Write header
        writer.write_all(BLOOM_MAGIC)?;
        writer.write_u32::<LittleEndian>(BLOOM_VERSION)?;
        
        // Serialize filter
        let data = filter.serialize()?;
        
        // Write data size and data
        writer.write_u64::<LittleEndian>(data.len() as u64)?;
        writer.write_all(&data)?;
        
        writer.flush()?;
        
        Ok(())
    }
    
    /// Get the path for a segment's Bloom filter file
    fn filter_path(&self, segment_id: u64) -> PathBuf {
        self.base_dir.join(format!("{}.bloom", segment_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use chrysalis_rs::{LogEntry, LogLevel};
    
    #[test]
    fn test_bloom_filter_text_indexing() {
        // Create a test entry
        let mut entry = LogEntry::new("This is a test message with important keywords", LogLevel::Info);
        entry.add_context("user_id", "12345").unwrap();
        
        // Create test filters
        let mut filters = SegmentBloomFilters::new(10);
        
        // Index the entry
        filters.add_message_text(&entry.message);
        
        // Test positive matches
        assert!(filters.may_contain_text("test"));
        assert!(filters.may_contain_text("important"));
        assert!(filters.may_contain_text("message"));
        
        // Test negative matches (might have false positives)
        assert!(!filters.may_contain_text("nonexistent"));
        assert!(!filters.may_contain_text("supercalifragilisticexpialidocious"));
    }
    
    #[test]
    fn test_bloom_filter_context_indexing() {
        // Create a test entry
        let mut entry = LogEntry::new("Test message", LogLevel::Info);
        entry.add_context("user_id", "12345").unwrap();
        entry.add_context("request_id", "abcdef").unwrap();
        entry.add_context("status", "success").unwrap();
        
        // Create test filters
        let mut filters = SegmentBloomFilters::new(10);
        
        // Index the entry
        for (key, value) in &entry.context {
            filters.add_context_key(key);
            filters.add_context_value(&value.to_string());
        }
        
        // Test positive matches for keys
        assert!(filters.may_contain_context_key("user_id"));
        assert!(filters.may_contain_context_key("request_id"));
        assert!(filters.may_contain_context_key("status"));
        
        // Test positive matches for values
        assert!(filters.may_contain_context_value("12345"));
        assert!(filters.may_contain_context_value("abcdef"));
        assert!(filters.may_contain_context_value("success"));
        
        // Test negative matches
        assert!(!filters.may_contain_context_key("nonexistent"));
        assert!(!filters.may_contain_context_value("nonexistent"));
    }
    
    #[test]
    fn test_bloom_filter_serialization() -> Result<()> {
        // Create test filters
        let mut filters = SegmentBloomFilters::new(10);
        
        // Add some test data
        filters.add_message_text("This is a test message");
        filters.add_context_key("user_id");
        filters.add_context_value("12345");
        
        // Serialize
        let serialized = filters.serialize()?;
        
        // Deserialize
        let deserialized = SegmentBloomFilters::deserialize(&serialized)?;
        
        // Test that deserialized filters contain the same data
        assert!(deserialized.may_contain_text("test"));
        assert!(deserialized.may_contain_context_key("user_id"));
        assert!(deserialized.may_contain_context_value("12345"));
        
        Ok(())
    }
    
    #[test]
    fn test_bloom_filter_manager() -> Result<()> {
        // Create a temporary directory
        let temp_dir = tempdir()?;
        let metrics = Arc::new(MetricsCollector::new());
        
        // Create the manager
        let manager = BloomFilterManager::new(
            temp_dir.path(),
            &StoreConfig::default(),
            metrics,
        )?;
        
        // Create test entries
        let mut entry1 = LogEntry::new("First test message", LogLevel::Info);
        entry1.add_context("user_id", "12345").unwrap();
        
        let mut entry2 = LogEntry::new("Second test message with error", LogLevel::Error);
        entry2.add_context("user_id", "67890").unwrap();
        entry2.add_context("error_code", "500").unwrap();
        
        // Index entries
        manager.index_entry(1, &entry1)?;
        manager.index_entry(2, &entry2)?;
        
        // Test lookups
        assert!(manager.may_contain_text(1, "first"));
        assert!(!manager.may_contain_text(1, "error"));
        assert!(manager.may_contain_text(2, "error"));
        
        assert!(manager.may_contain_context_key(1, "user_id"));
        assert!(!manager.may_contain_context_key(1, "error_code"));
        assert!(manager.may_contain_context_key(2, "error_code"));
        
        assert!(manager.may_contain_context_value(1, "12345"));
        assert!(!manager.may_contain_context_value(1, "67890"));
        assert!(manager.may_contain_context_value(2, "67890"));
        
        // Flush to disk
        manager.flush()?;
        
        // Check that files were created
        assert!(temp_dir.path().join("1.bloom").exists());
        assert!(temp_dir.path().join("2.bloom").exists());
        
        // Remove a segment
        manager.remove_segment(1)?;
        
        // Check that the file was removed
        assert!(!temp_dir.path().join("1.bloom").exists());
        assert!(temp_dir.path().join("2.bloom").exists());
        
        Ok(())
    }
}