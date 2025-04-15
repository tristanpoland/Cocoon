use std::time::Instant;

use chrono::{DateTime, Utc};
use chrysalis_rs::LogEntry;

use super::{util, ShardError, ShardResult};
use crate::shard::ShardManager;

/// Store a log entry in the appropriate shard
pub fn store(&self, entry: &LogEntry) -> ShardResult<()> {
    self.ensure_open()?;
    
    let timestamp = entry.metadata.timestamp;
    
    // Get or create the appropriate shard
    let shard_id = self.get_or_create_shard_for_timestamp(timestamp)?;
    
    // Store the entry in the shard
    self.store_in_shard(shard_id, entry)
}

/// Read a log entry by its index entry
pub fn read_entry(&self, index_entry: &IndexEntry) -> ShardResult<LogEntry> {
    self.ensure_open()?;
    
    // Read from the segment
    self.segment_manager.read_entry(
        index_entry.segment_id,
        index_entry.offset,
        index_entry.size,
    ).map_err(ShardError::from)
}

/// Read multiple log entries from a segment
pub fn read_segment_entries(&self, segment_id: u64, entries: &[IndexEntry]) -> ShardResult<Vec<LogEntry>> {
    self.ensure_open()?;
    
    // Read entries from the segment
    let start = Instant::now();
    let results = self.segment_manager.read_entries(segment_id, entries)
        .map_err(ShardError::from)?;
    let duration = start.elapsed();
    
    // Update metrics
    self.metrics.record_read_duration(duration);
    self.metrics.add_bytes_read(entries.iter().map(|e| e.size as usize).sum());
    
    Ok(results)
}

/// Get the timestamp range of all shards
pub fn timestamp_range(&self) -> ShardResult<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    self.ensure_open()?;
    
    let shards = self.shards.read();
    
    if shards.is_empty() {
        return Ok((None, None));
    }
    
    let mut min_time = None;
    let mut max_time = None;
    
    for shard in shards.values() {
        if min_time.is_none() || shard.start_time < min_time.unwrap() {
            min_time = Some(shard.start_time);
        }
        
        if max_time.is_none() || shard.end_time > max_time.unwrap() {
            max_time = Some(shard.end_time);
        }
    }
    
    Ok((min_time, max_time))
}

/// Get statistics about all shards
pub fn stats(&self) -> ShardResult<ShardStats> {
    self.ensure_open()?;
    
    let shards = self.shards.read();
    
    let mut stats = ShardStats::new();
    stats.shard_count = shards.len();
    
    let mut active_count = 0;
    let mut inactive_count = 0;
    let mut empty_count = 0;
    
    for shard in shards.values() {
        stats.segment_count += shard.segments.len();
        stats.total_logs += shard.entry_count;
        stats.raw_size_bytes += shard.size_bytes;
        
        // Update timestamp ranges
        if stats.oldest_timestamp.is_none() || 
           stats.oldest_timestamp.unwrap() > shard.start_time {
            stats.oldest_timestamp = Some(shard.start_time);
        }
        
        if stats.newest_timestamp.is_none() || 
           stats.newest_timestamp.unwrap() < shard.end_time {
            stats.newest_timestamp = Some(shard.end_time);
        }
        
        // Count active/inactive shards
        if shard.active {
            active_count += 1;
        } else {
            inactive_count += 1;
        }
        
        // Count empty shards
        if shard.is_empty() {
            empty_count += 1;
        }
        
        // Get segment info to calculate compressed size
        for &segment_id in &shard.segments {
            if let Ok(segment_info) = self.segment_manager.get_segment_info(segment_id) {
                stats.compressed_size_bytes += segment_info.compressed_size;
            }
        }
    }
    
    stats.active_shards = active_count;
    stats.inactive_shards = inactive_count;
    stats.empty_shards = empty_count;
    
    Ok(stats)
}

/// List all shards
pub fn list_shards(&self) -> ShardResult<Vec<ShardInfo>> {
    self.ensure_open()?;
    
    let shards = self.shards.read();
    let mut shard_list = Vec::with_capacity(shards.len());
    
    for shard in shards.values() {
        shard_list.push(shard.clone());
    }
    
    // Sort by start time
    shard_list.sort_by(|a, b| a.start_time.cmp(&b.start_time));
    
    Ok(shard_list)
}

/// Run compaction on all shards
pub fn compact_all(&self) -> ShardResult<()> {
    self.ensure_open()?;
    
    let start = Instant::now();
    
    let shards = self.shards.read();
    let mut shards_to_compact = Vec::new();
    
    // Find shards with multiple segments
    for (id, info) in shards.iter() {
        if info.segments.len() > 1 {
            shards_to_compact.push(id.clone());
        }
    }
    
    // Compact shards
    for shard_id in shards_to_compact {
        if let Err(e) = self.compact_shard(&shard_id) {
            eprintln!("Error compacting shard {}: {}", shard_id, e);
        }
    }
    
    let duration = start.elapsed();
    self.metrics.record_compaction_duration(duration);
    
    Ok(())
}

/// Run compaction on a specific shard
pub fn compact_shard(&self, shard_id: &str) -> ShardResult<()> {
    self.ensure_open()?;
    
    let start = Instant::now();
    
    // Get the shard info
    let shard_info = {
        let shards = self.shards.read();
        match shards.get(shard_id) {
            Some(info) => info.clone(),
            None => return Err(ShardError::not_found(shard_id)),
        }
    };
    
    // Skip if only one segment or no segments
    if shard_info.segments.len() <= 1 {
        return Ok(());
    }
    
    // Get all entries from the shard's segments
    let mut all_entries = Vec::new();
    
    for &segment_id in &shard_info.segments {
        // Get entries from the index
        let segment_entries = self.index_manager.find_by_segment(segment_id)
            .map_err(ShardError::from)?;
        
        // Read entries from the segment
        let logs = self.read_segment_entries(segment_id, &segment_entries)?;
        all_entries.extend(logs);
    }
    
    // Sort entries by timestamp
    all_entries.sort_by(|a, b| a.metadata.timestamp.cmp(&b.metadata.timestamp));
    
    // Create a new segment for the compacted data
    let new_segment_id = self.segment_manager.create_segment()
        .map_err(ShardError::from)?;
    
    // Store all entries in the new segment
    let mut offset = 0;
    let mut entry_count = 0;
    
    for entry in &all_entries {
        let entry_bytes = self.segment_manager.write_entry(new_segment_id, entry)
            .map_err(ShardError::from)?;
        
        // Update the index to point to the new segment
        self.index_manager.add_entry(
            entry, 
            new_segment_id, 
            offset, 
            entry_bytes as u32
        ).map_err(ShardError::from)?;
        
        offset += entry_bytes as u64;
        entry_count += 1;
    }
    
    // Update the shard info
    {
        let mut shards = self.shards.write();
        if let Some(shard) = shards.get_mut(shard_id) {
            // Add the new segment
            let segment_size = self.segment_manager.get_segment_size(new_segment_id)
                .map_err(ShardError::from)?;
            
            // Remove old segments from the shard
            for &old_segment_id in &shard_info.segments {
                let segment_info = self.segment_manager.get_segment_info(old_segment_id)
                    .map_err(ShardError::from)?;
                shard.remove_segment(
                    old_segment_id, 
                    segment_info.entry_count, 
                    segment_info.size
                );
                
                // Remove the old segment from the index
                self.index_manager.remove_segment(old_segment_id)
                    .map_err(ShardError::from)?;
                
                // Delete the old segment file
                self.segment_manager.delete_segment(old_segment_id)
                    .map_err(ShardError::from)?;
            }
            
            // Add the new segment to the shard
            shard.segments = vec![new_segment_id];
            shard.entry_count = entry_count;
            shard.size_bytes = segment_size;
            
            // Update active segment mapping
            let mut active_segments = self.active_segments.write();
            active_segments.insert(shard_id.to_string(), new_segment_id);
        }
    }
    
    // Save the updated shard info
    self.save_shard_info(shard_id)?;
    
    let duration = start.elapsed();
    self.metrics.record_compaction_duration(duration);
    self.metrics.increment_compactions();
    
    Ok(())
}

/// Delete logs before a specific time
pub fn delete_before(&self, timestamp: DateTime<Utc>) -> ShardResult<usize> {
    self.ensure_open()?;
    
    let start = Instant::now();
    let mut deleted_count = 0;
    
    // Get shards that are entirely before the timestamp
    let shards_to_delete = {
        let shards = self.shards.read();
        shards.values()
            .filter(|shard| shard.end_time <= timestamp)
            .map(|shard| shard.id.clone())
            .collect::<Vec<_>>()
    };
    
    // Delete entire shards
    for shard_id in shards_to_delete {
        let shard_info = {
            let shards = self.shards.read();
            shards.get(&shard_id).cloned()
        };
        
        if let Some(shard) = shard_info {
            deleted_count += shard.entry_count;
            
            // Delete all segments in the shard
            for segment_id in &shard.segments {
                // Remove the segment from the index
                self.index_manager.remove_segment(*segment_id)
                    .map_err(ShardError::from)?;
                
                // Delete the segment file
                self.segment_manager.delete_segment(*segment_id)
                    .map_err(ShardError::from)?;
            }
            
            // Remove the shard
            {
                let mut shards = self.shards.write();
                shards.remove(&shard_id);
            }
            
            // Remove from active segments
            {
                let mut active_segments = self.active_segments.write();
                active_segments.remove(&shard_id);
            }
            
            // Delete the shard file
            let shard_path = util::shard_path(&self.base_dir, &shard_id);
            util::delete_shard_file(&shard_path)?;
        }
    }
    
    // Handle shards that overlap with the timestamp
    let overlapping_shards = {
        let shards = self.shards.read();
        shards.values()
            .filter(|shard| shard.start_time < timestamp && shard.end_time > timestamp)
            .map(|shard| shard.id.clone())
            .collect::<Vec<_>>()
    };
    
    // Process overlapping shards
    for shard_id in overlapping_shards {
        // Get entries to keep (after timestamp)
        let entries_to_keep = {
            let shard_info = {
                let shards = self.shards.read();
                shards.get(&shard_id).cloned().unwrap()
            };
            
            let mut keep_entries = Vec::new();
            
            for &segment_id in &shard_info.segments {
                // Find entries in the segment that are after the timestamp
                let segment_entries = self.index_manager.find_by_segment(segment_id)
                    .map_err(ShardError::from)?;
                
                for entry in segment_entries {
                    if entry.timestamp >= timestamp {
                        // Add to keep list
                        let log = self.read_entry(&entry)?;
                        keep_entries.push(log);
                    } else {
                        // Count as deleted
                        deleted_count += 1;
                    }
                }
            }
            
            keep_entries
        };
        
        // If there are entries to keep, create a new segment for them
        if !entries_to_keep.is_empty() {
            let shard_info = {
                let shards = self.shards.read();
                shards.get(&shard_id).cloned().unwrap()
            };
            
            // Create a new segment
            let new_segment_id = self.segment_manager.create_segment()
                .map_err(ShardError::from)?;
            
            // Store all entries in the new segment
            let mut offset = 0;
            let mut entry_count = 0;
            
            for entry in &entries_to_keep {
                let entry_bytes = self.segment_manager.write_entry(new_segment_id, entry)
                    .map_err(ShardError::from)?;
                
                // Update the index to point to the new segment
                self.index_manager.add_entry(
                    entry, 
                    new_segment_id, 
                    offset, 
                    entry_bytes as u32
                ).map_err(ShardError::from)?;
                
                offset += entry_bytes as u64;
                entry_count += 1;
            }
            
            // Update the shard info
            {
                let mut shards = self.shards.write();
                if let Some(shard) = shards.get_mut(&shard_id) {
                    // Add the new segment
                    let segment_size = self.segment_manager.get_segment_size(new_segment_id)
                        .map_err(ShardError::from)?;
                    
                    // Remove old segments from the shard
                    for &old_segment_id in &shard_info.segments {
                        // Remove the old segment from the index
                        self.index_manager.remove_segment(old_segment_id)
                            .map_err(ShardError::from)?;
                        
                        // Delete the old segment file
                        self.segment_manager.delete_segment(old_segment_id)
                            .map_err(ShardError::from)?;
                    }
                    
                    // Update shard info
                    shard.segments = vec![new_segment_id];
                    shard.entry_count = entry_count;
                    shard.size_bytes = segment_size;
                    
                    // Update active segment
                    let mut active_segments = self.active_segments.write();
                    active_segments.insert(shard_id.clone(), new_segment_id);
                }
            }
            
            // Save the updated shard info
            self.save_shard_info(&shard_id)?;
        } else {
            // All entries were deleted, so delete the shard too
            let shard_info = {
                let shards = self.shards.read();
                shards.get(&shard_id).cloned()
            };
            
            if let Some(shard) = shard_info {
                // Delete all segments in the shard
                for segment_id in &shard.segments {
                    // Remove the segment from the index
                    self.index_manager.remove_segment(*segment_id)
                        .map_err(ShardError::from)?;
                    
                    // Delete the segment file
                    self.segment_manager.delete_segment(*segment_id)
                        .map_err(ShardError::from)?;
                }
                
                // Remove the shard
                {
                    let mut shards = self.shards.write();
                    shards.remove(&shard_id);
                }
                
                // Remove from active segments
                {
                    let mut active_segments = self.active_segments.write();
                    active_segments.remove(&shard_id);
                }
                
                // Delete the shard file
                let shard_path = util::shard_path(&self.base_dir, &shard_id);
                util::delete_shard_file(&shard_path)?;
            }
        }
    }
    
    let duration = start.elapsed();
    self.metrics.record_compaction_duration(duration);
    self.metrics.add_deleted_logs(deleted_count);
    
    Ok(deleted_count)
}

/// Close the shard manager
pub fn close(&self) -> ShardResult<()> {
    // Check if already closed
    {
        let mut is_open = self.is_open.write();
        if !*is_open {
            return Ok(());
        }
        *is_open = false;
    }
    
    // Save all shard info
    let shards = self.shards.read();
    for shard_id in shards.keys() {
        if let Err(e) = self.save_shard_info(shard_id) {
            eprintln!("Error saving shard info for {}: {}", shard_id, e);
        }
    }
    
    Ok(())
}

/// Set the shard period to use
pub fn set_shard_period(&mut self, period: ShardPeriod) {
    self.shard_period = period;
}

/// Get the shard period in use
pub fn get_shard_period(&self) -> ShardPeriod {
    self.shard_period
}

// Internal methods

/// Ensure the shard manager is open
fn ensure_open(&self) -> ShardResult<()> {
    if !*self.is_open.read() {
        return Err(ShardError::Closed);
    }
    Ok(())
}

/// Get or create a shard for a timestamp
fn get_or_create_shard_for_timestamp(&self, timestamp: DateTime<Utc>) -> ShardResult<ShardId> {
    // Create the shard ID
    let shard_id = self.shard_period.create_shard_id(&timestamp);
    
    // Check if the shard exists
    {
        let shards = self.shards.read();
        if shards.contains_key(&shard_id) {
            // Update active shard if needed
            let mut active_shards = self.active_shards.write();
            active_shards.insert(self.shard_period, shard_id.clone());
            
            return Ok(shard_id);
        }
    }
    
    // Shard doesn't exist, create it
    self.create_shard(&shard_id, timestamp)?;
    
    // Update active shard
    {
        let mut active_shards = self.active_shards.write();
        active_shards.insert(self.shard_period, shard_id.clone());
    }
    
    Ok(shard_id)
}

/// Create a new shard
fn create_shard(&self, shard_id: &str, timestamp: DateTime<Utc>) -> ShardResult<ShardInfo> {
    // Calculate shard time range
    let (start_time, end_time) = self.shard_period.parse_shard_id(shard_id)?;
    
    // Create shard info
    let shard_info = ShardInfo::new(
        shard_id.to_string(),
        self.shard_period,
        start_time,
        end_time,
    );
    
    // Store shard info
    {
        let mut shards = self.shards.write();
        shards.insert(shard_id.to_string(), shard_info.clone());
    }
    
    // Save shard info to disk
    self.save_shard_info(shard_id)?;
    
    Ok(shard_info)
}

/// Store a log entry in a specific shard
fn store_in_shard(&self, shard_id: ShardId, entry: &LogEntry) -> ShardResult<()> {
    let start = Instant::now();
    
    // Get the active segment for the shard
    let segment_id = self.get_active_segment(shard_id.clone())?;
    
    // Write the entry to the segment
    let entry_size = self.segment_manager.write_entry(segment_id, entry)
        .map_err(ShardError::from)?;
    
    // Add to the index
    let segment_size = self.segment_manager.get_segment_size(segment_id)
        .map_err(ShardError::from)?;
    self.index_manager.add_entry(
        entry, 
        segment_id, 
        segment_size as u64 - entry_size as u64, 
        entry_size as u32
    ).map_err(ShardError::from)?;
    
    // Update shard info
    {
        let mut shards = self.shards.write();
        if let Some(shard) = shards.get_mut(&shard_id) {
            // Check if we need to add this segment to the shard
            if !shard.segments.contains(&segment_id) {
                shard.add_segment(segment_id, 1, entry_size);
            } else {
                // Just update counts
                shard.entry_count += 1;
                shard.size_bytes += entry_size;
            }
        }
    }
    
    let duration = start.elapsed();
    self.metrics.record_write_duration(duration);
    self.metrics.add_bytes_written(entry_size);
    self.metrics.increment_writes();
    
    Ok(())
}

/// Get the active segment for a shard
fn get_active_segment(&self, shard_id: ShardId) -> ShardResult<SegmentId> {
    // First check the cache of active segments
    {
        let active_segments = self.active_segments.read();
        if let Some(&segment_id) = active_segments.get(&shard_id) {
            // Check if the segment has room for more entries
            let segment_size = self.segment_manager.get_segment_size(segment_id)
                .map_err(ShardError::from)?;
            
            if segment_size < self.config.max_segment_size {
                return Ok(segment_id);
            }
        }
    }
    
    // If not found in cache or segment is full, check if the shard has segments
    let shard_info = {
        let shards = self.shards.read();
        match shards.get(&shard_id) {
            Some(info) => info.clone(),
            None => return Err(ShardError::not_found(&shard_id)),
        }
    };
    
    // If the shard has segments, check the last one
    if !shard_info.segments.is_empty() {
        let last_segment_id = *shard_info.segments.last().unwrap();
        
        // Check if the segment has room for more entries
        let segment_size = self.segment_manager.get_segment_size(last_segment_id)
            .map_err(ShardError::from)?;
        
        if segment_size < self.config.max_segment_size {
            // Update cache
            let mut active_segments = self.active_segments.write();
            active_segments.insert(shard_id.clone(), last_segment_id);
            
            return Ok(last_segment_id);
        }
    }
    
    // No usable segment found, create a new one
    let segment_id = self.segment_manager.create_segment()
        .map_err(ShardError::from)?;
    
    // Add the segment to the shard
    {
        let mut shards = self.shards.write();
        if let Some(shard) = shards.get_mut(&shard_id) {
            shard.segments.push(segment_id);
        }
    }
    
    // Update cache
    {
        let mut active_segments = self.active_segments.write();
        active_segments.insert(shard_id, segment_id);
    }
    
    Ok(segment_id)
}

/// Load existing shards from disk
fn load_shards(&self) -> ShardResult<()> {
    // Find all shard files
    let shard_files = util::list_shard_files(&self.base_dir)?;
    let mut loaded = 0;
    
    for (shard_id, path) in shard_files {
        match util::load_shard_info(&path, &shard_id) {
            Ok(shard_info) => {
                // Store in memory
                let mut shards = self.shards.write();
                shards.insert(shard_id.clone(), shard_info);
                loaded += 1;
            },
            Err(e) => {
                eprintln!("Error loading shard {}: {}", shard_id, e);
            }
        }
    }
    
    println!("Loaded {} shards", loaded);
    
    Ok(())
}

/// Update active shards based on current time
fn update_active_shards(&self) -> ShardResult<()> {
    let now = Utc::now();
    
    // Create active shard IDs for each period
    let day_id = ShardPeriod::Day.create_shard_id(&now);
    let week_id = ShardPeriod::Week.create_shard_id(&now);
    let month_id = ShardPeriod::Month.create_shard_id(&now);
    let year_id = ShardPeriod::Year.create_shard_id(&now);
    
    // Update active shards
    let mut active_shards = self.active_shards.write();
    
    // Check if shards exist, create if needed
    {
        let shards = self.shards.read();
        
        if !shards.contains_key(&day_id) {
            drop(shards); // Release the read lock
            self.create_shard(&day_id, now)?;
        }
        
        if !shards.contains_key(&week_id) {
            if !shards.is_empty() { // Only drop if we haven't already
                drop(shards);
            }
            self.create_shard(&week_id, now)?;
        }
        
        if !shards.contains_key(&month_id) {
            if !shards.is_empty() { // Only drop if we haven't already
                drop(shards);
            }
            self.create_shard(&month_id, now)?;
        }
        
        if !shards.contains_key(&year_id) {
            if !shards.is_empty() { // Only drop if we haven't already
                drop(shards);
            }
            self.create_shard(&year_id, now)?;
        }
    }
    
    // Set active shards
    active_shards.insert(ShardPeriod::Day, day_id);
    active_shards.insert(ShardPeriod::Week, week_id);
    active_shards.insert(ShardPeriod::Month, month_id);
    active_shards.insert(ShardPeriod::Year, year_id);
    
    Ok(())
}

/// Save shard info to disk
fn save_shard_info(&self, shard_id: &str) -> ShardResult<()> {
    // Get shard info
    let shard_info = {
        let shards = self.shards.read();
        match shards.get(shard_id) {
            Some(info) => info.clone(),
            None => return Err(ShardError::not_found(shard_id)),
        }
    };
    
    // Save to file
    let path = util::shard_path(&self.base_dir, shard_id);
    util::save_shard_info(&path, &shard_info)?;
    
    Ok(())
}

impl Drop for ShardManager {
fn drop(&mut self) {
    if *self.is_open.read() {
        if let Err(e) = self.close() {
            eprintln!("Error closing shard manager: {}", e);
        }
    }
}
}

// Default implementation for tests and examples
impl Default for ShardManager {
fn default() -> Self {
    // Use a temporary directory for testing
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let config = StoreConfig::default();
    let metrics = Arc::new(MetricsCollector::new());
    
    // Create managers
    let segment_manager = Arc::new(SegmentManager::default());
    let index_manager = Arc::new(IndexManager::default());
    
    Self {
        base_dir: temp_dir.path().to_path_buf(),
        segment_manager,
        index_manager,
        shards: RwLock::new(HashMap::new()),
        active_shards: RwLock::new(HashMap::new()),
        config,
        shard_period: ShardPeriod::default_period(),
        metrics,
        is_open: Arc::new(RwLock::new(true)),
        active_segments: RwLock::new(HashMap::new()),
    }
}
}