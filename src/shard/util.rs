//! Utility functions for shard management
//!
//! Provides helper functions for working with shards and their files.

use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};

use crate::shard::{ShardId, ShardPeriod, ShardInfo, ShardError, ShardResult, SHARD_MAGIC, SHARD_VERSION};

/// Get the path for a shard file
pub fn shard_path(base_dir: &Path, shard_id: &str) -> PathBuf {
    base_dir.join("shards").join(format!("{}.shard", shard_id))
}

/// Check if a shard file exists
pub fn shard_exists(base_dir: &Path, shard_id: &str) -> bool {
    shard_path(base_dir, shard_id).exists()
}

/// List all shard files in a directory
pub fn list_shard_files(base_dir: &Path) -> ShardResult<Vec<(ShardId, PathBuf)>> {
    let shards_dir = base_dir.join("shards");
    if !shards_dir.exists() {
        return Ok(Vec::new());
    }
    
    let mut result = Vec::new();
    
    for entry in fs::read_dir(&shards_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() && path.extension().map_or(false, |ext| ext == "shard") {
            if let Some(stem) = path.file_stem() {
                if let Some(shard_id) = stem.to_str() {
                    result.push((shard_id.to_string(), path.clone()));
                }
            }
        }
    }
    
    Ok(result)
}

/// Save shard info to a file
pub fn save_shard_info(path: &Path, shard_info: &ShardInfo) -> ShardResult<()> {
    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    
    // Open file for writing
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| {
            ShardError::file_error(path, format!("Failed to create shard file: {}", e))
        })?;
    
    let mut writer = BufWriter::new(file);
    
    // Write header
    writer.write_all(SHARD_MAGIC)?;
    writer.write_u32::<LittleEndian>(SHARD_VERSION)?;
    
    // Write period
    let period_byte = match shard_info.period {
        ShardPeriod::Day => 0u8,
        ShardPeriod::Week => 1u8,
        ShardPeriod::Month => 2u8,
        ShardPeriod::Year => 3u8,
    };
    writer.write_u8(period_byte)?;
    
    // Write time range
    writer.write_i64::<LittleEndian>(shard_info.start_time.timestamp())?;
    writer.write_u32::<LittleEndian>(shard_info.start_time.nanosecond())?;
    writer.write_i64::<LittleEndian>(shard_info.end_time.timestamp())?;
    writer.write_u32::<LittleEndian>(shard_info.end_time.nanosecond())?;
    
    // Write active flag
    writer.write_u8(if shard_info.active { 1 } else { 0 })?;
    
    // Write segment count
    writer.write_u32::<LittleEndian>(shard_info.segments.len() as u32)?;
    
    // Write segments
    for segment_id in &shard_info.segments {
        writer.write_u64::<LittleEndian>(*segment_id)?;
    }
    
    // Write entry count and size
    writer.write_u64::<LittleEndian>(shard_info.entry_count as u64)?;
    writer.write_u64::<LittleEndian>(shard_info.size_bytes as u64)?;
    
    writer.flush()?;
    
    Ok(())
}

/// Load shard info from a file
pub fn load_shard_info(path: &Path, shard_id: &str) -> ShardResult<ShardInfo> {
    // Open file for reading
    let file = File::open(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            ShardError::not_found(shard_id)
        } else {
            ShardError::file_error(path, format!("Failed to open shard file: {}", e))
        }
    })?;
    
    let mut reader = BufReader::new(file);
    
    // Read and validate header
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    
    if magic != *SHARD_MAGIC {
        return Err(ShardError::file_error(
            path, 
            format!("Invalid shard file header: {:?}", magic)
        ));
    }
    
    let version = reader.read_u32::<LittleEndian>()?;
    if version != SHARD_VERSION {
        return Err(ShardError::file_error(
            path,
            format!("Unsupported shard version: {}", version)
        ));
    }
    
    // Read period
    let period_byte = reader.read_u8()?;
    let period = match period_byte {
        0 => ShardPeriod::Day,
        1 => ShardPeriod::Week,
        2 => ShardPeriod::Month,
        3 => ShardPeriod::Year,
        _ => return Err(ShardError::file_error(
            path,
            format!("Invalid shard period: {}", period_byte)
        )),
    };
    
    // Read time range
    let start_timestamp = reader.read_i64::<LittleEndian>()?;
    let start_nanos = reader.read_u32::<LittleEndian>()?;
    let end_timestamp = reader.read_i64::<LittleEndian>()?;
    let end_nanos = reader.read_u32::<LittleEndian>()?;
    
    let start_time = DateTime::from_timestamp(start_timestamp, start_nanos).ok_or_else(|| {
        ShardError::timestamp_error(format!(
            "Invalid start timestamp: {}s {}ns", start_timestamp, start_nanos
        ))
    })?;
    
    let end_time = DateTime::from_timestamp(end_timestamp, end_nanos).ok_or_else(|| {
        ShardError::timestamp_error(format!(
            "Invalid end timestamp: {}s {}ns", end_timestamp, end_nanos
        ))
    })?;
    
    // Read active flag
    let active_byte = reader.read_u8()?;
    let active = active_byte != 0;
    
    // Read segment count
    let segment_count = reader.read_u32::<LittleEndian>()? as usize;
    
    // Read segments
    let mut segments = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        let segment_id = reader.read_u64::<LittleEndian>()?;
        segments.push(segment_id);
    }
    
    // Read entry count and size
    let entry_count = reader.read_u64::<LittleEndian>()? as usize;
    let size_bytes = reader.read_u64::<LittleEndian>()? as usize;
    
    // Create shard info
    let mut shard_info = ShardInfo::new(
        shard_id.to_string(),
        period,
        start_time,
        end_time,
    );
    
    shard_info.segments = segments;
    shard_info.entry_count = entry_count;
    shard_info.size_bytes = size_bytes;
    shard_info.active = active;
    
    Ok(shard_info)
}

/// Delete a shard file
pub fn delete_shard_file(path: &Path) -> ShardResult<()> {
    if path.exists() {
        fs::remove_file(path).map_err(|e| {
            ShardError::file_error(path, format!("Failed to delete shard file: {}", e))
        })?;
    }
    
    Ok(())
}

/// Create an empty shards directory if it doesn't exist
pub fn ensure_shards_dir(base_dir: &Path) -> ShardResult<()> {
    let shards_dir = base_dir.join("shards");
    
    if !shards_dir.exists() {
        fs::create_dir_all(&shards_dir).map_err(|e| {
            ShardError::file_error(
                &shards_dir,
                format!("Failed to create shards directory: {}", e)
            )
        })?;
    }
    
    Ok(())
}

/// Check if a path has enough disk space
pub fn check_disk_space(path: &Path, required_bytes: u64) -> ShardResult<bool> {
    if let Some(available) = available_disk_space(path)? {
        Ok(available >= required_bytes)
    } else {
        // Can't determine available space, assume there's enough
        Ok(true)
    }
}

/// Get available disk space for a path
pub fn available_disk_space(path: &Path) -> ShardResult<Option<u64>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let fs_stats = fs::metadata(path).map_err(|e| {
            ShardError::file_error(path, format!("Failed to get filesystem stats: {}", e))
        })?;
        
        // On Unix, we can get the free space from the device ID
        let mut statvfs = libc::statvfs {
            f_bsize: 0,
            f_frsize: 0,
            f_blocks: 0,
            f_bfree: 0,
            f_bavail: 0,
            f_files: 0,
            f_ffree: 0,
            f_favail: 0,
            f_fsid: 0,
            f_flag: 0,
            f_namemax: 0,
        };
        
        let path_cstr = std::ffi::CString::new(path.to_string_lossy().as_bytes())
            .map_err(|_| ShardError::other("Invalid path"))?;
            
        let result = unsafe { libc::statvfs(path_cstr.as_ptr(), &mut statvfs) };
        
        if result == 0 {
            let available = statvfs.f_bsize as u64 * statvfs.f_bavail as u64;
            Ok(Some(available))
        } else {
            Ok(None)
        }
    }
    
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use std::ffi::OsStr;
        use std::iter::once;
        use winapi::um::fileapi::{GetDiskFreeSpaceExW, PULARGE_INTEGER};
        use winapi::shared::minwindef::BOOL;
        
        // Get the root directory of the path
        let root = path.ancestors()
            .find(|p| p.parent().is_none())
            .unwrap_or(path);
            
        // Convert to wide string for Windows API
        let wide_path: Vec<u16> = OsStr::new(root)
            .encode_wide()
            .chain(once(0))
            .collect();
            
        let mut free_bytes_available = 0u64;
        let mut total_bytes = 0u64;
        let mut total_free_bytes = 0u64;
        
        let result = unsafe {
            GetDiskFreeSpaceExW(
                wide_path.as_ptr(),
                &mut free_bytes_available as *mut u64 as PULARGE_INTEGER,
                &mut total_bytes as *mut u64 as PULARGE_INTEGER,
                &mut total_free_bytes as *mut u64 as PULARGE_INTEGER,
            )
        };
        
        if result != 0 {
            Ok(Some(free_bytes_available))
        } else {
            Ok(None)
        }
    }
    
    #[cfg(not(any(unix, windows)))]
    {
        // For other platforms, we don't have a way to check disk space
        Ok(None)
    }
}

/// Get a backup path for a shard file
pub fn shard_backup_path(path: &Path) -> PathBuf {
    let mut backup_path = path.to_path_buf();
    let extension = format!("{}.bak", 
        backup_path.extension().and_then(|e| e.to_str()).unwrap_or("shard")
    );
    backup_path.set_extension(extension);
    backup_path
}

/// Create a backup of a shard file
pub fn backup_shard_file(path: &Path) -> ShardResult<PathBuf> {
    if !path.exists() {
        return Ok(path.to_path_buf());
    }
    
    let backup_path = shard_backup_path(path);
    
    if backup_path.exists() {
        fs::remove_file(&backup_path).map_err(|e| {
            ShardError::file_error(
                &backup_path,
                format!("Failed to remove existing backup: {}", e)
            )
        })?;
    }
    
    fs::copy(path, &backup_path).map_err(|e| {
        ShardError::file_error(
            path,
            format!("Failed to create backup: {}", e)
        )
    })?;
    
    Ok(backup_path)
}

/// Calculate which shard a timestamp belongs to
pub fn get_shard_for_timestamp(period: ShardPeriod, timestamp: DateTime<Utc>) -> ShardId {
    period.create_shard_id(&timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_shard_path() {
        let base = Path::new("/data/store");
        let path = shard_path(base, "20230101");
        assert_eq!(path, Path::new("/data/store/shards/20230101.shard"));
    }
    
    #[test]
    fn test_save_load_shard_info() -> ShardResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.shard");
        
        // Create test shard info
        let now = Utc::now();
        let tomorrow = now + chrono::Duration::days(1);
        
        let mut info = ShardInfo::new(
            "test".to_string(),
            ShardPeriod::Day,
            now,
            tomorrow,
        );
        
        info.add_segment(1, 100, 1000);
        info.add_segment(2, 200, 2000);
        
        // Save to file
        save_shard_info(&path, &info)?;
        
        // Load back
        let loaded = load_shard_info(&path, "test")?;
        
        // Compare
        assert_eq!(loaded.id, info.id);
        assert_eq!(loaded.period, info.period);
        assert_eq!(loaded.segments, info.segments);
        assert_eq!(loaded.entry_count, info.entry_count);
        assert_eq!(loaded.size_bytes, info.size_bytes);
        
        Ok(())
    }
    
    #[test]
    fn test_delete_shard_file() -> ShardResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.shard");
        
        // Create an empty file
        File::create(&path)?;
        
        // Verify it exists
        assert!(path.exists());
        
        // Delete it
        delete_shard_file(&path)?;
        
        // Verify it's gone
        assert!(!path.exists());
        
        // Deleting a non-existent file should be a no-op
        delete_shard_file(&path)?;
        
        Ok(())
    }
    
    #[test]
    fn test_ensure_shards_dir() -> ShardResult<()> {
        let temp_dir = tempdir()?;
        
        // Directory shouldn't exist yet
        let shards_dir = temp_dir.path().join("shards");
        assert!(!shards_dir.exists());
        
        // Create it
        ensure_shards_dir(temp_dir.path())?;
        
        // Verify it exists
        assert!(shards_dir.exists());
        
        // Calling again should be a no-op
        ensure_shards_dir(temp_dir.path())?;
        
        Ok(())
    }
    
    #[test]
    fn test_backup_shard_file() -> ShardResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.shard");
        
        // Create a file with content
        {
            let mut file = File::create(&path)?;
            file.write_all(b"test content")?;
        }
        
        // Create backup
        let backup_path = backup_shard_file(&path)?;
        assert!(backup_path.exists());
        
        // Verify content
        let mut content = String::new();
        File::open(backup_path)?.read_to_string(&mut content)?;
        assert_eq!(content, "test content");
        
        Ok(())
    }
    
    #[test]
    fn test_get_shard_for_timestamp() {
        let timestamp = Utc.with_ymd_and_hms(2023, 5, 15, 12, 0, 0).unwrap();
        
        assert_eq!(get_shard_for_timestamp(ShardPeriod::Day, timestamp), "20230515");
        assert_eq!(get_shard_for_timestamp(ShardPeriod::Month, timestamp), "202305");
        assert_eq!(get_shard_for_timestamp(ShardPeriod::Year, timestamp), "2023");
    }
}