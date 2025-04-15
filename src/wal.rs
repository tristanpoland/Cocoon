use bincode::{deserialize, serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrysalis_rs::LogEntry;
use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::config::StoreConfig;
use crate::error::{Error, Result};
use crate::metrics::MetricsCollector;

// Magic bytes to identify WAL files
const WAL_MAGIC: &[u8; 4] = b"CCNW"; // "Cocoon WAL"
const WAL_VERSION: u32 = 1;
const MAX_WAL_SIZE: u64 = 1024 * 1024 * 128; // 128 MB
const HEADER_SIZE: u64 = 16; // Magic(4) + Version(4) + EntryCount(8)

// WAL entry flags
const ENTRY_COMPLETE: u8 = 1; // Entry was completely written
const ENTRY_START: u8 = 2; // Start of a multi-entry transaction
const ENTRY_END: u8 = 4; // End of a multi-entry transaction

/// Represents a single entry in the WAL
#[derive(Debug, Clone)]
struct WalEntry {
    /// Unique ID for the entry
    id: u64,
    /// Flags for the entry
    flags: u8,
    /// Checksum of the serialized log entry
    checksum: u32,
    /// Serialized log entry
    data: Vec<u8>,
}

impl WalEntry {
    /// Create a new WAL entry from a log entry
    fn new(id: u64, entry: &LogEntry, flags: u8) -> Result<Self> {
        // Serialize the log entry
        let data = serialize(entry).map_err(|e| {
            Error::Serialization(serde_json::Error::custom(format!(
                "Bincode serialization error: {}",
                e
            )))
        })?;

        // Compute checksum
        let checksum = Self::compute_checksum(&data);

        Ok(Self {
            id,
            flags,
            checksum,
            data,
        })
    }

    /// Calculate the size of this entry in bytes
    fn size(&self) -> u64 {
        // ID(8) + Flags(1) + Checksum(4) + DataLength(4) + Data
        8 + 1 + 4 + 4 + self.data.len() as u64
    }

    /// Check if the entry is valid
    fn is_valid(&self) -> bool {
        self.compute_checksum(&self.data) == self.checksum
    }

    /// Compute checksum for data
    fn compute_checksum(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

/// State of the WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalState {
    /// WAL is closed
    Closed,
    /// WAL is open for reading and writing
    Open,
    /// WAL needs recovery
    NeedsRecovery,
}

/// Write-ahead log for ensuring durability
pub struct WriteAheadLog {
    /// Base directory for WAL files
    base_dir: PathBuf,
    /// Current WAL file
    current_file: Arc<RwLock<Option<File>>>,
    /// Current WAL file path
    current_path: Arc<RwLock<PathBuf>>,
    /// Current entry ID
    next_entry_id: AtomicU64,
    /// Current position in the WAL file
    position: AtomicU64,
    /// Current entry count
    entry_count: AtomicU64,
    /// Store configuration
    config: StoreConfig,
    /// WAL state
    state: Arc<RwLock<WalState>>,
    /// Write mutex to ensure only one writer at a time
    write_mutex: Mutex<()>,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
}

impl WriteAheadLog {
    /// Create a new WAL
    pub fn new<P: AsRef<Path>>(
        dir: P,
        config: &StoreConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Result<Self> {
        let base_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).map_err(|e| {
            Error::Wal(format!(
                "Failed to create WAL directory {}: {}",
                base_dir.display(),
                e
            ))
        })?;

        // Check for existing WAL files
        let mut wal_files = Self::find_wal_files(&base_dir)?;
        wal_files.sort(); // Sort by name to get the latest

        let (current_path, state) = if let Some(path) = wal_files.last() {
            // Check if the WAL file needs recovery
            if Self::check_needs_recovery(path)? {
                (path.clone(), WalState::NeedsRecovery)
            } else {
                // Create a new WAL file with a higher sequence number
                let next_seq = Self::extract_sequence_number(path)? + 1;
                let new_path = base_dir.join(format!("wal-{:010}.log", next_seq));
                (new_path, WalState::Open)
            }
        } else {
            // No existing WAL files, create the first one
            (base_dir.join("wal-0000000001.log"), WalState::Open)
        };

        let mut wal = Self {
            base_dir,
            current_file: Arc::new(RwLock::new(None)),
            current_path: Arc::new(RwLock::new(current_path)),
            next_entry_id: AtomicU64::new(1),
            position: AtomicU64::new(HEADER_SIZE),
            entry_count: AtomicU64::new(0),
            config: config.clone(),
            state: Arc::new(RwLock::new(state)),
            write_mutex: Mutex::new(()),
            metrics,
        };

        // Initialize the WAL file if state is Open
        if state == WalState::Open {
            wal.initialize_current_file()?;
        }

        Ok(wal)
    }

    /// Append a log entry to the WAL
    pub fn append(&self, entry: &LogEntry) -> Result<u64> {
        self.append_with_flags(entry, ENTRY_COMPLETE)
    }

    /// Ensure the WAL is open
    fn ensure_open(&self) -> Result<()> {
        match *self.state.read() {
            WalState::Open => Ok(()),
            WalState::Closed => Err(Error::Wal("WAL is closed".to_string())),
            WalState::NeedsRecovery => Err(Error::Wal("WAL needs recovery".to_string())),
        }
    }

    /// Append a log entry with specific flags
    pub fn append_with_flags(&self, entry: &LogEntry, flags: u8) -> Result<u64> {
        // Ensure WAL is open
        self.ensure_open()?;

        // Create WAL entry
        let id = self.next_entry_id.fetch_add(1, Ordering::SeqCst);
        let wal_entry = WalEntry::new(id, entry, flags)?;

        // Acquire write lock
        let _lock = self.write_mutex.lock();

        // Check if we need to rotate the WAL file
        let entry_size = wal_entry.size();
        if self.position.load(Ordering::SeqCst) + entry_size > MAX_WAL_SIZE {
            self.rotate_wal_file()?;
        }

        // Write the entry
        let start = Instant::now();
        self.write_entry(&wal_entry)?;
        let duration = start.elapsed();

        // Update metrics
        self.metrics.record_wal_write_duration(duration);
        self.metrics.add_wal_bytes_written(entry_size as usize);

        Ok(id)
    }

    /// Begin a multi-entry transaction
    pub fn begin_transaction(&self) -> Result<u64> {
        // Ensure WAL is open
        self.ensure_open()?;

        // Create a dummy entry to mark transaction start
        let id = self.next_entry_id.fetch_add(1, Ordering::SeqCst);
        let mut dummy_entry = LogEntry::default();
        dummy_entry.metadata.id = uuid::Uuid::nil(); // Use nil UUID for dummy entry

        // Append with transaction start flag
        self.append_with_flags(&dummy_entry, ENTRY_START)?;

        Ok(id)
    }

    /// End a multi-entry transaction
    pub fn end_transaction(&self, transaction_id: u64) -> Result<()> {
        // Ensure WAL is open
        self.ensure_open()?;

        // Create a dummy entry to mark transaction end
        let mut dummy_entry = LogEntry::default();
        dummy_entry.metadata.id = uuid::Uuid::nil(); // Use nil UUID for dummy entry

        // Append with transaction end flag and original transaction ID
        let wal_entry = WalEntry::new(transaction_id, &dummy_entry, ENTRY_END)?;

        // Acquire write lock
        let _lock = self.write_mutex.lock();

        // Write the entry
        self.write_entry(&wal_entry)?;

        // Sync to disk if configured
        if self.config.sync_writes {
            self.sync()?;
        }

        Ok(())
    }

    /// Check if the WAL needs recovery
    pub fn needs_recovery(&self) -> bool {
        *self.state.read() == WalState::NeedsRecovery
    }

    /// Perform best-effort recovery (may lose some data)
    pub fn recover_best_effort(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();

        // Ensure WAL state is NeedsRecovery
        if *self.state.read() != WalState::NeedsRecovery {
            return Ok(entries);
        }

        // Try to recover as many entries as possible
        let path = self.current_path.read().clone();
        let file = File::open(&path)
            .map_err(|e| Error::Wal(format!("Cannot open WAL file for recovery: {}", e)))?;

        let mut reader = BufReader::new(file);

        // Read and validate the header
        if !self.read_and_validate_header(&mut reader)? {
            return Ok(entries);
        }

        // Read entries until EOF or corruption
        let mut position = HEADER_SIZE;

        loop {
            match self.read_entry_at(&mut reader, position) {
                Ok(Some((entry, entry_size))) => {
                    // Validate the entry
                    if entry.is_valid() {
                        if entry.flags & ENTRY_COMPLETE == ENTRY_COMPLETE {
                            // Convert to LogEntry
                            match deserialize(&entry.data) {
                                Ok(log_entry) => {
                                    entries.push(log_entry);
                                }
                                Err(e) => {
                                    // Skip invalid entry but continue
                                    println!("Warning: Failed to deserialize WAL entry: {}", e);
                                }
                            }
                        }
                    } else {
                        // Found corrupt entry, stop recovery
                        break;
                    }

                    // Move to next entry
                    position += entry_size;
                }
                Ok(None) => {
                    // End of file
                    break;
                }
                Err(e) => {
                    // Error reading entry, stop recovery
                    println!("Warning: Error reading WAL entry: {}", e);
                    break;
                }
            }
        }

        // Update state
        *self.state.write() = WalState::Open;

        Ok(entries)
    }

    /// Perform consistent recovery (only recover complete transactions)
    pub fn recover_consistent(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();
        let mut transaction_entries: HashMap<u64, Vec<LogEntry>> = HashMap::new();
        let mut active_transaction: Option<u64> = None;

        // Ensure WAL state is NeedsRecovery
        if *self.state.read() != WalState::NeedsRecovery {
            return Ok(entries);
        }

        // Open the WAL file
        let path = self.current_path.read().clone();
        let file = File::open(&path)
            .map_err(|e| Error::Wal(format!("Cannot open WAL file for recovery: {}", e)))?;

        let mut reader = BufReader::new(file);

        // Read and validate the header
        if !self.read_and_validate_header(&mut reader)? {
            return Ok(entries);
        }

        // Read all entries
        let mut position = HEADER_SIZE;

        loop {
            match self.read_entry_at(&mut reader, position) {
                Ok(Some((entry, entry_size))) => {
                    // Validate the entry
                    if entry.is_valid() {
                        if entry.flags & ENTRY_START == ENTRY_START {
                            // Start of a transaction
                            active_transaction = Some(entry.id);
                            transaction_entries.insert(entry.id, Vec::new());
                        } else if entry.flags & ENTRY_END == ENTRY_END {
                            // End of a transaction
                            if let Some(txn_id) = active_transaction {
                                if txn_id == entry.id {
                                    // Move entries from transaction to main list
                                    if let Some(txn_entries) = transaction_entries.remove(&txn_id) {
                                        entries.extend(txn_entries);
                                    }
                                    active_transaction = None;
                                }
                            }
                        } else if entry.flags & ENTRY_COMPLETE == ENTRY_COMPLETE {
                            // Single-entry transaction
                            match deserialize(&entry.data) {
                                Ok(log_entry) => {
                                    if let Some(txn_id) = active_transaction {
                                        // Add to transaction
                                        if let Some(txn_entries) =
                                            transaction_entries.get_mut(&txn_id)
                                        {
                                            txn_entries.push(log_entry);
                                        }
                                    } else {
                                        // Add directly to entries
                                        entries.push(log_entry);
                                    }
                                }
                                Err(e) => {
                                    // Skip invalid entry but continue
                                    println!("Warning: Failed to deserialize WAL entry: {}", e);
                                }
                            }
                        }
                    } else {
                        // Found corrupt entry, stop recovery
                        break;
                    }

                    // Move to next entry
                    position += entry_size;
                }
                Ok(None) => {
                    // End of file
                    break;
                }
                Err(e) => {
                    // Error reading entry, stop recovery
                    println!("Warning: Error reading WAL entry: {}", e);
                    break;
                }
            }
        }

        // Update state
        *self.state.write() = WalState::Open;

        Ok(entries)
    }

    /// Perform conservative recovery (recover as much as possible)
    pub fn recover_conservative(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();

        // Try best-effort recovery first
        let recovered = self.recover_best_effort()?;
        entries.extend(recovered);

        // Try to recover additional entries from older WAL files
        let wal_files = Self::find_wal_files(&self.base_dir)?;

        for path in wal_files.iter().rev().skip(1) {
            // Skip the current file
            let file = match File::open(path) {
                Ok(f) => f,
                Err(_) => continue, // Skip files we can't open
            };

            let mut reader = BufReader::new(file);

            // Read and validate the header
            if !self.read_and_validate_header(&mut reader)? {
                continue;
            }

            // Read entries until EOF or corruption
            let mut position = HEADER_SIZE;

            loop {
                match self.read_entry_at(&mut reader, position) {
                    Ok(Some((entry, entry_size))) => {
                        // Validate the entry
                        if entry.is_valid() && entry.flags & ENTRY_COMPLETE == ENTRY_COMPLETE {
                            // Convert to LogEntry
                            match deserialize(&entry.data) {
                                Ok(log_entry) => {
                                    entries.push(log_entry);
                                }
                                Err(_) => {
                                    // Skip invalid entry
                                }
                            }
                        }

                        // Move to next entry
                        position += entry_size;
                    }
                    Ok(None) => break, // End of file
                    Err(_) => break,   // Error reading entry
                }
            }
        }

        Ok(entries)
    }

    /// Sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        let file_guard = self.current_file.read();
        if let Some(ref file) = *file_guard {
            file.sync_all()
                .map_err(|e| Error::Wal(format!("Failed to sync WAL file: {}", e)))?;
        }
        Ok(())
    }

    /// Clear the WAL
    pub fn clear(&self) -> Result<()> {
        // Close current file
        {
            let mut file_guard = self.current_file.write();
            *file_guard = None;
        }

        // Create a new WAL file
        let next_seq = self.next_sequence_number()?;
        let new_path = self.base_dir.join(format!("wal-{:010}.log", next_seq));

        // Update path
        {
            let mut path_guard = self.current_path.write();
            *path_guard = new_path;
        }

        // Reset state
        *self.state.write() = WalState::Open;
        self.position.store(HEADER_SIZE, Ordering::SeqCst);
        self.entry_count.store(0, Ordering::SeqCst);

        // Initialize new file
        self.initialize_current_file()?;

        Ok(())
    }

    /// Close the WAL
    pub fn close(&self) -> Result<()> {
        // Update state
        *self.state.write() = WalState::Closed;

        // Close current file
        {
            let mut file_guard = self.current_file.write();
            *file_guard = None;
        }

        Ok(())
    }

    // Helper methods

    /// Initialize the current WAL file
    fn initialize_current_file(&self) -> Result<()> {
        let path = self.current_path.read().clone();

        // Create or open the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                Error::Wal(format!(
                    "Failed to create WAL file {}: {}",
                    path.display(),
                    e
                ))
            })?;

        // Write header
        let mut writer = BufWriter::new(&file);
        writer.write_all(WAL_MAGIC)?;
        writer.write_u32::<LittleEndian>(WAL_VERSION)?;
        writer.write_u64::<LittleEndian>(0)?; // Entry count
        writer.flush()?;

        // Store the file
        {
            let mut file_guard = self.current_file.write();
            *file_guard = Some(file);
        }

        // Reset position and count
        self.position.store(HEADER_SIZE, Ordering::SeqCst);
        self.entry_count.store(0, Ordering::SeqCst);

        Ok(())
    }

    /// Rotate to a new WAL file
    fn rotate_wal_file(&self) -> Result<()> {
        // Sync current file
        self.sync()?;

        // Close current file
        {
            let mut file_guard = self.current_file.write();
            *file_guard = None;
        }

        // Create a new WAL file with next sequence number
        let next_seq = self.next_sequence_number()?;
        let new_path = self.base_dir.join(format!("wal-{:010}.log", next_seq));

        // Update path
        {
            let mut path_guard = self.current_path.write();
            *path_guard = new_path;
        }

        // Reset position and count
        self.position.store(HEADER_SIZE, Ordering::SeqCst);
        self.entry_count.store(0, Ordering::SeqCst);

        // Initialize new file
        self.initialize_current_file()?;

        Ok(())
    }

    /// Write a WAL entry to the current file
    fn write_entry(&self, entry: &WalEntry) -> Result<()> {
        let file_guard = self.current_file.read();

        if let Some(ref file) = *file_guard {
            let mut writer = BufWriter::new(file);

            // Seek to current position
            writer.seek(SeekFrom::Start(self.position.load(Ordering::SeqCst)))?;

            // Write entry
            writer.write_u64::<LittleEndian>(entry.id)?;
            writer.write_u8(entry.flags)?;
            writer.write_u32::<LittleEndian>(entry.checksum)?;
            writer.write_u32::<LittleEndian>(entry.data.len() as u32)?;
            writer.write_all(&entry.data)?;
            writer.flush()?;

            // Update position and count
            let entry_size = entry.size();
            self.position.fetch_add(entry_size, Ordering::SeqCst);
            let count = self.entry_count.fetch_add(1, Ordering::SeqCst) + 1;

            // Update entry count in header
            writer.seek(SeekFrom::Start(8))?; // Skip magic and version
            writer.write_u64::<LittleEndian>(count)?;
            writer.flush()?;

            // Sync to disk if configured
            if self.config.sync_writes {
                file.sync_data()?;
            }

            Ok(())
        } else {
            Err(Error::Wal("WAL file not open".to_string()))
        }
    }

    /// Read an entry at a specific position
    fn read_entry_at<R: Read + Seek>(
        &self,
        reader: &mut R,
        position: u64,
    ) -> Result<Option<(WalEntry, u64)>> {
        // Seek to position
        if let Err(e) = reader.seek(SeekFrom::Start(position)) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(Error::Wal(format!("Seek error: {}", e)));
        }

        // Read entry header
        let id = match reader.read_u64::<LittleEndian>() {
            Ok(id) => id,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(Error::Wal(format!("Read error: {}", e)));
            }
        };

        let flags = reader
            .read_u8()
            .map_err(|e| Error::Wal(format!("Read error: {}", e)))?;
        let checksum = reader
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::Wal(format!("Read error: {}", e)))?;
        let data_len = reader
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::Wal(format!("Read error: {}", e)))?;

        // Read data
        let mut data = vec![0; data_len as usize];
        reader
            .read_exact(&mut data)
            .map_err(|e| Error::Wal(format!("Read error: {}", e)))?;

        // Create WAL entry
        let entry = WalEntry {
            id,
            flags,
            checksum,
            data,
        };

        // Calculate entry size
        let entry_size = entry.size();

        Ok(Some((entry, entry_size)))
    }

    /// Read and validate the WAL header
fn read_and_validate_header<R: Read>(&self, reader: &mut R) -> Result<bool> {
    // Read magic
    let mut magic = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut magic) {
        return Err(Error::Wal(format!("Failed to read WAL magic: {}", e)));
    }

    if magic != *WAL_MAGIC {
        return Ok(false);
    }

    // Read version
    let version = match reader.read_u32::<LittleEndian>() {
        Ok(v) => v,
        Err(e) => return Err(Error::Wal(format!("Failed to read WAL version: {}", e))),
    };

    if version != WAL_VERSION {
        return Ok(false);
    }

    // Read entry count
    let _count = match reader.read_u64::<LittleEndian>() {
        Ok(c) => c,
        Err(e) => return Err(Error::Wal(format!("Failed to read WAL entry count: {}", e))),
    };
    
    // If we got here, the header is valid
    Ok(true)
}
}
