//! Error handling for Cocoon
//!
//! This module provides error types and result aliases for Cocoon operations.

use std::path::PathBuf;
use std::io;
use std::fmt;
use thiserror::Error;

/// Errors that can occur in Cocoon operations
#[derive(Error, Debug)]
pub enum Error {
    /// Errors related to storage operations
    #[error("Storage error: {0}")]
    Storage(String),
    
    /// Errors related to I/O operations
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    /// Errors related to serialization/deserialization
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// Errors related to index operations
    #[error("Index error: {0}")]
    Index(String),
    
    /// Errors related to segment operations
    #[error("Segment error for {path:?}: {message}")]
    Segment {
        path: PathBuf,
        message: String,
    },
    
    /// Errors related to transaction operations
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    /// Errors related to query operations
    #[error("Query error: {0}")]
    Query(String),
    
    /// Errors related to WAL operations
    #[error("WAL error: {0}")]
    Wal(String),
    
    /// Errors related to compression
    #[error("Compression error: {0}")]
    Compression(String),
    
    /// Errors related to the extension
    #[error("Extension error: {0}")]
    Extension(String),
    
    /// Errors related to configuration
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// Errors related to data corruption
    #[error("Data corruption detected: {0}")]
    Corruption(String),
    
    /// Errors related to shard operations
    #[error("Shard error: {0}")]
    Shard(String),
    
    /// Errors related to timestamp operations
    #[error("Timestamp error: {0}")]
    Timestamp(String),
    
    /// Errors related to permission issues
    #[error("Permission error: {0}")]
    Permission(String),
    
    /// Errors related to resource limitations
    #[error("Resource error: {0}")]
    Resource(String),
    
    /// Errors related to ChrysalisRS
    #[error("ChrysalisRS error: {0}")]
    Chrysalis(#[from] chrysalis_rs::Error),
    
    /// Generic error type for other cases
    #[error("{0}")]
    Other(String),
}

/// Result type for Cocoon operations
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Create a new storage error
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage(message.into())
    }
    
    /// Create a new segment error
    pub fn segment(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Segment {
            path: path.into(),
            message: message.into(),
        }
    }
    
    /// Create a new index error
    pub fn index(message: impl Into<String>) -> Self {
        Self::Index(message.into())
    }
    
    /// Create a new transaction error
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction(message.into())
    }
    
    /// Create a new query error
    pub fn query(message: impl Into<String>) -> Self {
        Self::Query(message.into())
    }
    
    /// Create a new WAL error
    pub fn wal(message: impl Into<String>) -> Self {
        Self::Wal(message.into())
    }
    
    /// Create a new compression error
    pub fn compression(message: impl Into<String>) -> Self {
        Self::Compression(message.into())
    }
    
    /// Create a new extension error
    pub fn extension(message: impl Into<String>) -> Self {
        Self::Extension(message.into())
    }
    
    /// Create a new configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }
    
    /// Create a new corruption error
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption(message.into())
    }
    
    /// Create a new shard error
    pub fn shard(message: impl Into<String>) -> Self {
        Self::Shard(message.into())
    }
    
    /// Create a new timestamp error
    pub fn timestamp(message: impl Into<String>) -> Self {
        Self::Timestamp(message.into())
    }
    
    /// Create a new permission error
    pub fn permission(message: impl Into<String>) -> Self {
        Self::Permission(message.into())
    }
    
    /// Create a new resource error
    pub fn resource(message: impl Into<String>) -> Self {
        Self::Resource(message.into())
    }
    
    /// Create a new generic error
    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }
    
    /// Check if this is an I/O error
    pub fn is_io_error(&self) -> bool {
        matches!(self, Self::Io(_))
    }
    
    /// Check if this is a serialization error
    pub fn is_serialization_error(&self) -> bool {
        matches!(self, Self::Serialization(_))
    }
    
    /// Check if this is a corruption error
    pub fn is_corruption_error(&self) -> bool {
        matches!(self, Self::Corruption(_))
    }
    
    /// Check if this is a permission error
    pub fn is_permission_error(&self) -> bool {
        matches!(self, Self::Permission(_))
    }
    
    /// Check if this is a resource error (e.g., out of disk space)
    pub fn is_resource_error(&self) -> bool {
        matches!(self, Self::Resource(_))
    }
    
    /// Get a developer-friendly description of the error
    pub fn dev_description(&self) -> String {
        match self {
            Self::Storage(msg) => format!("Storage error: {}", msg),
            Self::Io(err) => format!("I/O error: {}", err),
            Self::Serialization(err) => format!("Serialization error: {}", err),
            Self::Index(msg) => format!("Index error: {}", msg),
            Self::Segment { path, message } => format!("Segment error for {:?}: {}", path, message),
            Self::Transaction(msg) => format!("Transaction error: {}", msg),
            Self::Query(msg) => format!("Query error: {}", msg),
            Self::Wal(msg) => format!("WAL error: {}", msg),
            Self::Compression(msg) => format!("Compression error: {}", msg),
            Self::Extension(msg) => format!("Extension error: {}", msg),
            Self::Config(msg) => format!("Configuration error: {}", msg),
            Self::Corruption(msg) => format!("Data corruption detected: {}", msg),
            Self::Shard(msg) => format!("Shard error: {}", msg),
            Self::Timestamp(msg) => format!("Timestamp error: {}", msg),
            Self::Permission(msg) => format!("Permission error: {}", msg),
            Self::Resource(msg) => format!("Resource error: {}", msg),
            Self::Chrysalis(err) => format!("ChrysalisRS error: {}", err),
            Self::Other(msg) => format!("Error: {}", msg),
        }
    }
    
    /// Get a user-friendly suggestion for resolving the error
    pub fn suggestion(&self) -> Option<String> {
        match self {
            Self::Storage(_) => Some("Check if the storage path exists and is writable".to_string()),
            Self::Io(err) if err.kind() == io::ErrorKind::NotFound => {
                Some("The specified file or directory does not exist".to_string())
            }
            Self::Io(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                Some("You don't have permission to access this file or directory".to_string())
            }
            Self::Resource(_) => Some("Check available disk space or system resources".to_string()),
            Self::Permission(_) => Some("Verify permissions on the log storage directory".to_string()),
            Self::Corruption(_) => Some("Data corruption detected. Consider restoring from a backup".to_string()),
            _ => None,
        }
    }
}

// Conversion from bincode error to Cocoon error
impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::Serialization(serde_json::Error::custom(format!("Bincode error: {}", err)))
    }
}

// Conversion from zstd error to Cocoon error
impl From<zstd::Error> for Error {
    fn from(err: zstd::Error) -> Self {
        Self::Compression(format!("Zstd error: {}", err))
    }
}

// Conversion from lz4 error to Cocoon error
impl From<lz4::Error> for Error {
    fn from(err: lz4::Error) -> Self {
        Self::Compression(format!("LZ4 error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_creation() {
        // Test various error creation methods
        let storage_err = Error::storage("Failed to open file");
        assert!(matches!(storage_err, Error::Storage(_)));
        
        let segment_err = Error::segment("/path/to/segment", "Segment not found");
        assert!(matches!(segment_err, Error::Segment { .. }));
        
        let index_err = Error::index("Index corrupted");
        assert!(matches!(index_err, Error::Index(_)));
    }
    
    #[test]
    fn test_error_conversion() {
        // Test conversion from io::Error
        let io_err = io::Error::new(io::ErrorKind::NotFound, "File not found");
        let cocoon_err = Error::from(io_err);
        assert!(matches!(cocoon_err, Error::Io(_)));
        assert!(cocoon_err.is_io_error());
        
        // Test conversion from serde_json::Error
        let json_err = serde_json::Error::custom("Invalid JSON");
        let cocoon_err = Error::from(json_err);
        assert!(matches!(cocoon_err, Error::Serialization(_)));
        assert!(cocoon_err.is_serialization_error());
    }
    
    #[test]
    fn test_error_description_and_suggestion() {
        let err = Error::corruption("Index file corrupt");
        assert!(err.dev_description().contains("Data corruption detected"));
        assert!(err.suggestion().unwrap().contains("backup"));
        
        let err = Error::resource("Out of disk space");
        assert!(err.dev_description().contains("Resource error"));
        assert!(err.suggestion().unwrap().contains("disk space"));
    }
}