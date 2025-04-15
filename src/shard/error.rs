//! Error types for the shard module
//!
//! Defines error types specific to shard operations.

use std::path::PathBuf;
use std::io;
use std::fmt;
use thiserror::Error;

/// Errors that can occur during shard operations
#[derive(Error, Debug)]
pub enum ShardError {
    /// Error when shard is not found
    #[error("Shard not found: {0}")]
    NotFound(String),
    
    /// Error when shard ID is invalid
    #[error("Invalid shard ID: {0}")]
    InvalidId(String),
    
    /// Error when segment operations fail
    #[error("Segment error: {0}")]
    Segment(String),
    
    /// Error when index operations fail
    #[error("Index error: {0}")]
    Index(String),
    
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    /// File error
    #[error("File error for {path:?}: {message}")]
    File {
        path: PathBuf,
        message: String,
    },
    
    /// Disk space error
    #[error("Disk space error: {0}")]
    DiskSpace(String),
    
    /// Permission error
    #[error("Permission error: {0}")]
    Permission(String),
    
    /// Shard is closed
    #[error("Shard manager is closed")]
    Closed,
    
    /// Shard is inactive
    #[error("Shard is inactive: {0}")]
    Inactive(String),
    
    /// Shard compaction error
    #[error("Compaction error: {0}")]
    Compaction(String),
    
    /// Timestamp parsing error
    #[error("Timestamp error: {0}")]
    Timestamp(String),
    
    /// Generic error
    #[error("Shard error: {0}")]
    Other(String),
}

/// Result type for shard operations
pub type ShardResult<T> = std::result::Result<T, ShardError>;

impl ShardError {
    /// Create a new file error
    pub fn file_error(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::File {
            path: path.into(),
            message: message.into(),
        }
    }
    
    /// Create a new shard not found error
    pub fn not_found(shard_id: impl Into<String>) -> Self {
        Self::NotFound(shard_id.into())
    }
    
    /// Create a new invalid shard ID error
    pub fn invalid_id(message: impl Into<String>) -> Self {
        Self::InvalidId(message.into())
    }
    
    /// Create a new segment error
    pub fn segment_error(message: impl Into<String>) -> Self {
        Self::Segment(message.into())
    }
    
    /// Create a new index error
    pub fn index_error(message: impl Into<String>) -> Self {
        Self::Index(message.into())
    }
    
    /// Create a new serialization error
    pub fn serialization_error(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }
    
    /// Create a new disk space error
    pub fn disk_space_error(message: impl Into<String>) -> Self {
        Self::DiskSpace(message.into())
    }
    
    /// Create a new permission error
    pub fn permission_error(message: impl Into<String>) -> Self {
        Self::Permission(message.into())
    }
    
    /// Create a new inactive shard error
    pub fn inactive_error(shard_id: impl Into<String>) -> Self {
        Self::Inactive(shard_id.into())
    }
    
    /// Create a new compaction error
    pub fn compaction_error(message: impl Into<String>) -> Self {
        Self::Compaction(message.into())
    }
    
    /// Create a new timestamp error
    pub fn timestamp_error(message: impl Into<String>) -> Self {
        Self::Timestamp(message.into())
    }
    
    /// Create a new generic error
    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }
    
    /// Check if this is a not found error
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }
    
    /// Check if this is an I/O error
    pub fn is_io_error(&self) -> bool {
        matches!(self, Self::Io(_))
    }
    
    /// Check if this is a closed error
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
    
    /// Check if this is a permission error
    pub fn is_permission_error(&self) -> bool {
        matches!(self, Self::Permission(_))
    }
    
    /// Check if this is a disk space error
    pub fn is_disk_space_error(&self) -> bool {
        matches!(self, Self::DiskSpace(_))
    }
    
    /// Get a description of the error
    pub fn description(&self) -> String {
        match self {
            Self::NotFound(id) => format!("Shard not found: {}", id),
            Self::InvalidId(msg) => format!("Invalid shard ID: {}", msg),
            Self::Segment(msg) => format!("Segment error: {}", msg),
            Self::Index(msg) => format!("Index error: {}", msg),
            Self::Serialization(msg) => format!("Serialization error: {}", msg),
            Self::Io(err) => format!("I/O error: {}", err),
            Self::File { path, message } => format!("File error for {:?}: {}", path, message),
            Self::DiskSpace(msg) => format!("Disk space error: {}", msg),
            Self::Permission(msg) => format!("Permission error: {}", msg),
            Self::Closed => "Shard manager is closed".to_string(),
            Self::Inactive(id) => format!("Shard is inactive: {}", id),
            Self::Compaction(msg) => format!("Compaction error: {}", msg),
            Self::Timestamp(msg) => format!("Timestamp error: {}", msg),
            Self::Other(msg) => format!("Shard error: {}", msg),
        }
    }
}

impl From<crate::segment::SegmentError> for ShardError {
    fn from(err: crate::segment::SegmentError) -> Self {
        Self::Segment(err.to_string())
    }
}

impl From<crate::index::IndexError> for ShardError {
    fn from(err: crate::index::IndexError) -> Self {
        Self::Index(err.to_string())
    }
}

impl From<crate::error::Error> for ShardError {
    fn from(err: crate::error::Error) -> Self {
        match err {
            crate::error::Error::Storage(msg) => Self::Other(msg),
            crate::error::Error::Io(err) => Self::Io(err),
            crate::error::Error::Serialization(err) => Self::Serialization(err.to_string()),
            crate::error::Error::Index(msg) => Self::Index(msg),
            crate::error::Error::Segment { path, message } => Self::File {
                path,
                message,
            },
            crate::error::Error::Transaction(msg) => Self::Other(msg),
            crate::error::Error::Query(msg) => Self::Other(msg),
            crate::error::Error::Wal(msg) => Self::Other(msg),
            crate::error::Error::Compression(msg) => Self::Other(msg),
            crate::error::Error::Extension(msg) => Self::Other(msg),
            crate::error::Error::Config(msg) => Self::Other(msg),
            crate::error::Error::Corruption(msg) => Self::Other(msg),
            crate::error::Error::Chrysalis(err) => Self::Other(err.to_string()),
            crate::error::Error::Other(msg) => Self::Other(msg),
            crate::error::Error::Shard(msg) => Self::Other(msg),
            crate::error::Error::Timestamp(msg) => Self::Timestamp(msg),
            crate::error::Error::Permission(msg) => Self::Permission(msg),
            crate::error::Error::DiskSpace(msg) => Self::DiskSpace(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shard_error_creation() {
        // Test file error
        let err = ShardError::file_error("/path/to/file", "file not found");
        assert!(matches!(err, ShardError::File { path, message } if path == PathBuf::from("/path/to/file") && message == "file not found"));
        
        // Test not found error
        let err = ShardError::not_found("shard123");
        assert!(matches!(err, ShardError::NotFound(id) if id == "shard123"));
        
        // Test is_not_found
        assert!(err.is_not_found());
        
        // Test description
        assert_eq!(err.description(), "Shard not found: shard123");
    }
    
    #[test]
    fn test_shard_error_from_io() {
        // Create an IO error
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = ShardError::from(io_err);
        
        // Check conversion
        assert!(matches!(err, ShardError::Io(_)));
        assert!(err.is_io_error());
    }
}