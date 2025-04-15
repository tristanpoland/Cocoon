//! ChrysalisRS extension integration
//!
//! This module provides integration with ChrysalisRS extension system,
//! allowing Cocoon to be used as a storage backend for ChrysalisRS logs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::any::Any;

use chrysalis_rs::{LogEntry, Extension, Error as ChrysalisError};
use crate::error::{Result, Error};
use crate::store::CocoonStore;
use crate::config::StoreConfig;

/// ChrysalisRS extension for Cocoon integration
pub struct CocoonExtension {
    /// The Cocoon store
    store: Arc<CocoonStore>,
    /// Store base directory
    base_dir: PathBuf,
    /// Extension is enabled
    enabled: bool,
    /// Extension name
    name: String,
}

impl CocoonExtension {
    /// Create a new Cocoon extension with default configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = CocoonStore::new(path.as_ref())?;
        
        Ok(Self {
            store: Arc::new(store),
            base_dir: path.as_ref().to_path_buf(),
            enabled: true,
            name: "cocoon_storage".to_string(),
        })
    }
    
    /// Create a new Cocoon extension with custom configuration
    pub fn with_config<P: AsRef<Path>>(path: P, config: StoreConfig) -> Result<Self> {
        let store = CocoonStore::with_config(path.as_ref(), config)?;
        
        Ok(Self {
            store: Arc::new(store),
            base_dir: path.as_ref().to_path_buf(),
            enabled: true,
            name: "cocoon_storage".to_string(),
        })
    }
    
    /// Set a custom name for the extension
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    
    /// Get a reference to the underlying store
    pub fn store(&self) -> &CocoonStore {
        &self.store
    }
    
    /// Get the base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl Extension for CocoonExtension {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn initialize(&mut self) -> std::result::Result<(), ChrysalisError> {
        // Nothing to do here, store is already initialized
        Ok(())
    }
    
    fn shutdown(&mut self) -> std::result::Result<(), ChrysalisError> {
        match self.store.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(ChrysalisError::Extension(format!("Failed to flush Cocoon store: {}", e))),
        }
    }
    
    fn is_enabled(&self) -> bool {
        self.enabled
    }
    
    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    
    fn process_log(&self, entry: &LogEntry) -> std::result::Result<(), ChrysalisError> {
        if !self.enabled {
            return Ok(());
        }
        
        match self.store.store(entry) {
            Ok(_) => Ok(()),
            Err(e) => Err(ChrysalisError::Extension(format!("Failed to store log in Cocoon: {}", e))),
        }
    }
}

/// Factory function to create a Cocoon extension
pub fn create_cocoon_extension<P: AsRef<Path>>(path: P) -> Result<CocoonExtension> {
    CocoonExtension::new(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrysalis_rs::{LogLevel, ExtensionRegistry};
    use tempfile::tempdir;
    
    #[test]
    fn test_extension_creation() -> Result<()> {
        let temp_dir = tempdir()?;
        
        let extension = CocoonExtension::new(temp_dir.path())?;
        
        assert_eq!(extension.name(), "cocoon_storage");
        assert!(extension.is_enabled());
        
        Ok(())
    }
    
    #[test]
    fn test_extension_with_name() -> Result<()> {
        let temp_dir = tempdir()?;
        
        let extension = CocoonExtension::new(temp_dir.path())?
            .with_name("custom_name");
        
        assert_eq!(extension.name(), "custom_name");
        
        Ok(())
    }
    
    #[test]
    fn test_chrysalis_integration() -> Result<()> {
        let temp_dir = tempdir()?;
        
        // Create extension
        let cocoon = CocoonExtension::new(temp_dir.path())?;
        
        // Create registry
        let mut registry = ExtensionRegistry::new();
        
        // Register extension
        registry.register(cocoon)
            .map_err(|e| Error::Extension(format!("Failed to register extension: {}", e)))?;
        
        // Initialize extensions
        registry.initialize_all()
            .map_err(|e| Error::Extension(format!("Failed to initialize extensions: {}", e)))?;
        
        // Create a log entry
        let mut entry = LogEntry::new("Test log message", LogLevel::Info);
        entry.add_context("test_key", "test_value")
            .map_err(|e| Error::Extension(format!("Failed to add context: {}", e)))?;
        
        // Process log entry (should be stored in Cocoon)
        // In a real application, this would be done by ChrysalisRS
        
        // Get the Cocoon extension
        let ext = registry.get("cocoon_storage")
            .ok_or_else(|| Error::Extension("Extension not found".into()))?;
        
        // Process the log
        ext.process_log(&entry)
            .map_err(|e| Error::Extension(format!("Failed to process log: {}", e)))?;
        
        // Retrieve the extension
        let cocoon_ext = ext.as_any().downcast_ref::<CocoonExtension>()
            .ok_or_else(|| Error::Extension("Failed to downcast extension".into()))?;
        
        // Query the store to verify log was stored
        let logs = cocoon_ext.store().query()
            .containing_text("Test log message")
            .execute()?;
        
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "Test log message");
        
        // Shutdown extensions
        registry.shutdown_all()
            .map_err(|e| Error::Extension(format!("Failed to shutdown extensions: {}", e)))?;
        
        Ok(())
    }
}