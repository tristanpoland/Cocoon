//! Compression utilities for Cocoon
//!
//! This module handles compression and decompression of log data
//! using various algorithms based on configuration settings.

use std::io::{self, Read, Write};

use lz4::{EncoderBuilder as Lz4EncoderBuilder, Decoder as Lz4Decoder};
use zstd::{Encoder as ZstdEncoder, Decoder as ZstdDecoder};

use crate::error::{Result, Error};
use crate::config::CompressionAlgorithm;

/// Compression interface for compressing log data
pub trait Compressor: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
    
    /// Get compression algorithm
    fn algorithm(&self) -> CompressionAlgorithm;
    
    /// Get compression level
    fn level(&self) -> i32;
}

/// Decompression interface for decompressing log data
pub trait Decompressor: Send + Sync {
    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    
    /// Get compression algorithm
    fn algorithm(&self) -> CompressionAlgorithm;
}

/// Factory for creating compressors and decompressors
pub struct CompressionFactory;

impl CompressionFactory {
    /// Create a new compressor based on algorithm and level
    pub fn create_compressor(algorithm: CompressionAlgorithm, level: i32) -> Box<dyn Compressor> {
        match algorithm {
            CompressionAlgorithm::None => Box::new(NoCompression),
            CompressionAlgorithm::Lz4 => Box::new(Lz4Compression::new(level)),
            CompressionAlgorithm::Zstd => Box::new(ZstdCompression::new(level)),
        }
    }
    
    /// Create a new decompressor based on algorithm
    pub fn create_decompressor(algorithm: CompressionAlgorithm) -> Box<dyn Decompressor> {
        match algorithm {
            CompressionAlgorithm::None => Box::new(NoCompression),
            CompressionAlgorithm::Lz4 => Box::new(Lz4Decompression),
            CompressionAlgorithm::Zstd => Box::new(ZstdDecompression),
        }
    }
}

/// No compression (passthrough)
pub struct NoCompression;

impl Compressor for NoCompression {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }
    
    fn level(&self) -> i32 {
        0
    }
}

impl Decompressor for NoCompression {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }
}

/// LZ4 compression
pub struct Lz4Compression {
    level: i32,
}

impl Lz4Compression {
    /// Create a new LZ4 compressor with specified level
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(0, 9),
        }
    }
}

impl Compressor for Lz4Compression {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = Lz4EncoderBuilder::new()
            .level(self.level)
            .build(Vec::new())?;
        
        encoder.write_all(data)?;
        
        let (compressed, result) = encoder.finish();
        result?;
        
        Ok(compressed)
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }
    
    fn level(&self) -> i32 {
        self.level
    }
}

/// LZ4 decompression
pub struct Lz4Decompression;

impl Decompressor for Lz4Decompression {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = Lz4Decoder::new(data)?;
        let mut decompressed = Vec::new();
        
        decoder.read_to_end(&mut decompressed)?;
        
        Ok(decompressed)
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }
}

/// Zstandard compression
pub struct ZstdCompression {
    level: i32,
}

impl ZstdCompression {
    /// Create a new Zstandard compressor with specified level
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(0, 9),
        }
    }
}

impl Compressor for ZstdCompression {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = ZstdEncoder::new(&mut compressed, self.level.clamp(1, 22))?;
        
        encoder.write_all(data)?;
        encoder.finish()?;
        
        Ok(compressed)
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }
    
    fn level(&self) -> i32 {
        self.level
    }
}

/// Zstandard decompression
pub struct ZstdDecompression;

impl Decompressor for ZstdDecompression {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = ZstdDecoder::new(data)?;
        
        decoder.read_to_end(&mut decompressed)?;
        
        Ok(decompressed)
    }
    
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }
}

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Compression level used
    pub level: i32,
    /// Compression ratio (original / compressed)
    pub ratio: f64,
}

impl CompressionStats {
    /// Create new compression stats
    pub fn new(
        original_size: usize,
        compressed_size: usize,
        algorithm: CompressionAlgorithm,
        level: i32,
    ) -> Self {
        let ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            1.0
        };
        
        Self {
            original_size,
            compressed_size,
            algorithm,
            level,
            ratio,
        }
    }
    
    /// Calculate space savings as a percentage
    pub fn space_savings(&self) -> f64 {
        (1.0 - (1.0 / self.ratio)) * 100.0
    }
}

/// Utility functions for compression
pub mod util {
    use super::*;
    
    /// Compress data with the specified algorithm and level
    pub fn compress(data: &[u8], algorithm: CompressionAlgorithm, level: i32) -> Result<Vec<u8>> {
        let compressor = CompressionFactory::create_compressor(algorithm, level);
        compressor.compress(data)
    }
    
    /// Decompress data with the specified algorithm
    pub fn decompress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let decompressor = CompressionFactory::create_decompressor(algorithm);
        decompressor.decompress(data)
    }
    
    /// Compress data and return compression statistics
    pub fn compress_with_stats(
        data: &[u8],
        algorithm: CompressionAlgorithm,
        level: i32,
    ) -> Result<(Vec<u8>, CompressionStats)> {
        let original_size = data.len();
        let compressed = compress(data, algorithm, level)?;
        let compressed_size = compressed.len();
        
        let stats = CompressionStats::new(
            original_size,
            compressed_size,
            algorithm,
            level,
        );
        
        Ok((compressed, stats))
    }
    
    /// Add a compression header to compressed data
    pub fn add_compression_header(
        compressed: Vec<u8>,
        algorithm: CompressionAlgorithm,
        level: i32,
    ) -> Vec<u8> {
        let mut result = Vec::with_capacity(compressed.len() + 2);
        
        // Add algorithm byte
        result.push(match algorithm {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
            CompressionAlgorithm::Zstd => 2,
        });
        
        // Add level byte
        result.push(level as u8);
        
        // Add compressed data
        result.extend_from_slice(&compressed);
        
        result
    }
    
    /// Parse a compression header from data
    pub fn parse_compression_header(data: &[u8]) -> Result<(CompressionAlgorithm, i32, &[u8])> {
        if data.len() < 2 {
            return Err(Error::compression("Invalid compression header: too short"));
        }
        
        // Parse algorithm
        let algorithm = match data[0] {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            2 => CompressionAlgorithm::Zstd,
            _ => return Err(Error::compression(format!("Unknown compression algorithm: {}", data[0]))),
        };
        
        // Parse level
        let level = data[1] as i32;
        
        // Return algorithm, level, and the rest of the data
        Ok((algorithm, level, &data[2..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    /// Test data for compression
    const TEST_DATA: &[u8] = b"This is some test data that will be compressed. It should be reasonably long to demonstrate compression effectiveness. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.";
    
    #[test]
    fn test_no_compression() {
        let compressor = NoCompression;
        let compressed = compressor.compress(TEST_DATA).unwrap();
        
        // No compression should return the same data
        assert_eq!(compressed, TEST_DATA);
        
        // Decompress
        let decompressor = NoCompression;
        let decompressed = decompressor.decompress(&compressed).unwrap();
        
        // Should be the same as the original
        assert_eq!(decompressed, TEST_DATA);
    }
    
    #[test]
    fn test_lz4_compression() {
        let compressor = Lz4Compression::new(6);
        let compressed = compressor.compress(TEST_DATA).unwrap();
        
        // Compressed data should be smaller
        assert!(compressed.len() < TEST_DATA.len());
        
        // Decompress
        let decompressor = Lz4Decompression;
        let decompressed = decompressor.decompress(&compressed).unwrap();
        
        // Should be the same as the original
        assert_eq!(decompressed, TEST_DATA);
    }
    
    #[test]
    fn test_zstd_compression() {
        let compressor = ZstdCompression::new(3);
        let compressed = compressor.compress(TEST_DATA).unwrap();
        
        // Compressed data should be smaller
        assert!(compressed.len() < TEST_DATA.len());
        
        // Decompress
        let decompressor = ZstdDecompression;
        let decompressed = decompressor.decompress(&compressed).unwrap();
        
        // Should be the same as the original
        assert_eq!(decompressed, TEST_DATA);
    }
    
    #[test]
    fn test_compression_factory() {
        // Test creating compressors
        let none_compressor = CompressionFactory::create_compressor(CompressionAlgorithm::None, 0);
        assert_eq!(none_compressor.algorithm(), CompressionAlgorithm::None);
        
        let lz4_compressor = CompressionFactory::create_compressor(CompressionAlgorithm::Lz4, 5);
        assert_eq!(lz4_compressor.algorithm(), CompressionAlgorithm::Lz4);
        assert_eq!(lz4_compressor.level(), 5);
        
        let zstd_compressor = CompressionFactory::create_compressor(CompressionAlgorithm::Zstd, 7);
        assert_eq!(zstd_compressor.algorithm(), CompressionAlgorithm::Zstd);
        assert_eq!(zstd_compressor.level(), 7);
        
        // Test creating decompressors
        let none_decompressor = CompressionFactory::create_decompressor(CompressionAlgorithm::None);
        assert_eq!(none_decompressor.algorithm(), CompressionAlgorithm::None);
        
        let lz4_decompressor = CompressionFactory::create_decompressor(CompressionAlgorithm::Lz4);
        assert_eq!(lz4_decompressor.algorithm(), CompressionAlgorithm::Lz4);
        
        let zstd_decompressor = CompressionFactory::create_decompressor(CompressionAlgorithm::Zstd);
        assert_eq!(zstd_decompressor.algorithm(), CompressionAlgorithm::Zstd);
    }
    
    #[test]
    fn test_compression_utils() {
        // Test compress/decompress
        let compressed = util::compress(TEST_DATA, CompressionAlgorithm::Zstd, 3).unwrap();
        let decompressed = util::decompress(&compressed, CompressionAlgorithm::Zstd).unwrap();
        assert_eq!(decompressed, TEST_DATA);
        
        // Test compress with stats
        let (compressed, stats) = util::compress_with_stats(
            TEST_DATA,
            CompressionAlgorithm::Zstd,
            3,
        ).unwrap();
        
        assert_eq!(stats.original_size, TEST_DATA.len());
        assert_eq!(stats.compressed_size, compressed.len());
        assert_eq!(stats.algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(stats.level, 3);
        assert!(stats.ratio > 1.0); // Should compress somewhat
        
        // Test compression header
        let with_header = util::add_compression_header(
            compressed.clone(),
            CompressionAlgorithm::Zstd,
            3,
        );
        
        let (parsed_algo, parsed_level, parsed_data) = util::parse_compression_header(&with_header).unwrap();
        assert_eq!(parsed_algo, CompressionAlgorithm::Zstd);
        assert_eq!(parsed_level, 3);
        assert_eq!(parsed_data, compressed);
    }
    
    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new(
            1000,
            200,
            CompressionAlgorithm::Zstd,
            3,
        );
        
        assert_eq!(stats.original_size, 1000);
        assert_eq!(stats.compressed_size, 200);
        assert_eq!(stats.algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(stats.level, 3);
        assert_eq!(stats.ratio, 5.0);
        assert_eq!(stats.space_savings(), 80.0);
    }
}