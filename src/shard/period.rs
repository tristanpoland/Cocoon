//! Shard time period implementation
//!
//! Defines different time periods for shards and provides functionality
//! for creating shard IDs and parsing them.

use std::fmt;
use chrono::{DateTime, Utc, TimeZone, Duration as ChronoDuration, Datelike};
use crate::shard::{ShardId, ShardResult, ShardError};

/// Shard time period
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardPeriod {
    /// Daily shard (one day of logs)
    Day,
    /// Weekly shard (one week of logs)
    Week,
    /// Monthly shard (one month of logs)
    Month,
    /// Yearly shard (one year of logs)
    Year,
}

impl fmt::Display for ShardPeriod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardPeriod::Day => write!(f, "day"),
            ShardPeriod::Week => write!(f, "week"),
            ShardPeriod::Month => write!(f, "month"),
            ShardPeriod::Year => write!(f, "year"),
        }
    }
}

impl ShardPeriod {
    /// Create a shard ID from a timestamp
    pub fn create_shard_id(&self, timestamp: &DateTime<Utc>) -> ShardId {
        match self {
            ShardPeriod::Day => {
                format!("{:04}{:02}{:02}", timestamp.year(), timestamp.month(), timestamp.day())
            },
            ShardPeriod::Week => {
                // Get the Monday of the week
                let weekday = timestamp.weekday().num_days_from_monday();
                let monday = *timestamp - ChronoDuration::days(weekday as i64);
                format!("{:04}{:02}{:02}_week", monday.year(), monday.month(), monday.day())
            },
            ShardPeriod::Month => {
                format!("{:04}{:02}", timestamp.year(), timestamp.month())
            },
            ShardPeriod::Year => {
                format!("{:04}", timestamp.year())
            },
        }
    }
    
    /// Parse a shard ID to get the time range
    pub fn parse_shard_id(&self, shard_id: &str) -> ShardResult<(DateTime<Utc>, DateTime<Utc>)> {
        match self {
            ShardPeriod::Day => {
                if shard_id.len() != 8 {
                    return Err(ShardError::InvalidId(format!("Invalid day shard ID: {}", shard_id)));
                }
                
                let year = shard_id[0..4].parse::<i32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                let month = shard_id[4..6].parse::<u32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid month in shard ID: {}", shard_id))
                })?;
                
                let day = shard_id[6..8].parse::<u32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid day in shard ID: {}", shard_id))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).single().ok_or_else(|| {
                    ShardError::InvalidId(format!("Invalid date in shard ID: {}", shard_id))
                })?;
                
                let end = start + ChronoDuration::days(1);
                
                Ok((start, end))
            },
            ShardPeriod::Week => {
                if !shard_id.ends_with("_week") || shard_id.len() != 13 {
                    return Err(ShardError::InvalidId(format!("Invalid week shard ID: {}", shard_id)));
                }
                
                let date_part = &shard_id[0..8];
                
                let year = date_part[0..4].parse::<i32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                let month = date_part[4..6].parse::<u32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid month in shard ID: {}", shard_id))
                })?;
                
                let day = date_part[6..8].parse::<u32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid day in shard ID: {}", shard_id))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).single().ok_or_else(|| {
                    ShardError::InvalidId(format!("Invalid date in shard ID: {}", shard_id))
                })?;
                
                let end = start + ChronoDuration::days(7);
                
                Ok((start, end))
            },
            ShardPeriod::Month => {
                if shard_id.len() != 6 {
                    return Err(ShardError::InvalidId(format!("Invalid month shard ID: {}", shard_id)));
                }
                
                let year = shard_id[0..4].parse::<i32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                let month = shard_id[4..6].parse::<u32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid month in shard ID: {}", shard_id))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).single().ok_or_else(|| {
                    ShardError::InvalidId(format!("Invalid date in shard ID: {}", shard_id))
                })?;
                
                // Get the first day of the next month
                let end = if month == 12 {
                    Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single().ok_or_else(|| {
                        ShardError::InvalidId(format!("Invalid date in shard ID: {}", shard_id))
                    })?
                } else {
                    Utc.with_ymd_and_hms(year, month + 1, 1, 0, 0, 0).single().ok_or_else(|| {
                        ShardError::InvalidId(format!("Invalid date in shard ID: {}", shard_id))
                    })?
                };
                
                Ok((start, end))
            },
            ShardPeriod::Year => {
                if shard_id.len() != 4 {
                    return Err(ShardError::InvalidId(format!("Invalid year shard ID: {}", shard_id)));
                }
                
                let year = shard_id.parse::<i32>().map_err(|_| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                let start = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().ok_or_else(|| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                let end = Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single().ok_or_else(|| {
                    ShardError::InvalidId(format!("Invalid year in shard ID: {}", shard_id))
                })?;
                
                Ok((start, end))
            },
        }
    }
    
    /// Get the next finer time period
    pub fn finer(&self) -> Option<Self> {
        match self {
            Self::Year => Some(Self::Month),
            Self::Month => Some(Self::Week),
            Self::Week => Some(Self::Day),
            Self::Day => None,
        }
    }
    
    /// Get the next coarser time period
    pub fn coarser(&self) -> Option<Self> {
        match self {
            Self::Day => Some(Self::Week),
            Self::Week => Some(Self::Month),
            Self::Month => Some(Self::Year),
            Self::Year => None,
        }
    }
    
    /// Get all shard periods
    pub fn all() -> [Self; 4] {
        [Self::Day, Self::Week, Self::Month, Self::Year]
    }
    
    /// Get the default shard period
    pub fn default_period() -> Self {
        Self::Day
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shard_period_id_creation() {
        let timestamp = Utc.with_ymd_and_hms(2023, 5, 15, 12, 30, 0).unwrap();
        
        // Test day shard ID
        let day_id = ShardPeriod::Day.create_shard_id(&timestamp);
        assert_eq!(day_id, "20230515");
        
        // Test week shard ID
        let week_id = ShardPeriod::Week.create_shard_id(&timestamp);
        // May 15, 2023 was a Monday
        assert_eq!(week_id, "20230515_week");
        
        // Test month shard ID
        let month_id = ShardPeriod::Month.create_shard_id(&timestamp);
        assert_eq!(month_id, "202305");
        
        // Test year shard ID
        let year_id = ShardPeriod::Year.create_shard_id(&timestamp);
        assert_eq!(year_id, "2023");
    }
    
    #[test]
    fn test_shard_period_id_parsing() -> ShardResult<()> {
        // Test day shard ID
        let (start, end) = ShardPeriod::Day.parse_shard_id("20230515")?;
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 15, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 5, 16, 0, 0, 0).unwrap());
        
        // Test week shard ID
        let (start, end) = ShardPeriod::Week.parse_shard_id("20230515_week")?;
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 15, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 5, 22, 0, 0, 0).unwrap());
        
        // Test month shard ID
        let (start, end) = ShardPeriod::Month.parse_shard_id("202305")?;
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 5, 1, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap());
        
        // Test year shard ID
        let (start, end) = ShardPeriod::Year.parse_shard_id("2023")?;
        assert_eq!(start, Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());
        
        Ok(())
    }
}