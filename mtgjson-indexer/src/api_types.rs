use serde::{Deserialize, Serialize};
use crate::redis_client::{MemoryUsage, DatabaseStats};


// =============================================================================
// RESPONSE TYPES
// =============================================================================

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub redis: String,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub redis_connection: String,
    pub total_keys: usize,
    pub memory_usage: MemoryUsage,
    pub database_stats: DatabaseStats,
}

// =============================================================================
// QUERY PARAMETERS
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub set_code: Option<String>,
    pub rarity: Option<String>,
    pub color: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExpensiveQuery {
    #[serde(default = "default_min_price")]
    pub min_price: f64,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct AutocompleteQuery {
    pub prefix: String,
    #[serde(default = "default_autocomplete_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct PriceQuery {
    #[serde(default = "default_condition")]
    pub condition: String,
}

#[derive(Debug, Deserialize)]
pub struct PriceHistoryQuery {
    #[serde(default = "default_days")]
    pub days: u32,
}

#[derive(Debug, Deserialize)]
pub struct TrendingQuery {
    #[serde(default = "default_direction")]
    pub direction: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct ArbitrageQuery {
    #[serde(default)]
    pub card_filter: String,
    #[serde(default = "default_min_diff")]
    pub min_diff: f64,
}

pub fn default_limit() -> usize { 50 }
pub fn default_autocomplete_limit() -> usize { 10 }
pub fn default_min_price() -> f64 { 50.0 }
pub fn default_condition() -> String { "Near Mint".to_string() }
pub fn default_days() -> u32 { 30 }
pub fn default_direction() -> String { "up".to_string() }
pub fn default_min_diff() -> f64 { 5.0 }