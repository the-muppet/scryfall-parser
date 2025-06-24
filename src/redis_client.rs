use redis::{Client, AsyncCommands};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use anyhow::{Result, Context};
use chrono::{DateTime, Utc};
use std::env;

// API-specific type definitions (simplified from mtgjson-indexer/types.rs)
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IndexedCard {
    pub uuid: String,
    pub name: String,
    pub set_code: String,
    pub set_name: String,
    pub collector_number: String,
    pub rarity: String,
    pub mana_value: f32,
    pub mana_cost: Option<String>,
    pub colors: Vec<String>,
    pub color_identity: Vec<String>,
    pub types: Vec<String>,
    pub subtypes: Vec<String>,
    pub supertypes: Vec<String>,
    pub power: Option<String>,
    pub toughness: Option<String>,
    pub loyalty: Option<String>,
    pub defense: Option<String>,
    pub text: Option<String>,
    pub flavor_text: Option<String>,
    pub layout: String,
    pub availability: Vec<String>,
    pub finishes: Vec<String>,
    pub has_foil: bool,
    pub has_non_foil: bool,
    pub is_reserved: bool,
    pub is_promo: bool,
    pub release_date: String,
    pub scryfall_oracle_id: Option<String>,
    pub scryfall_id: Option<String>,
    pub tcgplayer_product_id: Option<String>,
    pub tcgplayer_skus: Vec<TcgplayerSku>,
    pub purchase_urls: PurchaseUrls,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexedDeck {
    pub uuid: String,
    pub name: String,
    pub code: String,
    pub deck_type: String,
    pub release_date: String,
    pub is_commander: bool,
    pub total_cards: u32,
    pub unique_cards: u32,
    pub commanders: Vec<DeckCardInfo>,
    pub main_board: Vec<DeckCardInfo>,
    pub side_board: Vec<DeckCardInfo>,
    pub estimated_value: Option<DeckValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeckCardInfo {
    pub uuid: String,
    pub name: String,
    pub count: u32,
    pub is_foil: bool,
    pub set_code: String,
    pub tcgplayer_product_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeckValue {
    pub market_total: f64,
    pub direct_total: f64,
    pub low_total: f64,
    pub cards_with_pricing: u32,
    pub cards_without_pricing: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TcgplayerSku {
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub printing: Option<String>,
    pub product_id: u64,
    pub sku_id: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PurchaseUrls {
    #[serde(default)]
    pub card_kingdom: Option<String>,
    #[serde(default)]
    pub card_kingdom_etched: Option<String>,
    #[serde(default)]
    pub card_kingdom_foil: Option<String>,
    #[serde(default)]
    pub cardmarket: Option<String>,
    #[serde(default)]
    pub tcgplayer: Option<String>,
    #[serde(default)]
    pub tcgplayer_etched: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetInfo {
    pub code: String,
    pub name: String,
    pub release_date: String,
    pub set_type: String,
    pub total_cards: usize,
    pub base_set_size: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TcgPrice {
    pub tcgplayer_id: String,
    pub product_line: String,
    pub set_name: String,
    pub product_name: String,
    pub title: String,
    pub number: String,
    pub rarity: String,
    pub condition: String,
    pub tcg_market_price: Option<f64>,
    pub tcg_direct_low: Option<f64>,
    pub tcg_low_price_with_shipping: Option<f64>,
    pub tcg_low_price: Option<f64>,
    pub total_quantity: Option<i32>,
    pub add_to_quantity: Option<i32>,
    pub tcg_marketplace_price: Option<f64>,
}

pub struct MTGRedisClient {
    client: Client,
}

impl MTGRedisClient {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .context("Failed to create Redis client")?;
        
        Ok(Self {
            client,
        })
    }

    pub async fn from_env() -> Result<Self> {
        let redis_url = env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:9999".to_string());
        Self::new(&redis_url).await
    }

    // =============================================================================
    // CARD OPERATIONS
    // =============================================================================

    pub async fn get_card_by_uuid(&mut self, uuid: &str) -> Result<Option<IndexedCard>> {
        let mut con = self.client.get_async_connection().await?;
        let key = format!("card:{}", uuid);
        let data: Option<String> = con.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let card = serde_json::from_str(&json_str)?;
                Ok(Some(card))
            }
            None => Ok(None),
        }
    }

    pub async fn get_card_by_oracle_id(&mut self, oracle_id: &str) -> Result<Option<IndexedCard>> {
        let mut con = self.client.get_async_connection().await?;
        let key = format!("card:oracle:{}", oracle_id);
        let data: Option<String> = con.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let card = serde_json::from_str(&json_str)?;
                Ok(Some(card))
            }
            None => Ok(None),
        }
    }

    pub async fn search_cards_by_name(&mut self, query: &str, max_results: usize) -> Result<Vec<IndexedCard>> {
        // Simple implementation - could be enhanced with Lua scripts
        let mut con = self.client.get_async_connection().await?;
        let mut results = Vec::new();
        let query_lower = query.to_lowercase();
        
        // Search through autocomplete prefixes
        for prefix_len in (1..=std::cmp::min(query_lower.len(), 10)).rev() {
            let prefix = &query_lower[..prefix_len];
            let key = format!("auto:prefix:{}", prefix);
            
            let oracle_ids: HashSet<String> = con.smembers(&key).await.unwrap_or_default();
            
            for oracle_id in oracle_ids {
                if results.len() >= max_results {
                    break;
                }
                
                if let Ok(Some(card)) = self.get_card_by_oracle_id(&oracle_id).await {
                    if card.name.to_lowercase().contains(&query_lower) {
                        results.push(card);
                    }
                }
            }
            
            if results.len() >= max_results {
                break;
            }
        }
        
        Ok(results)
    }

    pub async fn get_cards_in_set(&mut self, set_code: &str) -> Result<HashSet<String>> {
        let mut con = self.client.get_async_connection().await?;
        let key = format!("set:{}:cards", set_code);
        let card_uuids = con.smembers(&key).await?;
        Ok(card_uuids)
    }

    pub async fn autocomplete_card_names(&mut self, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let mut con = self.client.get_async_connection().await?;
        let prefix_lower = prefix.to_lowercase();
        let key = format!("auto:prefix:{}", prefix_lower);
        
        let oracle_ids: HashSet<String> = con.smembers(&key).await.unwrap_or_default();
        let mut card_names = Vec::new();
        
        for oracle_id in oracle_ids.into_iter().take(limit * 2) {
            if card_names.len() >= limit {
                break;
            }
            
            if let Ok(Some(card)) = self.get_card_by_oracle_id(&oracle_id).await {
                card_names.push(card.name);
            }
        }
        
        Ok(card_names)
    }

    // =============================================================================
    // DECK OPERATIONS
    // =============================================================================

    pub async fn get_deck_by_uuid(&mut self, uuid: &str) -> Result<Option<IndexedDeck>> {
        let mut con = self.client.get_async_connection().await?;
        
        // Try meta first for lightweight operations
        let meta_key = format!("deck:meta:deck_{}", uuid);
        let meta_data: Option<String> = con.get(&meta_key).await.unwrap_or(None);
        
        if let Some(json_str) = meta_data {
            let deck = serde_json::from_str(&json_str)?;
            return Ok(Some(deck));
        }
        
        // Fall back to full deck data
        let full_key = format!("deck:deck_{}", uuid);
        let full_data: Option<String> = con.get(&full_key).await.unwrap_or(None);
        
        match full_data {
            Some(json_str) => {
                let deck = serde_json::from_str(&json_str)?;
                Ok(Some(deck))
            }
            None => Ok(None),
        }
    }

    pub async fn get_decks_by_type(&mut self, deck_type: &str) -> Result<HashSet<String>> {
        let key = format!("deck:type:{}", deck_type);
        let deck_uuids = self.connection_manager.smembers(&key).await?;
        Ok(deck_uuids)
    }

    pub async fn get_decks_in_set(&mut self, set_code: &str) -> Result<HashSet<String>> {
        let key = format!("deck:set:{}", set_code);
        let deck_uuids = self.connection_manager.smembers(&key).await?;
        Ok(deck_uuids)
    }

    // =============================================================================
    // PRICING OPERATIONS
    // =============================================================================

    pub async fn get_card_price(&mut self, uuid: &str, condition: &str) -> Result<Option<TcgPrice>> {
        let key = format!("price:{}:{}", uuid, condition);
        let data: Option<String> = self.connection_manager.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let price = serde_json::from_str(&json_str)?;
                Ok(Some(price))
            }
            None => Ok(None),
        }
    }

    pub async fn get_sku_price_latest(&mut self, sku_id: &str) -> Result<Option<TcgPrice>> {
        let key = format!("price:sku:{}:latest", sku_id);
        let data: Option<String> = self.connection_manager.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let price = serde_json::from_str(&json_str)?;
                Ok(Some(price))
            }
            None => Ok(None),
        }
    }

    pub async fn get_sku_price_history(&mut self, sku_id: &str, days: u32) -> Result<Vec<(f64, i64)>> {
        let key = format!("price:sku:{}:history", sku_id);
        let end_time = Utc::now().timestamp();
        let start_time = end_time - (days as i64 * 86400);
        
        let history: Vec<(String, f64)> = self.connection_manager
            .zrangebyscore_withscores(&key, start_time, end_time)
            .await
            .unwrap_or_default();
        
        let result = history
            .into_iter()
            .filter_map(|(price_str, timestamp)| {
                price_str.parse::<f64>().ok().map(|price| (price, timestamp as i64))
            })
            .collect();
        
        Ok(result)
    }

    pub async fn get_card_skus(&mut self, uuid: &str) -> Result<HashSet<String>> {
        let key = format!("card:{}:skus", uuid);
        let sku_ids = self.connection_manager.smembers(&key).await?;
        Ok(sku_ids)
    }

    pub async fn get_card_by_sku_id(&mut self, sku_id: &str) -> Result<Option<String>> {
        let key = format!("sku:{}", sku_id);
        let card_uuid = self.connection_manager.get(&key).await?;
        Ok(card_uuid)
    }

    pub async fn get_card_by_tcgplayer_id(&mut self, tcgplayer_id: &str) -> Result<Option<String>> {
        let key = format!("tcgplayer:{}", tcgplayer_id);
        let card_uuid = self.connection_manager.get(&key).await?;
        Ok(card_uuid)
    }

    // =============================================================================
    // SET OPERATIONS
    // =============================================================================

    pub async fn get_set_by_code(&mut self, set_code: &str) -> Result<Option<SetInfo>> {
        let key = format!("set:{}", set_code);
        let data: Option<String> = self.connection_manager.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let set_info = serde_json::from_str(&json_str)?;
                Ok(Some(set_info))
            }
            None => Ok(None),
        }
    }

    pub async fn get_all_sets(&mut self) -> Result<Vec<String>> {
        let pattern = "set:*";
        let keys: Vec<String> = self.connection_manager.keys(pattern).await?;
        
        let set_codes = keys
            .into_iter()
            .filter_map(|key| {
                if !key.contains(":cards") && !key.contains(":decks") {
                    key.strip_prefix("set:").map(String::from)
                } else {
                    None
                }
            })
            .collect();
        
        Ok(set_codes)
    }

    // =============================================================================
    // ANALYTICS & STATISTICS
    // =============================================================================

    pub async fn get_key_count(&mut self, pattern: &str) -> Result<usize> {
        let keys: Vec<String> = self.connection_manager.keys(pattern).await?;
        Ok(keys.len())
    }

    pub async fn get_database_stats(&mut self) -> Result<DatabaseStats> {
        let card_count = self.get_key_count("card:*").await.unwrap_or(0);
        let deck_count = self.get_key_count("deck:*").await.unwrap_or(0);
        let set_count = self.get_all_sets().await.unwrap_or_default().len();
        
        Ok(DatabaseStats {
            total_cards: card_count,
            total_decks: deck_count,
            total_sets: set_count,
            last_update: Utc::now(),
        })
    }

    // =============================================================================
    // UTILITY METHODS
    // =============================================================================

    pub async fn ping(&mut self) -> bool {
        match self.connection_manager.get::<_, String>("ping").await {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub async fn get_memory_usage(&mut self) -> Result<MemoryUsage> {
        // This would need to be implemented with Redis INFO command
        // For now, return placeholder
        Ok(MemoryUsage {
            used_memory: 0,
            used_memory_human: "Unknown".to_string(),
            used_memory_peak: 0,
            used_memory_peak_human: "Unknown".to_string(),
        })
    }
}

// =============================================================================
// RESPONSE TYPES
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub total_cards: usize,
    pub total_decks: usize,
    pub total_sets: usize,
    pub last_update: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryUsage {
    pub used_memory: u64,
    pub used_memory_human: String,
    pub used_memory_peak: u64,
    pub used_memory_peak_human: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse<T> {
    pub query: String,
    pub count: usize,
    pub results: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

pub async fn create_mtg_client(redis_url: &str) -> Result<MTGRedisClient> {
    MTGRedisClient::new(redis_url).await
}

pub async fn create_mtg_client_from_env() -> Result<MTGRedisClient> {
    MTGRedisClient::from_env().await
} 