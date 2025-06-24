use redis::{Client, AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use anyhow::{Result, Context};
use chrono::{DateTime, Utc};
use std::env;
use std::path::Path;
use tokio::fs;

// API-specific type definitions
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
    lua_scripts: HashMap<String, Script>,
}

impl MTGRedisClient {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .context("Failed to create Redis client")?;
        
        let lua_scripts = Self::load_lua_scripts().await
            .context("Failed to load Lua scripts")?;
        
        Ok(Self {
            client,
            lua_scripts,
        })
    }

    async fn load_lua_scripts() -> Result<HashMap<String, Script>> {
        let mut scripts = HashMap::new();
        
        // Try multiple possible locations for the lua directory
        let possible_lua_dirs = vec![
            Path::new("lua"),
            Path::new("mtgjson-indexer/lua"),
            Path::new("./lua"),
        ];
        
        let lua_dir = possible_lua_dirs
            .into_iter()
            .find(|dir| dir.exists())
            .ok_or_else(|| anyhow::anyhow!("Lua scripts directory not found. Tried: lua, mtgjson-indexer/lua, ./lua"))?;
        
        println!("✓ Found Lua scripts directory at: {}", lua_dir.display());

        let script_mappings = [
            ("search_cards", "search_cards.lua"),
            ("deck_search", "deck_search.lua"),
            ("card_stats", "card_stats.lua"),
            ("find_expensive_cards", "find_expensive_cards.lua"),
            ("price_comparison", "price_comparison.lua"),
            ("pricing_trends", "pricing_trends.lua"),
            ("sku_price_analysis", "sku_price_analysis.lua"),
            ("set_analysis", "set_analysis.lua"),
            ("export_tcg_csv", "export_tcg_csv.lua"),
            ("cleanup_indexes", "cleanup_indexes.lua"),
            ("create_redis_indexes", "create_redis_indexes.lua"),
            ("find_missing_data", "find_missing_data.lua"),
            ("find_duplicates", "find_duplicates.lua"),
            ("sealed_arbitrage", "sealed_arbitrage.lua"),
            ("unique_printings", "unique_printings.lua"),
        ];

        let mut loaded_count = 0;
        for (script_name, filename) in script_mappings {
            let script_path = lua_dir.join(filename);
            if script_path.exists() {
                match fs::read_to_string(&script_path).await {
                    Ok(script_content) => {
                        let script = Script::new(&script_content);
                        scripts.insert(script_name.to_string(), script);
                        loaded_count += 1;
                    }
                    Err(e) => {
                        eprintln!("Warning: Could not load Lua script '{}': {}", filename, e);
                    }
                }
            } else {
                eprintln!("Warning: Lua script not found: {}", script_path.display());
            }
        }

        println!("✓ Loaded {}/{} Lua scripts", loaded_count, script_mappings.len());
        Ok(scripts)
    }

    async fn execute_lua_script<T>(&mut self, script_name: &str, args: Vec<String>) -> Result<T>
    where
        T: redis::FromRedisValue,
    {
        let script = self.lua_scripts.get(script_name)
            .ok_or_else(|| anyhow::anyhow!("Lua script '{}' not loaded", script_name))?;

        let mut con = self.client.get_multiplexed_async_connection().await?;
        
        // Convert args to the format expected by Redis
        let mut cmd = script.prepare_invoke();
        for arg in args {
            cmd.arg(arg);
        }
        
        let result = cmd.invoke_async(&mut con).await?;
        Ok(result)
    }

    async fn execute_lua_script_raw(&mut self, script_name: &str, args: Vec<String>) -> Result<redis::Value> {
        let script = self.lua_scripts.get(script_name)
            .ok_or_else(|| anyhow::anyhow!("Lua script '{}' not loaded", script_name))?;

        let mut con = self.client.get_multiplexed_async_connection().await?;
        
        // Convert args to the format expected by Redis
        let mut cmd = script.prepare_invoke();
        for arg in args {
            cmd.arg(arg);
        }
        
        let result: redis::Value = cmd.invoke_async(&mut con).await?;
        Ok(result)
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
        let mut con = self.client.get_multiplexed_async_connection().await?;
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
        let mut con = self.client.get_multiplexed_async_connection().await?;
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

    pub async fn search_cards_by_name(&mut self, query: &str, max_results: usize, filters: HashMap<String, String>) -> Result<Vec<serde_json::Value>> {
        let mut args = vec![query.to_string(), max_results.to_string()];
        
        // Add filter arguments
        for (key, value) in filters {
            args.push(key);
            args.push(value);
        }
        
        let result: redis::Value = self.execute_lua_script_raw("search_cards", args).await?;
        
        match result {
            redis::Value::Array(items) => {
                let mut cards = Vec::new();
                for item in items.iter() {
                    match item {
                        redis::Value::BulkString(json_bytes) => {
                            if let Ok(json_str) = String::from_utf8(json_bytes.clone()) {
                                if let Ok(card_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                                    cards.push(card_json);
                                }
                            }
                        }
                        redis::Value::SimpleString(json_str) => {
                            if let Ok(card_json) = serde_json::from_str::<serde_json::Value>(json_str) {
                                cards.push(card_json);
                            }
                        }
                        redis::Value::Array(card_fields) => {
                            if let Ok(card_json) = redis_array_to_json_object(&card_fields) {
                                cards.push(card_json);
                            }
                        }
                        _ => {
                            // Handle other Redis value types if needed
                            if let Ok(json_val) = redis_value_to_json(&item) {
                                cards.push(json_val);
                            }
                        }
                    }
                }
                Ok(cards)
            }
            _ => {
                Err(anyhow::anyhow!("Unexpected return type from search_cards Lua script: {:?}", result))
            }
        }
    }

    pub async fn get_cards_in_set(&mut self, set_code: &str) -> Result<HashSet<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("set:{}:cards", set_code);
        let card_uuids = con.smembers(&key).await?;
        Ok(card_uuids)
    }

    pub async fn autocomplete_card_names(&mut self, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let prefix_lower = prefix.to_lowercase();
        let key = format!("auto:prefix:{}", prefix_lower);
        
        let card_uuids: HashSet<String> = con.smembers(&key).await.unwrap_or_default();
        let mut card_names = Vec::new();
        
        for card_uuid in card_uuids.into_iter().take(limit * 2) {
            if card_names.len() >= limit {
                break;
            }
            
            if let Ok(Some(card)) = self.get_card_by_uuid(&card_uuid).await {
                card_names.push(card.name);
            }
        }
        
        Ok(card_names)
    }

    // =============================================================================
    // DECK OPERATIONS
    // =============================================================================

    pub async fn get_deck_composition(&mut self, deck_uuid: &str) -> Result<serde_json::Value> {
        let formatted_uuid = if deck_uuid.starts_with("deck_") {
            deck_uuid.to_string()
        } else {
            format!("deck_{}", deck_uuid)
        };
        
        let args = vec!["composition".to_string(), formatted_uuid];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let composition: serde_json::Value = serde_json::from_str(&result)?;
        Ok(composition)
    }

    pub async fn get_deck_statistics(&mut self) -> Result<serde_json::Value> {
        let args = vec!["statistics".to_string()];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let stats: serde_json::Value = serde_json::from_str(&result)?;
        Ok(stats)
    }

    pub async fn get_commander_decks(&mut self) -> Result<Vec<serde_json::Value>> {
        let args = vec!["commander_decks".to_string()];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let decks: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(decks)
    }

    pub async fn find_decks_containing_card(&mut self, card_name: &str) -> Result<Vec<serde_json::Value>> {
        let args = vec!["contains_card".to_string(), card_name.to_string()];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let decks: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(decks)
    }

    pub async fn get_expensive_decks(&mut self, min_value: f64) -> Result<Vec<serde_json::Value>> {
        let args = vec!["expensive".to_string(), min_value.to_string()];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let decks: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(decks)
    }

    pub async fn search_decks_by_name(&mut self, deck_name: &str) -> Result<Vec<serde_json::Value>> {
        let args = vec!["search_name".to_string(), deck_name.to_string()];
        let result: String = self.execute_lua_script("deck_search", args).await?;
        let decks: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(decks)
    }

    // =============================================================================
    // DECK OPERATIONS - Direct Redis Access
    // =============================================================================

    pub async fn get_deck_by_uuid(&mut self, uuid: &str) -> Result<Option<IndexedDeck>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        
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
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("deck:type:{}", deck_type);
        let deck_uuids = con.smembers(&key).await?;
        Ok(deck_uuids)
    }

    pub async fn get_decks_in_set(&mut self, set_code: &str) -> Result<HashSet<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("deck:set:{}", set_code);
        let deck_uuids = con.smembers(&key).await?;
        Ok(deck_uuids)
    }

    // =============================================================================
    // PRICING OPERATIONS
    // =============================================================================

    pub async fn get_card_price(&mut self, uuid: &str, condition: &str) -> Result<Option<TcgPrice>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("price:{}:{}", uuid, condition);
        let data: Option<String> = con.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let price = serde_json::from_str(&json_str)?;
                Ok(Some(price))
            }
            None => Ok(None),
        }
    }

    pub async fn get_sku_price_latest(&mut self, sku_id: &str) -> Result<Option<TcgPrice>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("price:sku:{}:latest", sku_id);
        let data: Option<String> = con.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let price = serde_json::from_str(&json_str)?;
                Ok(Some(price))
            }
            None => Ok(None),
        }
    }

    pub async fn get_sku_price_history(&mut self, sku_id: &str, days: u32) -> Result<Vec<(f64, i64)>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("price:sku:{}:history", sku_id);
        let end_time = Utc::now().timestamp();
        let start_time = end_time - (days as i64 * 86400);
        
        let history: Vec<(String, f64)> = con
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
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("card:{}:skus", uuid);
        let sku_ids = con.smembers(&key).await?;
        Ok(sku_ids)
    }

    pub async fn get_card_by_sku_id(&mut self, sku_id: &str) -> Result<Option<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("sku:{}", sku_id);
        let card_uuid = con.get(&key).await?;
        Ok(card_uuid)
    }

    pub async fn get_card_by_tcgplayer_id(&mut self, tcgplayer_id: &str) -> Result<Option<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("tcgplayer:{}", tcgplayer_id);
        let card_uuid = con.get(&key).await?;
        Ok(card_uuid)
    }

    // =============================================================================
    // SET OPERATIONS
    // =============================================================================

    pub async fn get_set_by_code(&mut self, set_code: &str) -> Result<Option<SetInfo>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("set:{}", set_code);
        let data: Option<String> = con.get(&key).await?;
        
        match data {
            Some(json_str) => {
                let set_info = serde_json::from_str(&json_str)?;
                Ok(Some(set_info))
            }
            None => Ok(None),
        }
    }

    pub async fn get_all_sets(&mut self) -> Result<Vec<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let pattern = "set:*";
        let keys: Vec<String> = con.keys(pattern).await?;
        
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
    // PRICING OPERATIONS (Using Lua Scripts)
    // =============================================================================

    pub async fn get_expensive_cards(&mut self, min_price: f64, max_results: usize) -> Result<Vec<serde_json::Value>> {
        let args = vec![min_price.to_string(), max_results.to_string()];
        let result: redis::Value = self.execute_lua_script_raw("find_expensive_cards", args).await?;
        
        match result {
            redis::Value::Array(items) => {
                let mut cards = Vec::new();
                for item in items.iter() {
                    match item {
                        redis::Value::BulkString(json_bytes) => {
                            if let Ok(json_str) = String::from_utf8(json_bytes.clone()) {
                                if let Ok(card_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                                    cards.push(card_json);
                                }
                            }
                        }
                        redis::Value::SimpleString(json_str) => {
                            if let Ok(card_json) = serde_json::from_str::<serde_json::Value>(json_str) {
                                cards.push(card_json);
                            }
                        }
                        _ => {
                            if let Ok(json_val) = redis_value_to_json(&item) {
                                cards.push(json_val);
                            }
                        }
                    }
                }
                Ok(cards)
            }
            _ => {
                Err(anyhow::anyhow!("Unexpected return type from find_expensive_cards Lua script: {:?}", result))
            }
        }
    }

    pub async fn get_trending_cards(&mut self, direction: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        let args = vec!["trending".to_string(), direction.to_string(), limit.to_string()];
        let result: String = self.execute_lua_script("sku_price_analysis", args).await?;
        let cards: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(cards)
    }

    pub async fn get_price_arbitrage_opportunities(&mut self, card_filter: &str, min_diff: f64) -> Result<Vec<serde_json::Value>> {
        let args = vec!["arbitrage".to_string(), card_filter.to_string(), min_diff.to_string()];
        let result: String = self.execute_lua_script("sku_price_analysis", args).await?;
        let opportunities: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(opportunities)
    }

    pub async fn compare_card_prices_by_condition(&mut self, card_name: &str) -> Result<Vec<serde_json::Value>> {
        let args = vec!["condition_compare".to_string(), card_name.to_string()];
        let result: String = self.execute_lua_script("sku_price_analysis", args).await?;
        let comparison: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(comparison)
    }

    pub async fn get_pricing_trends_distribution(&mut self) -> Result<Vec<String>> {
        let args = vec!["distribution".to_string()];
        let result: Vec<String> = self.execute_lua_script("pricing_trends", args).await?;
        Ok(result)
    }

    pub async fn get_pricing_trends_by_set(&mut self, set_code: &str) -> Result<Vec<String>> {
        let args = vec!["by_set".to_string(), set_code.to_string()];
        let result: Vec<String> = self.execute_lua_script("pricing_trends", args).await?;
        Ok(result)
    }

    pub async fn export_deck_to_tcg_csv(&mut self, deck_uuid: &str) -> Result<String> {
        let formatted_uuid = if deck_uuid.starts_with("deck_") {
            deck_uuid.to_string()
        } else {
            format!("deck_{}", deck_uuid)
        };
        
        let args = vec![formatted_uuid, "single".to_string()];
        let result: String = self.execute_lua_script("export_tcg_csv", args).await?;
        
        // Parse JSON result if needed
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&result) {
            if let Some(csv_data) = data.get("csv_data").and_then(|v| v.as_str()) {
                return Ok(csv_data.to_string());
            }
        }
        
        Ok(result)
    }

    pub async fn export_all_decks_to_csv(&mut self) -> Result<String> {
        let args = vec!["".to_string(), "all".to_string()];
        let result: String = self.execute_lua_script("export_tcg_csv", args).await?;
        
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&result) {
            if let Some(csv_data) = data.get("csv_data").and_then(|v| v.as_str()) {
                return Ok(csv_data.to_string());
            }
        }
        
        Ok(result)
    }

    // =============================================================================
    // ANALYTICS & STATISTICS 
    // =============================================================================

    pub async fn get_database_stats_detailed(&mut self) -> Result<Vec<String>> {
        let args: Vec<String> = vec![];
        let result: Vec<String> = self.execute_lua_script("card_stats", args).await?;
        Ok(result)
    }

    pub async fn get_missing_data_analysis(&mut self, data_type: &str, max_results: usize) -> Result<Vec<String>> {
        let args = vec![data_type.to_string(), max_results.to_string()];
        let result: Vec<String> = self.execute_lua_script("find_missing_data", args).await?;
        Ok(result)
    }

    pub async fn get_set_analysis(&mut self, set_code: &str) -> Result<Vec<serde_json::Value>> {
        let args = if set_code.is_empty() { 
            vec![] 
        } else { 
            vec![set_code.to_string()] 
        };
        let result: String = self.execute_lua_script("set_analysis", args).await?;
        let analysis: Vec<serde_json::Value> = serde_json::from_str(&result)?;
        Ok(analysis)
    }

    // =============================================================================
    // MAINTENANCE OPERATIONS
    // =============================================================================

    pub async fn cleanup_search_indexes(&mut self) -> Result<Vec<String>> {
        let args: Vec<String> = vec![];
        let result: Vec<String> = self.execute_lua_script("cleanup_indexes", args).await?;
        Ok(result)
    }

    pub async fn create_search_indexes(&mut self) -> Result<Vec<String>> {
        let args: Vec<String> = vec![];
        let result: Vec<String> = self.execute_lua_script("create_redis_indexes", args).await?;
        Ok(result)
    }

    // =============================================================================
    // ANALYTICS & STATISTICS
    // =============================================================================

    pub async fn get_key_count(&mut self, pattern: &str) -> Result<usize> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = con.keys(pattern).await?;
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
        match self.client.get_multiplexed_async_connection().await {
            Ok(mut con) => {
                let result: Result<String, redis::RedisError> = redis::cmd("PING").query_async(&mut con).await;
                match result {
                    Ok(response) => response == "PONG",
                    Err(_) => false,
                }
            }
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

// Helper functions for converting Redis values to JSON
fn redis_value_to_json(value: &redis::Value) -> Result<serde_json::Value> {
    match value {
        redis::Value::Nil => Ok(serde_json::Value::Null),
        redis::Value::Int(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        redis::Value::BulkString(bytes) => {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                Ok(serde_json::Value::String(s))
            } else {
                Ok(serde_json::Value::Null)
            }
        }
        redis::Value::Array(arr) => {
            let mut json_array = Vec::new();
            for item in arr {
                json_array.push(redis_value_to_json(item)?);
            }
            Ok(serde_json::Value::Array(json_array))
        }
        redis::Value::Okay => Ok(serde_json::Value::String("OK".to_string())),
        redis::Value::SimpleString(s) => Ok(serde_json::Value::String(s.clone())),
        redis::Value::Double(f) => Ok(serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)))),
        redis::Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        redis::Value::Map(map) => {
            let mut object = serde_json::Map::new();
            for (key, value) in map {
                if let Ok(key_str) = redis_value_to_json(key) {
                    if let Some(key_string) = key_str.as_str() {
                        let value_json = redis_value_to_json(value)?;
                        object.insert(key_string.to_string(), value_json);
                    }
                }
            }
            Ok(serde_json::Value::Object(object))
        }
        redis::Value::Set(set) => {
            let mut json_array = Vec::new();
            for item in set {
                json_array.push(redis_value_to_json(item)?);
            }
            Ok(serde_json::Value::Array(json_array))
        }
        _ => Ok(serde_json::Value::Null),
    }
}

fn redis_array_to_json_object(fields: &[redis::Value]) -> Result<serde_json::Value> {
    let mut object = serde_json::Map::new();
    
    let mut i = 0;
    while i + 1 < fields.len() {
        let key = match &fields[i] {
            redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).unwrap_or_default(),
            redis::Value::SimpleString(s) => s.clone(),
            _ => continue,
        };
        
        let value = redis_value_to_json(&fields[i + 1])?;
        object.insert(key, value);
        i += 2;
    }
    
    Ok(serde_json::Value::Object(object))
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