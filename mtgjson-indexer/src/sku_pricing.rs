use crate::types::{TcgPrice, TcgplayerSku};
use anyhow::{Context, Result};
use redis::{Client, Connection, Commands, Pipeline};
use serde_json::json;
use std::collections::HashMap;

pub struct SkuPricingManager {
    pub redis_client: Client,
}

impl SkuPricingManager {
    pub fn new(redis_client: Client) -> Self {
        Self { redis_client }
    }

    /// Store SKU-based pricing
    pub fn store_sku_pricing_batch(
        &self,
        con: &mut Connection,
        pricing_data: &HashMap<String, Vec<TcgPrice>>,
        sku_index: &HashMap<String, Vec<TcgplayerSku>>,
        card_uuid: &str,
    ) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp();
        let mut pipe = redis::pipe();

        // Get card's TCGPlayer product ID
        if let Some(product_id) = self.get_card_tcgplayer_product_id(con, card_uuid)? {
            // Find SKUs for this product
            if let Some(skus) = sku_index.get(&product_id) {
                for sku in skus {
                    // Find matching price data
                    if let Some(prices) = pricing_data.get(&product_id) {
                        for price in prices {
                            // Match SKU condition with price condition
                            if sku.condition.as_deref().unwrap_or("") == price.condition {
                                self.store_single_sku_price(&mut pipe, sku, price, card_uuid, timestamp)?;
                            }
                        }
                    }
                }
            }
        }

        // Execute all operations
        pipe.query::<()>(con).context("Failed to store SKU pricing data")?;
        Ok(())
    }

    /// Store a single SKU price using existing types
    fn store_single_sku_price(
        &self,
        pipe: &mut Pipeline,
        sku: &TcgplayerSku,
        price: &TcgPrice,
        card_uuid: &str,
        timestamp: i64,
    ) -> Result<()> {
        let sku_id = sku.sku_id.to_string();

        // Store latest pricing
        let price_json = json!({
            "tcg_market_price": price.tcg_market_price,
            "tcg_direct_low": price.tcg_direct_low,
            "tcg_low_price": price.tcg_low_price,
            "timestamp": timestamp
        });

        pipe.cmd("SET")
            .arg(format!("price:sku:{}:latest", sku_id))
            .arg(price_json.to_string());

        // Store historical price point
        if let Some(market_price) = price.tcg_market_price {
            pipe.cmd("ZADD")
                .arg(format!("price:sku:{}:history", sku_id))
                .arg(timestamp)
                .arg(market_price);
        }

        // Store SKU metadata (separate from pricing)
        let sku_meta = json!({
            "condition": sku.condition.clone().unwrap_or_default(),
            "language": sku.language.clone().unwrap_or_else(|| "English".to_string()),
            "foil": sku.printing.as_deref() == Some("Foil"),
            "product_id": sku.product_id,
            "product_name": price.product_name,
            "set_name": price.set_name
        });

        pipe.cmd("SET")
            .arg(format!("sku:{}:meta", sku_id))
            .arg(sku_meta.to_string());

        // Create bidirectional mapping
        pipe.cmd("SET")
            .arg(format!("sku:{}:card", sku_id))
            .arg(card_uuid);

        pipe.cmd("SADD")
            .arg(format!("card:{}:skus", card_uuid))
            .arg(&sku_id);

        Ok(())
    }

    /// Get card's TCGPlayer product ID
    fn get_card_tcgplayer_product_id(&self, con: &mut Connection, card_uuid: &str) -> Result<Option<String>> {
        let card_data: Option<String> = con.get(format!("card:{}", card_uuid))?;
        
        if let Some(json_str) = card_data {
            if let Ok(card_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                return Ok(card_json["tcgplayer_product_id"].as_str().map(|s| s.to_string()));
            }
        }
        
        Ok(None)
    }
} 