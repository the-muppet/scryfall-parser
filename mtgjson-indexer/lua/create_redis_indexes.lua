-- Redis Search Index Creation Script for MTGJSON Card Data
-- This script creates comprehensive FT.CREATE indexes for the MTGJSON system
-- Usage: redis-cli --eval create_redis_indexes.lua

local function log(message)
    redis.log(redis.LOG_NOTICE, "INDEXER: " .. message)
    return "LOG: " .. message
end

local function safe_create_index(index_name, schema_args)
    -- Check if index already exists
    local exists = pcall(function()
        return redis.call("FT.INFO", index_name)
    end)
    
    if exists then
        log("Index " .. index_name .. " already exists, dropping and recreating...")
        redis.call("FT.DROPINDEX", index_name, "DD")
    end
    
    -- Create the index
    local success, err = pcall(function()
        return redis.call("FT.CREATE", index_name, unpack(schema_args))
    end)
    
    if success then
        log("✓ Created index: " .. index_name)
        return "✓ Created index: " .. index_name
    else
        log("✗ Failed to create index " .. index_name .. ": " .. tostring(err))
        return "✗ Failed to create index " .. index_name .. ": " .. tostring(err)
    end
end

local results = {}

-- =============================================================================
-- MTGJSON SYSTEM INDEXES (card:* and deck:* patterns)
-- =============================================================================

log("Creating MTGJSON system indexes...")

-- 1. Main MTGJSON Card Index
table.insert(results, safe_create_index("idx_mtgjson_cards", {
    "ON", "JSON",
    "PREFIX", "1", "card:",
    "SCHEMA",
    "$.uuid", "AS", "uuid", "TEXT", "NOSTEM",
    "$.name", "AS", "name", "TEXT", "PHONETIC", "dm:en",
    "$.mana_cost", "AS", "mana_cost", "TEXT",
    "$.mana_value", "AS", "mana_value", "NUMERIC", "SORTABLE",
    "$.type_line", "AS", "type_line", "TEXT",
    "$.oracle_text", "AS", "oracle_text", "TEXT",
    "$.colors", "AS", "colors", "TAG", "SEPARATOR", ",",
    "$.color_identity", "AS", "color_identity", "TAG", "SEPARATOR", ",",
    "$.set_code", "AS", "set_code", "TAG", "SORTABLE",
    "$.set_name", "AS", "set_name", "TEXT", "SORTABLE",
    "$.rarity", "AS", "rarity", "TAG", "SORTABLE",
    "$.collector_number", "AS", "collector_number", "TEXT", "SORTABLE",
    "$.released_at", "AS", "release_date", "TEXT", "SORTABLE",
    "$.scryfall_oracle_id", "AS", "scryfall_oracle_id", "TEXT", "NOSTEM",
    "$.tcgplayer_product_id", "AS", "tcgplayer_product_id", "NUMERIC", "SORTABLE",
    "$.power", "AS", "power", "TEXT",
    "$.toughness", "AS", "toughness", "TEXT", 
    "$.loyalty", "AS", "loyalty", "TEXT",
    "$.artist", "AS", "artist", "TEXT", "SORTABLE",
    "$.flavor_text", "AS", "flavor_text", "TEXT",
    "$.watermark", "AS", "watermark", "TAG",
    "$.frame", "AS", "frame", "TAG",
    "$.is_reserved", "AS", "is_reserved", "TAG",
    "$.is_oversized", "AS", "is_oversized", "TAG",
    "$.security_stamp", "AS", "security_stamp", "TAG",
    "$.promo_types", "AS", "promo_types", "TAG", "SEPARATOR", ",",
    "$.keywords", "AS", "keywords", "TAG", "SEPARATOR", ",",
    "$.legalities.standard", "AS", "legal_standard", "TAG",
    "$.legalities.pioneer", "AS", "legal_pioneer", "TAG",
    "$.legalities.modern", "AS", "legal_modern", "TAG", 
    "$.legalities.legacy", "AS", "legal_legacy", "TAG",
    "$.legalities.vintage", "AS", "legal_vintage", "TAG",
    "$.legalities.commander", "AS", "legal_commander", "TAG",
    "$.legalities.pauper", "AS", "legal_pauper", "TAG",
    "$.tcgplayer_skus[*].sku_id", "AS", "sku_ids", "NUMERIC"
}))

-- 2. Set Information Index
table.insert(results, safe_create_index("idx_mtgjson_sets", {
    "ON", "JSON", 
    "PREFIX", "1", "set:",
    "SCHEMA",
    "$.code", "AS", "set_code", "TAG", "SORTABLE",
    "$.name", "AS", "set_name", "TEXT", "SORTABLE",
    "$.release_date", "AS", "release_date", "TEXT", "SORTABLE",
    "$.set_type", "AS", "set_type", "TAG", "SORTABLE",
    "$.total_cards", "AS", "total_cards", "NUMERIC", "SORTABLE",
    "$.base_set_size", "AS", "base_set_size", "NUMERIC", "SORTABLE"
}))

-- 3. Current Pricing Data Index (price:UUID:condition pattern)
table.insert(results, safe_create_index("idx_pricing", {
    "ON", "JSON",
    "PREFIX", "1", "price:",
    "SCHEMA",
    "$.tcg_market_price", "AS", "market_price", "NUMERIC", "SORTABLE",
    "$.tcg_direct_low", "AS", "direct_low", "NUMERIC", "SORTABLE", 
    "$.tcg_low_price", "AS", "low_price", "NUMERIC", "SORTABLE",
    "$.condition", "AS", "condition", "TAG", "SORTABLE",
    "$.product_name", "AS", "product_name", "TEXT",
    "$.set_name", "AS", "set_name", "TEXT"
}))

-- 4. Deck Index
table.insert(results, safe_create_index("idx_mtgjson_decks", {
    "ON", "JSON",
    "PREFIX", "1", "deck:",
    "FILTER", "@uuid != ''",  -- Only index actual deck objects, not metadata
    "SCHEMA",
    "$.uuid", "AS", "deck_uuid", "TEXT", "NOSTEM",
    "$.name", "AS", "deck_name", "TEXT", "SORTABLE",
    "$.code", "AS", "deck_code", "TAG", "SORTABLE", 
    "$.deck_type", "AS", "deck_type", "TAG", "SORTABLE",
    "$.release_date", "AS", "release_date", "TEXT", "SORTABLE",
    "$.is_commander", "AS", "is_commander", "TAG",
    "$.total_cards", "AS", "total_cards", "NUMERIC", "SORTABLE",
    "$.unique_cards", "AS", "unique_cards", "NUMERIC", "SORTABLE",
    "$.estimated_value.market_total", "AS", "market_value", "NUMERIC", "SORTABLE",
    "$.estimated_value.direct_total", "AS", "direct_value", "NUMERIC", "SORTABLE",
    "$.estimated_value.low_total", "AS", "low_value", "NUMERIC", "SORTABLE"
}))

-- 5. Deck Metadata Index (for lightweight browsing)
table.insert(results, safe_create_index("idx_deck_meta", {
    "ON", "JSON",
    "PREFIX", "1", "deck:meta:",
    "SCHEMA",
    "$.uuid", "AS", "deck_uuid", "TEXT", "NOSTEM",
    "$.name", "AS", "deck_name", "TEXT", "SORTABLE",
    "$.code", "AS", "deck_code", "TAG", "SORTABLE",
    "$.type", "AS", "deck_type", "TAG", "SORTABLE",
    "$.release_date", "AS", "release_date", "TEXT", "SORTABLE",
    "$.is_commander", "AS", "is_commander", "TAG",
    "$.total_cards", "AS", "total_cards", "NUMERIC", "SORTABLE",
    "$.unique_cards", "AS", "unique_cards", "NUMERIC", "SORTABLE",
    "$.estimated_value", "AS", "estimated_value", "NUMERIC", "SORTABLE",
    "$.slug", "AS", "slug", "TEXT", "NOSTEM"
}))

-- =============================================================================
-- AGGREGATION-FRIENDLY INDEXES
-- =============================================================================

log("Creating aggregation indexes...")

-- 6. Price Analysis Index (for market analysis)
table.insert(results, safe_create_index("idx_price_analysis", {
    "ON", "JSON",
    "PREFIX", "2", "card:", "price:",
    "SCHEMA",
    "$.name", "AS", "card_name", "TEXT", "SORTABLE",
    "$.set_code", "AS", "set_code", "TAG", "SORTABLE",
    "$.rarity", "AS", "rarity", "TAG", "SORTABLE",
    "$.tcg_market_price", "AS", "market_price", "NUMERIC", "SORTABLE",
    "$.tcg_direct_low", "AS", "direct_price", "NUMERIC", "SORTABLE",
    "$.condition", "AS", "condition", "TAG", "SORTABLE",
    "$.release_date", "AS", "release_date", "TEXT", "SORTABLE"
}))

-- 7. Format Legality Index (for competitive play analysis)
table.insert(results, safe_create_index("idx_format_legality", {
    "ON", "JSON", 
    "PREFIX", "1", "card:",
    "SCHEMA",
    "$.name", "AS", "card_name", "TEXT", "SORTABLE",
    "$.type_line", "AS", "type_line", "TEXT",
    "$.mana_value", "AS", "mana_value", "NUMERIC", "SORTABLE",
    "$.colors", "AS", "colors", "TAG", "SEPARATOR", ",",
    "$.legalities.standard", "AS", "standard", "TAG",
    "$.legalities.pioneer", "AS", "pioneer", "TAG", 
    "$.legalities.modern", "AS", "modern", "TAG",
    "$.legalities.legacy", "AS", "legacy", "TAG",
    "$.legalities.vintage", "AS", "vintage", "TAG",
    "$.legalities.commander", "AS", "commander", "TAG",
    "$.legalities.pauper", "AS", "pauper", "TAG",
    "$.rarity", "AS", "rarity", "TAG"
}))

-- =============================================================================
-- SKU-BASED PRICING INDEXES (Future Enhancement - add when implementing)
-- =============================================================================

-- 8. SKU Metadata Index (ready for when SKU-based pricing is implemented)
table.insert(results, safe_create_index("idx_sku_metadata", {
    "ON", "JSON",
    "PREFIX", "1", "sku:",
    "SCHEMA",
    "$.condition", "AS", "condition", "TAG", "SORTABLE",
    "$.language", "AS", "language", "TAG", "SORTABLE",
    "$.foil", "AS", "foil", "TAG",
    "$.product_id", "AS", "product_id", "NUMERIC", "SORTABLE",
    "$.product_name", "AS", "product_name", "TEXT", "SORTABLE",
    "$.set_name", "AS", "set_name", "TEXT", "SORTABLE"
}))

-- 9. SKU Pricing Index (now active for SKU-based pricing system)
table.insert(results, safe_create_index("idx_sku_pricing", {
    "ON", "JSON",
    "PREFIX", "1", "price:sku:",
    "SCHEMA",
    "$.sku_id", "AS", "sku_id", "NUMERIC", "SORTABLE",
    "$.tcg_market_price", "AS", "market_price", "NUMERIC", "SORTABLE",
    "$.tcg_direct_low", "AS", "direct_low", "NUMERIC", "SORTABLE",
    "$.tcg_low_price", "AS", "low_price", "NUMERIC", "SORTABLE",
    "$.timestamp", "AS", "timestamp", "NUMERIC", "SORTABLE"
}))

-- =============================================================================
-- CREATE ALIASES AND SUGGESTIONS
-- =============================================================================

log("Creating search aliases...")

-- Create search aliases for common queries
local aliases = {
    {"FT.ALIASADD", "search_cards", "idx_mtgjson_cards"},
    {"FT.ALIASADD", "search_decks", "idx_mtgjson_decks"},
    {"FT.ALIASADD", "search_prices", "idx_pricing"},
    {"FT.ALIASADD", "search_sets", "idx_mtgjson_sets"},
    {"FT.ALIASADD", "search_skus", "idx_sku_metadata"},
    {"FT.ALIASADD", "search_sku_prices", "idx_sku_pricing"}
}

for _, alias_cmd in ipairs(aliases) do
    local success, err = pcall(function()
        return redis.call(unpack(alias_cmd))
    end)
    if success then
        log("✓ Created alias: " .. alias_cmd[2] .. " -> " .. alias_cmd[3])
        table.insert(results, "✓ Created alias: " .. alias_cmd[2] .. " -> " .. alias_cmd[3])
    else
        -- Aliases might already exist, that's okay
        log("~ Alias may already exist: " .. alias_cmd[2])
    end
end

-- =============================================================================
-- RETURN RESULTS
-- =============================================================================

log("Index creation complete!")
table.insert(results, "\n=== REDIS SEARCH INDEXES CREATED ===")
table.insert(results, "✓ All indexes have been created successfully!")
table.insert(results, "\nAvailable indexes:")
table.insert(results, "• idx_mtgjson_cards - MTGJSON cards with full metadata")
table.insert(results, "• idx_mtgjson_sets - Set information and statistics")
table.insert(results, "• idx_pricing - Current TCGPlayer pricing data")
table.insert(results, "• idx_mtgjson_decks - Preconstructed deck data")
table.insert(results, "• idx_deck_meta - Lightweight deck metadata")
table.insert(results, "• idx_price_analysis - Price analysis and trends")
table.insert(results, "• idx_format_legality - Format legality lookup")
table.insert(results, "• idx_sku_metadata - SKU metadata for products")
table.insert(results, "• idx_sku_pricing - SKU-based pricing with time series")
table.insert(results, "\nSearch aliases:")
table.insert(results, "• search_cards -> idx_mtgjson_cards")
table.insert(results, "• search_decks -> idx_mtgjson_decks")
table.insert(results, "• search_prices -> idx_pricing")
table.insert(results, "• search_sets -> idx_mtgjson_sets")
table.insert(results, "• search_skus -> idx_sku_metadata")
table.insert(results, "• search_sku_prices -> idx_sku_pricing")

return results 