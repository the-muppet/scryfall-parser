-- Find cards with missing or incomplete data
-- Usage: EVAL script 0 [data_type] [max_results]
-- data_type: "prices", "tcgplayer", "images", "sets", "all", "summary"

local data_type = ARGV[1] or "summary"
local max_results = tonumber(ARGV[2]) or 20

local missing_data = {
    no_price = {},
    no_tcgplayer = {},
    no_image = {},
    no_sets = {},
    incomplete = {}
}

-- Pre-build price lookup table for efficiency (avoids KEYS in loop)
local cards_with_prices = {}
local cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "price:????????-????-????-????-????????????:*", "COUNT", 1000)
    cursor = result[1]
    
    for _, price_key in ipairs(result[2]) do
        -- Extract UUID from price:UUID:condition pattern
        local uuid = string.match(price_key, "price:([^:]+):")
        if uuid and string.len(uuid) == 36 then
            -- Check if this price key has actual pricing data
            local price_data = redis.call("GET", price_key)
            if price_data and string.match(price_data, '"tcg_market_price":%s*[%d%.]+') then
                cards_with_prices[uuid] = true
            end
        end
    end
until cursor == "0"

-- Scan all MTGJSON cards and check for missing data
local total_cards = 0
cursor = "0"

repeat
    local result = redis.call("SCAN", cursor, "MATCH", "card:????????-????-????-????-????????????", "COUNT", 500)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        -- Filter to direct card:UUID keys only
        local parts = {}
        for part in string.gmatch(key, "[^:]+") do
            table.insert(parts, part)
        end
        
        if #parts == 2 and string.len(parts[2]) == 36 then
            local card_data = redis.call("GET", key)
            if card_data then
                total_cards = total_cards + 1
                local uuid = parts[2]
                
                -- Extract card name and set
                local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                local set_match = string.match(card_data, '"set_code":%s*"([^"]+)"')
                local card_name = name_match or "Unknown"
                local sets_display = set_match or "No set"
                
                local issues = {}
                
                -- Check for missing price (use pre-built lookup)
                local has_price = cards_with_prices[uuid] or false
                
                if not has_price then
                    table.insert(missing_data.no_price, {
                        name = card_name,
                        uuid = uuid,
                        sets = sets_display
                    })
                    table.insert(issues, "no_price")
                end
                
                -- Check for missing TCGPlayer Product ID
                local has_tcgplayer = string.match(card_data, '"tcgplayer_product_id":%s*"[^"]+"') ~= nil
                if not has_tcgplayer then
                    table.insert(missing_data.no_tcgplayer, {
                        name = card_name,
                        uuid = uuid,
                        sets = sets_display
                    })
                    table.insert(issues, "no_tcgplayer")
                end
                
                -- Check for missing main image (purchase URLs or other image indicators)
                local has_image = string.match(card_data, '"purchase_urls":%s*{[^}]*"[^"]*":%s*"[^"]+"') ~= nil
                if not has_image then
                    table.insert(missing_data.no_image, {
                        name = card_name,
                        uuid = uuid,
                        sets = sets_display
                    })
                    table.insert(issues, "no_image")
                end
                
                -- Check for missing/insufficient set data (MTGJSON cards should always have set_code)
                if not set_match or set_match == "" then
                    table.insert(missing_data.no_sets, {
                        name = card_name,
                        uuid = uuid,
                        sets = sets_display
                    })
                    table.insert(issues, "no_sets")
                end
                
                -- Track cards with multiple issues
                if #issues >= 2 then
                    table.insert(missing_data.incomplete, {
                        name = card_name,
                        uuid = uuid,
                        sets = sets_display,
                        issues = table.concat(issues, ", ")
                    })
                end
            end
        end
    end
until cursor == "0"

-- Generate results based on requested data type
if data_type == "summary" then
    local completeness = {
        with_prices = total_cards - #missing_data.no_price,
        with_tcgplayer = total_cards - #missing_data.no_tcgplayer,
        with_images = total_cards - #missing_data.no_image,
        with_sets = total_cards - #missing_data.no_sets
    }
    
    return {
        "=== DATA COMPLETENESS SUMMARY ===",
        "",
        "Total cards analyzed: " .. total_cards,
        "",
        "=== COMPLETENESS STATISTICS ===",
        string.format("Cards with prices: %d/%d (%.1f%%)", 
                     completeness.with_prices, total_cards, 
                     completeness.with_prices/total_cards*100),
        string.format("Cards with TCGPlayer IDs: %d/%d (%.1f%%)", 
                     completeness.with_tcgplayer, total_cards, 
                     completeness.with_tcgplayer/total_cards*100),
        string.format("Cards with images: %d/%d (%.1f%%)", 
                     completeness.with_images, total_cards, 
                     completeness.with_images/total_cards*100),
        string.format("Cards with sets: %d/%d (%.1f%%)", 
                     completeness.with_sets, total_cards, 
                     completeness.with_sets/total_cards*100),
        "",
        "=== MISSING DATA COUNTS ===",
        "Missing prices: " .. #missing_data.no_price,
        "Missing TCGPlayer IDs: " .. #missing_data.no_tcgplayer,
        "Missing images: " .. #missing_data.no_image,
        "Missing sets: " .. #missing_data.no_sets,
        "Multiple issues: " .. #missing_data.incomplete,
        "",
        "Use specific data types for detailed lists:",
        "  prices, tcgplayer, images, sets, incomplete"
    }

elseif data_type == "prices" then
    local results = {
        "=== CARDS MISSING PRICE DATA ===",
        "",
        "Found " .. #missing_data.no_price .. " cards without prices:",
        ""
    }
    
    for i = 1, math.min(max_results, #missing_data.no_price) do
        local card = missing_data.no_price[i]
        results[#results + 1] = string.format("%2d. %-30s [%s]", 
                                             i, card.name, card.sets)
    end
    
    if #missing_data.no_price > max_results then
        results[#results + 1] = string.format("... and %d more", #missing_data.no_price - max_results)
    end
    
    return results

elseif data_type == "tcgplayer" then
    local results = {
        "=== CARDS MISSING TCGPLAYER IDS ===",
        "",
        "Found " .. #missing_data.no_tcgplayer .. " cards without TCGPlayer IDs:",
        ""
    }
    
    for i = 1, math.min(max_results, #missing_data.no_tcgplayer) do
        local card = missing_data.no_tcgplayer[i]
        results[#results + 1] = string.format("%2d. %-30s [%s]", 
                                             i, card.name, card.sets)
    end
    
    if #missing_data.no_tcgplayer > max_results then
        results[#results + 1] = string.format("... and %d more", #missing_data.no_tcgplayer - max_results)
    end
    
    return results

elseif data_type == "images" then
    local results = {
        "=== CARDS MISSING IMAGE DATA ===",
        "",
        "Found " .. #missing_data.no_image .. " cards without images:",
        ""
    }
    
    for i = 1, math.min(max_results, #missing_data.no_image) do
        local card = missing_data.no_image[i]
        results[#results + 1] = string.format("%2d. %-30s [%s]", 
                                             i, card.name, card.sets)
    end
    
    if #missing_data.no_image > max_results then
        results[#results + 1] = string.format("... and %d more", #missing_data.no_image - max_results)
    end
    
    return results

elseif data_type == "sets" then
    local results = {
        "=== CARDS MISSING SET DATA ===",
        "",
        "Found " .. #missing_data.no_sets .. " cards without sets:",
        ""
    }
    
    for i = 1, math.min(max_results, #missing_data.no_sets) do
        local card = missing_data.no_sets[i]
        results[#results + 1] = string.format("%2d. %-30s [%s]", 
                                             i, card.name, card.sets)
    end
    
    if #missing_data.no_sets > max_results then
        results[#results + 1] = string.format("... and %d more", #missing_data.no_sets - max_results)
    end
    
    return results

elseif data_type == "incomplete" then
    local results = {
        "=== CARDS WITH MULTIPLE MISSING DATA ===",
        "",
        "Found " .. #missing_data.incomplete .. " cards with multiple issues:",
        ""
    }
    
    for i = 1, math.min(max_results, #missing_data.incomplete) do
        local card = missing_data.incomplete[i]
        results[#results + 1] = string.format("%2d. %-25s [%s]", 
                                             i, card.name, card.sets)
        results[#results + 1] = string.format("    Issues: %s", card.issues)
    end
    
    if #missing_data.incomplete > max_results then
        results[#results + 1] = string.format("... and %d more", #missing_data.incomplete - max_results)
    end
    
    return results

else
    return {
        "=== FIND MISSING DATA USAGE ===",
        "",
        "Available data types:",
        "  summary     - Overview of data completeness",
        "  prices      - Cards missing price data",
        "  tcgplayer   - Cards missing TCGPlayer IDs",
        "  images      - Cards missing image data",
        "  sets        - Cards missing set data",
        "  incomplete  - Cards with multiple issues",
        "",
        "Examples:",
        "  python run_lua.py find_missing_data.lua summary",
        "  python run_lua.py find_missing_data.lua prices 50",
        "  python run_lua.py find_missing_data.lua incomplete 10"
    }
end 