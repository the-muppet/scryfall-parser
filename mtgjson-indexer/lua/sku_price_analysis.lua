-- SKU-based Price Analysis Script
-- This script demonstrates the improved SKU-based time-series pricing system
-- Usage: EVAL script 0 [analysis_type] [param1] [param2]
-- analysis_type: "history", "trending", "arbitrage", "condition_compare"

local analysis_type = ARGV[1] or "history"
local param1 = ARGV[2]
local param2 = ARGV[3]

-- Helper function to get SKU metadata
local function get_sku_metadata(sku_id)
    local meta_key = "sku:" .. sku_id .. ":meta"
    local meta_json = redis.call("GET", meta_key)
    
    if meta_json then
        return cjson.decode(meta_json)
    end
    
    return {
        condition = "Unknown",
        language = "Unknown", 
        foil = false,
        product_id = "",
        product_name = "Unknown Product",
        set_name = "Unknown Set"
    }
end

-- Helper function to get card name from SKU
local function get_card_name_from_sku(sku_id)
    local card_uuid = redis.call("GET", "sku:" .. sku_id .. ":card")
    
    if card_uuid then
        local card_data = redis.call("GET", "card:" .. card_uuid)
        if card_data then
            local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
            return name_match or "Unknown Card"
        end
    end
    
    return "Unknown Card"
end

-- Helper function to get latest price for SKU
local function get_latest_sku_price(sku_id)
    local price_data = redis.call("GET", "price:sku:" .. sku_id .. ":latest")
    
    if price_data then
        local price_obj = cjson.decode(price_data)
        return price_obj.tcg_market_price
    end
    
    return nil
end

if analysis_type == "history" then
    -- Get price history for a specific SKU
    local sku_id = param1
    local days = tonumber(param2) or 30
    
    if not sku_id then
        return {"Error: SKU ID required for history analysis"}
    end
    
    local end_time = redis.call("TIME")[1]
    local start_time = end_time - (days * 86400)
    
    local history = redis.call("ZRANGEBYSCORE", 
                              "price:sku:" .. sku_id .. ":history",
                              start_time, end_time, "WITHSCORES")
    
    if #history == 0 then
        return {"No price history found for SKU " .. sku_id}
    end
    
    -- Get SKU metadata
    local sku_meta = get_sku_metadata(sku_id)
    local card_name = get_card_name_from_sku(sku_id)
    
    local results = {
        "=== PRICE HISTORY: " .. card_name .. " ===",
        "SKU ID: " .. sku_id,
        "Condition: " .. sku_meta.condition,
        "Language: " .. sku_meta.language,
        "Foil: " .. (sku_meta.foil and "Yes" or "No"),
        "",
        "Price History (Last " .. days .. " days):",
        ""
    }
    
    -- Process history (timestamp, price pairs)
    for i = 1, #history, 2 do
        local price = history[i]
        local timestamp = history[i + 1]
        
        -- Convert timestamp to readable date
        local date = os.date("%Y-%m-%d %H:%M", timestamp)
        results[#results + 1] = string.format("%s: $%.2f", date, price)
    end
    
    -- Calculate trend
    if #history >= 4 then
        local first_price = tonumber(history[1])
        local last_price = tonumber(history[#history - 1])
        local change_pct = ((last_price - first_price) / first_price) * 100
        
        results[#results + 1] = ""
        results[#results + 1] = string.format("Trend: %.1f%% (%s)",
                                             math.abs(change_pct),
                                             change_pct > 0 and "UP" or "DOWN")
    end
    
    return results

elseif analysis_type == "trending" then
    -- Compute trending SKUs from price history (since price:trending:* doesn't exist)
    local direction = param1 or "up"
    local limit = tonumber(param2) or 20
    local days = 7  -- Look at last 7 days for trending
    
    local results = {
        "=== TRENDING " .. string.upper(direction) .. " SKUS (Last " .. days .. " days) ===",
        "",
        "Note: Computing trends from price history data...",
        "",
        string.format("%-5s %-30s %-12s %-8s %-8s %s", 
                     "Rank", "Card Name", "Condition", "Change%", "Price", "SKU ID"),
        string.rep("-", 80)
    }
    
    -- Get all SKU price history keys to analyze
    local history_keys = redis.call("KEYS", "price:sku:*:history")
    local trending_data = {}
    
    local end_time = redis.call("TIME")[1]
    local start_time = end_time - (days * 86400)
    
    -- Analyze a limited sample (first 1000 to avoid timeout)
    for i = 1, math.min(1000, #history_keys) do
        local history_key = history_keys[i]
        local sku_id = string.match(history_key, "price:sku:([^:]+):history")
        
        if sku_id then
            local history = redis.call("ZRANGEBYSCORE", history_key, start_time, end_time, "WITHSCORES")
            
            if #history >= 4 then  -- Need at least 2 price points
                local first_price = tonumber(history[1])
                local last_price = tonumber(history[#history - 1])
                
                if first_price and last_price and first_price > 0 then
                    local change_pct = ((last_price - first_price) / first_price) * 100
                    
                    -- Filter by direction
                    if (direction == "up" and change_pct > 5) or (direction == "down" and change_pct < -5) then
                        table.insert(trending_data, {
                            sku_id = sku_id,
                            change_pct = change_pct,
                            current_price = last_price
                        })
                    end
                end
            end
        end
    end
    
    -- Sort by change percentage
    if direction == "up" then
        table.sort(trending_data, function(a, b) return a.change_pct > b.change_pct end)
    else
        table.sort(trending_data, function(a, b) return a.change_pct < b.change_pct end)
    end
    
    -- Return top results
    for i = 1, math.min(limit, #trending_data) do
        local data = trending_data[i]
        local card_name = get_card_name_from_sku(data.sku_id)
        local sku_meta = get_sku_metadata(data.sku_id)
        
        results[#results + 1] = string.format("%-5d %-30s %-12s %+7.1f%% $%7.2f %s",
                                             i,
                                             string.sub(card_name, 1, 30),
                                             sku_meta.condition,
                                             data.change_pct,
                                             data.current_price,
                                             data.sku_id)
    end
    
    if #trending_data == 0 then
        results[#results + 1] = "No significant trending " .. direction .. " SKUs found"
        results[#results + 1] = "(Analyzed " .. math.min(1000, #history_keys) .. " SKUs with price history)"
    end
    
    return results

elseif analysis_type == "arbitrage" then
    -- Find arbitrage opportunities between conditions
    local min_diff = tonumber(param2) or 5.0
    
    local arbitrage_opportunities = {}
    
    -- Get all cards and check for condition price differences
    local cursor = "0"
    repeat
        local result = redis.call("SCAN", cursor, "MATCH", "card:????????-????-????-????-????????????", "COUNT", 500)
        cursor = result[1]
        local keys = result[2]
        
        for _, card_key in ipairs(keys) do
            -- Filter to direct card:UUID keys only
            local parts = {}
            for part in string.gmatch(card_key, "[^:]+") do
                table.insert(parts, part)
            end
            
            if #parts == 2 and string.len(parts[2]) == 36 then
                local card_uuid = parts[2]
                
                -- Get card name
                local card_data = redis.call("GET", card_key)
                local card_name = "Unknown"
                if card_data then
                    local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                    if name_match then
                        card_name = name_match
                    end
                end
                
                -- Get all SKUs for this card
                local sku_ids = redis.call("SMEMBERS", "card:" .. card_uuid .. ":skus")
                local condition_prices = {}
                
                for _, sku_id in ipairs(sku_ids) do
                    local sku_meta = get_sku_metadata(sku_id)
                    local price = get_latest_sku_price(sku_id)
                    
                    if price and price > 0 then
                        if not condition_prices[sku_meta.condition] or condition_prices[sku_meta.condition].price > price then
                            condition_prices[sku_meta.condition] = {
                                price = price,
                                sku_id = sku_id,
                                foil = sku_meta.foil
                            }
                        end
                    end
                end
                
                -- Look for arbitrage opportunities
                local conditions = {}
                for condition, data in pairs(condition_prices) do
                    table.insert(conditions, {condition = condition, price = data.price, sku_id = data.sku_id, foil = data.foil})
                end
                
                if #conditions >= 2 then
                    table.sort(conditions, function(a, b) return a.price > b.price end)
                    
                    local max_price = conditions[1].price
                    local min_price = conditions[#conditions].price
                    local diff = max_price - min_price
                    
                    if diff >= min_diff then
                        table.insert(arbitrage_opportunities, {
                            card_name = card_name,
                            high_price = max_price,
                            low_price = min_price,
                            diff = diff,
                            high_condition = conditions[1].condition,
                            low_condition = conditions[#conditions].condition,
                            high_sku = conditions[1].sku_id,
                            low_sku = conditions[#conditions].sku_id
                        })
                    end
                end
            end
        end
    until cursor == "0"
    
    -- Sort by difference (descending)
    table.sort(arbitrage_opportunities, function(a, b) return a.diff > b.diff end)
    
    local results = {
        "=== CONDITION ARBITRAGE OPPORTUNITIES ===",
        "",
        string.format("%-25s %10s %10s %8s %-12s %-12s", 
                     "Card Name", "High $", "Low $", "Diff $", "High Cond", "Low Cond"),
        string.rep("-", 85)
    }
    
    for i = 1, math.min(20, #arbitrage_opportunities) do
        local opp = arbitrage_opportunities[i]
        results[#results + 1] = string.format("%-25s $%8.2f $%8.2f $%6.2f %-12s %-12s",
                                             string.sub(opp.card_name, 1, 25),
                                             opp.high_price, opp.low_price, opp.diff,
                                             opp.high_condition, opp.low_condition)
    end
    
    if #arbitrage_opportunities == 0 then
        results[#results + 1] = "No arbitrage opportunities found with current criteria"
    end
    
    return results

elseif analysis_type == "condition_compare" then
    -- Compare prices across all conditions for a specific card
    local card_name_search = param1
    
    if not card_name_search then
        return {"Error: Card name required for condition comparison"}
    end
    
    -- Find cards matching the name
    local matching_cards = {}
    local cursor = "0"
    repeat
        local result = redis.call("SCAN", cursor, "MATCH", "card:????????-????-????-????-????????????", "COUNT", 500)
        cursor = result[1]
        local keys = result[2]
        
        for _, card_key in ipairs(keys) do
            -- Filter to direct card:UUID keys only
            local parts = {}
            for part in string.gmatch(card_key, "[^:]+") do
                table.insert(parts, part)
            end
            
            if #parts == 2 and string.len(parts[2]) == 36 then
                local card_data = redis.call("GET", card_key)
                if card_data then
                    local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                    if name_match and string.find(string.lower(name_match), string.lower(card_name_search)) then
                        table.insert(matching_cards, {
                            uuid = parts[2],
                            name = name_match
                        })
                    end
                end
            end
        end
    until cursor == "0"
    
    if #matching_cards == 0 then
        return {"No cards found matching: " .. card_name_search}
    end
    
    local results = {
        "=== CONDITION PRICE COMPARISON ===",
        "",
        "Found " .. #matching_cards .. " matching card(s):",
        ""
    }
    
    for _, card in ipairs(matching_cards) do
        results[#results + 1] = "Card: " .. card.name
        results[#results + 1] = string.rep("-", 50)
        
        -- Get all SKUs for this card
        local sku_ids = redis.call("SMEMBERS", "card:" .. card.uuid .. ":skus")
        local condition_data = {}
        
        for _, sku_id in ipairs(sku_ids) do
            local sku_meta = get_sku_metadata(sku_id)
            local price = get_latest_sku_price(sku_id)
            
            if price and price > 0 then
                local key = sku_meta.condition .. (sku_meta.foil and " (Foil)" or "")
                if not condition_data[key] or condition_data[key].price > price then
                    condition_data[key] = {
                        price = price,
                        sku_id = sku_id,
                        language = sku_meta.language
                    }
                end
            end
        end
        
        -- Sort conditions by price
        local conditions = {}
        for condition, data in pairs(condition_data) do
            table.insert(conditions, {
                condition = condition,
                price = data.price,
                sku_id = data.sku_id,
                language = data.language
            })
        end
        table.sort(conditions, function(a, b) return a.price > b.price end)
        
        if #conditions > 0 then
            for _, cond in ipairs(conditions) do
                results[#results + 1] = string.format("  %-20s $%8.2f (%s) [SKU: %s]",
                                                     cond.condition, cond.price, cond.language, cond.sku_id)
            end
        else
            results[#results + 1] = "  No pricing data available"
        end
        
        results[#results + 1] = ""
    end
    
    return results

else
    return {
        "=== SKU PRICE ANALYSIS USAGE ===",
        "",
        "Available analysis types:",
        "  history <sku_id> [days]        - Price history for specific SKU (default 30 days)",
        "  trending <up|down> [limit]     - Trending SKUs by price change (default 20)",
        "  arbitrage [min]  - Condition arbitrage opportunities (default $5)",
        "  condition_compare <card_name>  - Compare all conditions for a card",
        "",
        "Examples:",
        "  python run_lua.py sku_price_analysis.lua history 12345 7",
        "  python run_lua.py sku_price_analysis.lua trending up 10",
        "  python run_lua.py sku_price_analysis.lua arbitrage 'Black Lotus' 50",
        "  python run_lua.py sku_price_analysis.lua condition_compare 'Lightning Bolt'"
    }
end 