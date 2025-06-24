-- Compare pricing data between different conditions and sources
-- Usage: EVAL script 0 [analysis_type] [param]
-- analysis_type: "summary", "conditions", "missing", "arbitrage"

local analysis_type = ARGV[1] or "summary"
local param = tonumber(ARGV[2]) or 20

local results = {}

if analysis_type == "summary" then
    -- Summary of pricing data coverage across conditions
    local price_counts = {}
    local total_cards = 0
    local cards_with_any_price = 0
    
    -- Get all cards
    local cursor = "0"
    repeat
        local result = redis.call("SCAN", cursor, "MATCH", "card:????????-????-????-????-????????????", "COUNT", 1000)
        cursor = result[1]
        local keys = result[2]
        
        for _, key in ipairs(keys) do
            -- Filter to direct card:UUID keys only
            local parts = {}
            for part in string.gmatch(key, "[^:]+") do
                table.insert(parts, part)
            end
            
            if #parts == 2 and string.len(parts[2]) == 36 then
                total_cards = total_cards + 1
                local uuid = parts[2]
                
                -- Check for pricing data across conditions
                local price_keys = redis.call("KEYS", "price:" .. uuid .. ":*")
                local card_has_price = false
                
                for _, price_key in ipairs(price_keys) do
                    local condition = string.match(price_key, ":([^:]+)$")
                    if condition then
                        price_counts[condition] = (price_counts[condition] or 0) + 1
                        card_has_price = true
                    end
                end
                
                if card_has_price then
                    cards_with_any_price = cards_with_any_price + 1
                end
            end
        end
    until cursor == "0"
    
    local coverage_pct = total_cards > 0 and (cards_with_any_price / total_cards * 100) or 0
    
    results = {
        "=== PRICING DATA COVERAGE SUMMARY ===",
        "",
        "Total cards: " .. total_cards,
        "Cards with any pricing: " .. cards_with_any_price .. " (" .. string.format("%.1f%%", coverage_pct) .. ")",
        "",
        "=== BY CONDITION ===",
    }
    
    -- Sort conditions by count
    local condition_list = {}
    for condition, count in pairs(price_counts) do
        table.insert(condition_list, {condition = condition, count = count})
    end
    table.sort(condition_list, function(a, b) return a.count > b.count end)
    
    for _, item in ipairs(condition_list) do
        local pct = total_cards > 0 and (item.count / total_cards * 100) or 0
        results[#results + 1] = string.format("%-15s: %6d cards (%5.1f%%)", 
                                             item.condition, item.count, pct)
    end
    
    results[#results + 1] = ""
    results[#results + 1] = "Use 'conditions' to see price differences between conditions"
    results[#results + 1] = "Use 'missing' to find cards without pricing"

elseif analysis_type == "conditions" then
    -- Compare prices between different conditions for the same card
    local condition_diffs = {}
    local min_diff = tonumber(param) or 5.0  -- Default $5 minimum difference
    
    local cursor = "0"
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
                local uuid = parts[2]
                
                -- Get all pricing data for this card
                local price_keys = redis.call("KEYS", "price:" .. uuid .. ":*")
                local card_prices = {}
                
                for _, price_key in ipairs(price_keys) do
                    local condition = string.match(price_key, ":([^:]+)$")
                    local price_data = redis.call("GET", price_key)
                    
                    if condition and price_data then
                        local price_match = string.match(price_data, '"tcg_market_price":%s*([%d%.]+)')
                        if price_match then
                            local price = tonumber(price_match)
                            if price then
                                card_prices[condition] = price
                            end
                        end
                    end
                end
                
                -- Compare conditions if we have multiple prices
                if next(card_prices) then
                    local conditions = {}
                    for condition, price in pairs(card_prices) do
                        table.insert(conditions, {condition = condition, price = price})
                    end
                    table.sort(conditions, function(a, b) return a.price > b.price end)
                    
                    -- Look for significant differences
                    if #conditions >= 2 then
                        local max_price = conditions[1].price
                        local min_price = conditions[#conditions].price
                        local diff = max_price - min_price
                        
                        if diff >= min_diff then
                            -- Get card name
                            local card_data = redis.call("GET", key)
                            local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                            
                            if name_match then
                                table.insert(condition_diffs, {
                                    name = name_match,
                                    uuid = uuid,
                                    max_price = max_price,
                                    min_price = min_price,
                                    diff = diff,
                                    max_condition = conditions[1].condition,
                                    min_condition = conditions[#conditions].condition,
                                    all_prices = card_prices
                                })
                            end
                        end
                    end
                end
            end
        end
    until cursor == "0"
    
    -- Sort by difference (descending)
    table.sort(condition_diffs, function(a, b) return a.diff > b.diff end)
    
    results = {
        "=== CONDITION PRICE DIFFERENCES (>= $" .. min_diff .. ") ===",
        "",
        "Found " .. #condition_diffs .. " cards with significant condition differences:",
        "",
        string.format("%-25s %10s %10s %8s %-12s %-12s", 
                     "Card Name", "High", "Low", "Diff", "High Cond", "Low Cond"),
        string.rep("-", 80)
    }
    
    for i = 1, math.min(25, #condition_diffs) do
        local card = condition_diffs[i]
        results[#results + 1] = string.format("%-25s $%8.2f $%8.2f $%6.2f %-12s %-12s",
                                             card.name:sub(1, 25), 
                                             card.max_price, card.min_price, card.diff,
                                             card.max_condition, card.min_condition)
    end
    
    if #condition_diffs > 25 then
        results[#results + 1] = string.format("... and %d more differences", #condition_diffs - 25)
    end

elseif analysis_type == "missing" then
    -- Find cards missing pricing from specific conditions
    local missing_by_condition = {}
    local cards_with_some_pricing = 0
    
    local cursor = "0"
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
                local uuid = parts[2]
                
                -- Check what conditions this card has pricing for
                local price_keys = redis.call("KEYS", "price:" .. uuid .. ":*")
                local available_conditions = {}
                
                for _, price_key in ipairs(price_keys) do
                    local condition = string.match(price_key, ":([^:]+)$")
                    if condition then
                        available_conditions[condition] = true
                    end
                end
                
                if next(available_conditions) then
                    cards_with_some_pricing = cards_with_some_pricing + 1
                    
                    -- Check for common conditions that might be missing
                    local common_conditions = {"Near Mint", "Lightly Played", "Moderately Played", "Heavily Played"}
                    
                    for _, condition in ipairs(common_conditions) do
                        if not available_conditions[condition] then
                            if not missing_by_condition[condition] then
                                missing_by_condition[condition] = {}
                            end
                            
                            -- Get card name
                            local card_data = redis.call("GET", key)
                            local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                            
                            if name_match then
                                table.insert(missing_by_condition[condition], {
                                    name = name_match,
                                    uuid = uuid,
                                    has_conditions = {}
                                })
                                
                                -- Store what conditions it does have
                                for cond, _ in pairs(available_conditions) do
                                    table.insert(missing_by_condition[condition][#missing_by_condition[condition]].has_conditions, cond)
                                end
                            end
                        end
                    end
                end
            end
        end
    until cursor == "0"
    
    results = {
        "=== MISSING CONDITION PRICING ===",
        "",
        "Cards with some pricing: " .. cards_with_some_pricing,
        ""
    }
    
    for condition, cards in pairs(missing_by_condition) do
        results[#results + 1] = "=== MISSING " .. condition:upper() .. " PRICING ==="
        results[#results + 1] = "Found " .. #cards .. " cards without " .. condition .. " pricing:"
        results[#results + 1] = ""
        
        for i = 1, math.min(param, #cards) do
            local card = cards[i]
            local has_conditions_str = table.concat(card.has_conditions, ", ")
            results[#results + 1] = string.format("%2d. %-30s (has: %s)", 
                                                 i, card.name, has_conditions_str)
        end
        
        if #cards > param then
            results[#results + 1] = string.format("... and %d more", #cards - param)
        end
        results[#results + 1] = ""
    end

else
    results = {
        "=== PRICE COMPARISON USAGE ===",
        "",
        "Available analysis types:",
        "  summary              - Coverage summary across conditions",
        "  conditions [min_diff]- Price differences between conditions (default $5)",
        "  missing [count]      - Cards missing specific condition pricing (default 20)",
        "",
        "Examples:",
        "  python run_lua.py price_comparison.lua summary",
        "  python run_lua.py price_comparison.lua conditions 10",
        "  python run_lua.py price_comparison.lua missing 50"
    }
end

return results 