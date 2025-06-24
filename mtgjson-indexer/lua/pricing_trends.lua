-- Analyze pricing trends and distribution patterns
-- Usage: EVAL script 0 [analysis_type] [param]
-- analysis_type: "distribution", "by_set", "by_rarity", "outliers"

local analysis_type = ARGV[1] or "distribution"
local param = ARGV[2]

if analysis_type == "distribution" then
    -- Price distribution analysis using MTGJSON card data
    local price_ranges = {
        {min = 0, max = 1, label = "$0-1", count = 0},
        {min = 1, max = 5, label = "$1-5", count = 0},
        {min = 5, max = 10, label = "$5-10", count = 0},
        {min = 10, max = 25, label = "$10-25", count = 0},
        {min = 25, max = 50, label = "$25-50", count = 0},
        {min = 50, max = 100, label = "$50-100", count = 0},
        {min = 100, max = 500, label = "$100-500", count = 0},
        {min = 500, max = math.huge, label = "$500+", count = 0}
    }
    
    local total_priced = 0
    local total_value = 0
    local max_price = 0
    local max_card = ""
    
    -- Scan all MTGJSON cards
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
                local card_data = redis.call("GET", key)
                if card_data then
                    -- Extract TCG market price from JSON
                    local price_match = string.match(card_data, '"tcg_market_price":%s*([%d%.]+)')
                    if price_match then
                        local price = tonumber(price_match)
                        if price and price > 0 then
                            total_priced = total_priced + 1
                            total_value = total_value + price
                            
                            -- Track max price
                            if price > max_price then
                                max_price = price
                                local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                                max_card = name_match or "Unknown"
                            end
                            
                            -- Categorize by price range
                            for _, range in ipairs(price_ranges) do
                                if price >= range.min and price < range.max then
                                    range.count = range.count + 1
                                    break
                                end
                            end
                        end
                    end
                end
            end
        end
    until cursor == "0"
    
    local avg_price = total_priced > 0 and (total_value / total_priced) or 0
    
    local results = {
        "=== PRICING DISTRIBUTION ANALYSIS ===",
        "",
        "Total cards with prices: " .. total_priced,
        string.format("Total collection value: $%.2f", total_value),
        string.format("Average card price: $%.2f", avg_price),
        string.format("Most expensive: %s ($%.2f)", max_card, max_price),
        "",
        "=== PRICE DISTRIBUTION ===",
    }
    
    for _, range in ipairs(price_ranges) do
        local percentage = total_priced > 0 and (range.count / total_priced * 100) or 0
        results[#results + 1] = string.format("%-10s: %6d cards (%5.1f%%)", 
                                             range.label, range.count, percentage)
    end
    
    return results

elseif analysis_type == "by_set" then
    -- Price analysis by set using Redis Search
    local target_set = param
    
    if target_set then
        -- Analysis for specific set
        local success, search_result = pcall(function()
            return redis.call("FT.SEARCH", "search_cards", "@set_code:{" .. target_set .. "}", "LIMIT", "0", "10000")
        end)
        
        if not success or search_result[1] == 0 then
            return {"Set '" .. target_set .. "' not found or no cards"}
        end
        
        local prices = {}
        local total_cards = search_result[1]
        
        -- Parse search results (format: count, id1, doc1, id2, doc2, ...)
        for i = 2, #search_result, 2 do
            local card_id = search_result[i]
            local card_data = redis.call("GET", card_id)
            if card_data then
                local price_match = string.match(card_data, '"tcg_market_price":%s*([%d%.]+)')
                if price_match then
                    local price = tonumber(price_match)
                    if price and price > 0 then
                        table.insert(prices, price)
                    end
                end
            end
        end
        
        if #prices == 0 then
            return {"No price data found for set " .. target_set}
        end
        
        -- Sort prices for statistics
        table.sort(prices)
        
        local total = 0
        for _, price in ipairs(prices) do
            total = total + price
        end
        
        local median = #prices > 0 and prices[math.ceil(#prices/2)] or 0
        local avg = #prices > 0 and (total / #prices) or 0
        local min_price = prices[1] or 0
        local max_price = prices[#prices] or 0
        
        return {
            "=== PRICING ANALYSIS: " .. string.upper(target_set) .. " ===",
            "",
            "Cards with prices: " .. #prices .. "/" .. total_cards,
            string.format("Total value: $%.2f", total),
            string.format("Average price: $%.2f", avg),
            string.format("Median price: $%.2f", median),
            string.format("Price range: $%.2f - $%.2f", min_price, max_price),
        }
    else
        -- Analysis across all sets using Redis Search aggregation
        local success, sets_result = pcall(function()
            return redis.call("FT.AGGREGATE", "search_cards", "*",
                             "GROUPBY", "1", "@set_code",
                             "REDUCE", "COUNT", "0", "AS", "total_cards",
                             "SORTBY", "2", "@total_cards", "DESC",
                             "LIMIT", "0", "20")
        end)
        
        if not success then
            return {"Redis Search not available for set analysis"}
        end
        
        local results = {
            "=== PRICING TRENDS BY SET ===",
            "",
            string.format("%-8s %6s %6s %8s %8s %10s", 
                         "Set", "Cards", "Priced", "Avg $", "Max $", "Total $"),
            string.rep("-", 60)
        }
        
        -- Parse aggregation results
        for i = 2, math.min(22, #sets_result), 2 do
            local set_info = sets_result[i]
            local set_code = ""
            local total_cards = 0
            
            for j = 1, #set_info, 2 do
                if set_info[j] == "set_code" then
                    set_code = set_info[j + 1]
                elseif set_info[j] == "total_cards" then
                    total_cards = tonumber(set_info[j + 1]) or 0
                end
            end
            
            if set_code ~= "" and total_cards > 0 then
                -- Get pricing data for this set
                local set_search = pcall(function()
                    return redis.call("FT.SEARCH", "search_cards", "@set_code:{" .. set_code .. "}", "LIMIT", "0", "1000")
                end)
                
                if set_search then
                    local prices = {}
                    local max_price = 0
                    
                    -- Would need to process individual cards for pricing - simplified for now
                    results[#results + 1] = string.format("%-8s %6d %6s %8s %8s %10s",
                                                         set_code, total_cards, "N/A", "N/A", "N/A", "N/A")
                end
            end
        end
        
        return results
    end

elseif analysis_type == "outliers" then
    -- Find pricing outliers (unusually expensive cards)
    local threshold = tonumber(param) or 100 -- Default $100+
    
    local outliers = {}
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
                local card_data = redis.call("GET", key)
                if card_data then
                    local price_match = string.match(card_data, '"tcg_market_price":%s*([%d%.]+)')
                    if price_match then
                        local price = tonumber(price_match)
                        if price and price >= threshold then
                            local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                            local set_match = string.match(card_data, '"set_code":%s*"([^"]+)"')
                            
                            if name_match then
                                table.insert(outliers, {
                                    name = name_match,
                                    price = price,
                                    set = set_match or "Unknown"
                                })
                            end
                        end
                    end
                end
            end
        end
    until cursor == "0"
    
    -- Sort by price (descending)
    table.sort(outliers, function(a, b) return a.price > b.price end)
    
    local results = {
        "=== PRICING OUTLIERS (>= $" .. threshold .. ") ===",
        "",
        "Found " .. #outliers .. " high-value cards:",
        ""
    }
    
    for i = 1, math.min(25, #outliers) do
        local card = outliers[i]
        results[#results + 1] = string.format("%2d. %-35s $%7.2f (%s)", 
                                             i, card.name, card.price, card.set)
    end
    
    return results

else
    return {
        "=== PRICING TRENDS USAGE ===",
        "",
        "Available analysis types:",
        "  distribution  - Price distribution across ranges",
        "  by_set [set]  - Price analysis by set",
        "  outliers [min]- Find expensive cards (default $100+)",
        "",
        "Examples:",
        "  python run_lua.py pricing_trends.lua distribution",
        "  python run_lua.py pricing_trends.lua by_set lea",
        "  python run_lua.py pricing_trends.lua outliers 50"
    }
end 