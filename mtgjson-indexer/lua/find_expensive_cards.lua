-- Find expensive cards above a specified price threshold
-- Usage: EVAL script 0 <min_price> [max_results]
local min_price = tonumber(ARGV[1]) or 50
local max_results = tonumber(ARGV[2]) or 20

local expensive_cards = {}

-- Scan through all MTGJSON pricing keys (price:UUID:condition pattern)
local cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "price:????????-????-????-????-????????????:*", "COUNT", 1000)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        local price_data = redis.call("GET", key)
        if price_data then
            -- Extract TCG market price from JSON
            local price_match = string.match(price_data, '"tcg_market_price":%s*([%d%.]+)')
            if price_match then
                local price = tonumber(price_match)
                if price and price >= min_price then
                    -- Extract UUID from key (price:UUID:condition)
                    local uuid = string.match(key, "price:([^:]+):")
                    
                    if uuid then
                        -- Get card details from card:UUID
                        local card_key = "card:" .. uuid
                        local card_data = redis.call("GET", card_key)
                        if card_data then
                            -- Extract card name from JSON
                            local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                            local set_match = string.match(card_data, '"set_code":%s*"([^"]+)"')
                            
                            if name_match then
                                -- Extract condition from pricing key
                                local condition = string.match(key, ":([^:]+)$") or "Unknown"
                                
                                table.insert(expensive_cards, {
                                    name = name_match,
                                    price = price,
                                    set = set_match or "Unknown",
                                    condition = condition,
                                    uuid = uuid
                                })
                            end
                        end
                    end
                end
            end
        end
    end
until cursor == "0"

-- Sort by price (descending)
table.sort(expensive_cards, function(a, b) return a.price > b.price end)

-- Return JSON array 
local json_results = {}

for i = 1, math.min(max_results, #expensive_cards) do
    local card = expensive_cards[i]
    local result_entry = cjson.encode({
        uuid = card.uuid,
        name = card.name,
        price = card.price,
        condition = card.condition,
        set_code = card.set,
        rank = i,
        min_price_threshold = min_price,
        total_found = #expensive_cards
    })
    table.insert(json_results, result_entry)
end

return json_results 