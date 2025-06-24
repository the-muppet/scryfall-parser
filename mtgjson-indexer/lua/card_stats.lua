-- Get comprehensive statistics about the MTGJSON card database
local stats = {}

-- Count total cards (MTGJSON pattern: card:UUID)
local card_count = 0
local cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "card:????????-????-????-????-????????????", "COUNT", 1000)
    cursor = result[1]
    
    -- Filter out non-card keys (like card:UUID:deck_info)
    for _, key in ipairs(result[2]) do
        local parts = {}
        for part in string.gmatch(key, "[^:]+") do
            table.insert(parts, part)
        end
        -- Only count direct card:UUID keys (2 parts exactly)
        if #parts == 2 and string.len(parts[2]) == 36 then
            card_count = card_count + 1
        end
    end
until cursor == "0"

stats.total_cards = card_count

-- Count cards by set using existing set indexes
local set_counts = {}
local total_sets = 0

-- Get sets by scanning for set:*:cards patterns
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "set:*:cards", "COUNT", 1000)
    cursor = result[1]
    
    for _, set_key in ipairs(result[2]) do
        local set_code = string.match(set_key, "set:([^:]+):cards")
        if set_code then
            local set_card_count = redis.call("SCARD", set_key)
            if set_card_count > 0 then
                set_counts[set_code] = set_card_count
                total_sets = total_sets + 1
            end
        end
    end
until cursor == "0"

-- Get top 10 sets by card count
local top_sets = {}
for set_code, count in pairs(set_counts) do
    table.insert(top_sets, {set = set_code, count = count})
end

-- Sort sets by count (descending)
table.sort(top_sets, function(a, b) return a.count > b.count end)

-- Take top 10
stats.top_sets = {}
for i = 1, math.min(10, #top_sets) do
    stats.top_sets[i] = top_sets[i]
end

stats.total_sets = total_sets

-- Count cards with pricing data (separate price:UUID:condition keys)
local cards_with_prices = 0
local unique_priced_cards = {}

cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "price:????????-????-????-????-????????????:*", "COUNT", 1000)
    cursor = result[1]
    
    for _, price_key in ipairs(result[2]) do
        -- Extract UUID from price:UUID:condition pattern
        local uuid = string.match(price_key, "price:([^:]+):")
        if uuid and string.len(uuid) == 36 then
            unique_priced_cards[uuid] = true
        end
    end
until cursor == "0"

-- Count unique cards with pricing
for _ in pairs(unique_priced_cards) do
    cards_with_prices = cards_with_prices + 1
end

stats.cards_with_prices = cards_with_prices

-- Count expensive cards by checking pricing data
local expensive_cards = {over_10 = 0, over_50 = 0, over_100 = 0}

cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "price:????????-????-????-????-????????????:*", "COUNT", 500)
    cursor = result[1]
    
    for _, price_key in ipairs(result[2]) do
        local price_data = redis.call("GET", price_key)
        if price_data then
            -- Extract TCG market price from JSON
            local price_match = string.match(price_data, '"tcg_market_price":%s*([%d%.]+)')
            if price_match then
                local price = tonumber(price_match)
                if price then
                    if price > 10 then expensive_cards.over_10 = expensive_cards.over_10 + 1 end
                    if price > 50 then expensive_cards.over_50 = expensive_cards.over_50 + 1 end
                    if price > 100 then expensive_cards.over_100 = expensive_cards.over_100 + 1 end
                end
            end
        end
    end
until cursor == "0"

stats.expensive_cards = expensive_cards

-- Count index sizes
stats.index_sizes = {
    ngrams = 0,
    metaphones = 0,
    words = 0,
    prefixes = 0
}

-- Count n-grams
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "ngram:*", "COUNT", 1000)
    cursor = result[1]
    stats.index_sizes.ngrams = stats.index_sizes.ngrams + #result[2]
until cursor == "0"

-- Count metaphones
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "metaphone:*", "COUNT", 1000)
    cursor = result[1]
    stats.index_sizes.metaphones = stats.index_sizes.metaphones + #result[2]
until cursor == "0"

-- Count words
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "word:*", "COUNT", 1000)
    cursor = result[1]
    stats.index_sizes.words = stats.index_sizes.words + #result[2]
until cursor == "0"

-- Count prefixes
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "auto:prefix:*", "COUNT", 1000)
    cursor = result[1]
    stats.index_sizes.prefixes = stats.index_sizes.prefixes + #result[2]
until cursor == "0"

-- Return the complete stats
return {
    "=== MTGJSON CARD DATABASE STATISTICS ===",
    "Total Cards: " .. stats.total_cards,
    "Total Sets: " .. stats.total_sets,
    "Cards with Prices: " .. stats.cards_with_prices,
    "",
    "=== EXPENSIVE CARDS ===",
    "Over $10: " .. expensive_cards.over_10,
    "Over $50: " .. expensive_cards.over_50,
    "Over $100: " .. expensive_cards.over_100,
    "",
    "=== TOP 10 SETS BY CARD COUNT ===",
    stats.top_sets[1] and (stats.top_sets[1].set .. ": " .. stats.top_sets[1].count) or "No data",
    stats.top_sets[2] and (stats.top_sets[2].set .. ": " .. stats.top_sets[2].count) or "",
    stats.top_sets[3] and (stats.top_sets[3].set .. ": " .. stats.top_sets[3].count) or "",
    stats.top_sets[4] and (stats.top_sets[4].set .. ": " .. stats.top_sets[4].count) or "",
    stats.top_sets[5] and (stats.top_sets[5].set .. ": " .. stats.top_sets[5].count) or "",
    "",
    "=== SEARCH INDEX SIZES ===",
    "N-grams: " .. stats.index_sizes.ngrams,
    "Metaphones: " .. stats.index_sizes.metaphones,
    "Words: " .. stats.index_sizes.words,
    "Prefixes: " .. stats.index_sizes.prefixes
} 