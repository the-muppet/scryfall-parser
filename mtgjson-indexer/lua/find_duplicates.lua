-- Find cards with similar or duplicate names
-- Usage: EVAL script 0 [similarity_threshold]
local threshold = tonumber(ARGV[1]) or 0.8

-- Simple Levenshtein distance function
local function levenshtein(s1, s2)
    local len1, len2 = #s1, #s2
    local matrix = {}
    
    for i = 0, len1 do
        matrix[i] = {[0] = i}
    end
    
    for j = 0, len2 do
        matrix[0][j] = j
    end
    
    for i = 1, len1 do
        for j = 1, len2 do
            local cost = (s1:sub(i, i) == s2:sub(j, j)) and 0 or 1
            matrix[i][j] = math.min(
                matrix[i-1][j] + 1,      -- deletion
                matrix[i][j-1] + 1,      -- insertion
                matrix[i-1][j-1] + cost  -- substitution
            )
        end
    end
    
    return matrix[len1][len2]
end

-- Calculate similarity ratio
local function similarity(s1, s2)
    local len = math.max(#s1, #s2)
    if len == 0 then return 1.0 end
    return 1.0 - (levenshtein(s1, s2) / len)
end

-- Get all card names from MTGJSON data
local cards = {}
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
                local uuid = parts[2]
                
                -- Extract card name and set from JSON
                local name_match = string.match(card_data, '"name":%s*"([^"]+)"')
                local set_match = string.match(card_data, '"set_code":%s*"([^"]+)"')
                
                if name_match then
                    table.insert(cards, {
                        name = name_match,
                        name_lower = string.lower(name_match),
                        uuid = uuid,
                        set = set_match or "Unknown"
                    })
                end
            end
        end
    end
until cursor == "0"

-- Find similar cards
local similar_groups = {}
local processed = {}

for i = 1, #cards do
    if not processed[i] then
        local group = {cards[i]}
        processed[i] = true
        
        for j = i + 1, #cards do
            if not processed[j] then
                local sim = similarity(cards[i].name_lower, cards[j].name_lower)
                if sim >= threshold then
                    table.insert(group, cards[j])
                    processed[j] = true
                end
            end
        end
        
        -- Only include groups with more than 1 card
        if #group > 1 then
            table.insert(similar_groups, group)
        end
    end
end

-- Sort groups by number of similar cards (descending)
table.sort(similar_groups, function(a, b) return #a > #b end)

-- Format results
local results = {
    "=== SIMILAR/DUPLICATE CARD NAMES ===",
    string.format("Similarity threshold: %.1f%%", threshold * 100),
    "",
    "Found " .. #similar_groups .. " groups of similar cards:",
    ""
}

for group_num, group in ipairs(similar_groups) do
    if group_num > 50 then break end -- Limit output
    
    results[#results + 1] = "Group " .. group_num .. " (" .. #group .. " cards):"
    
    for _, card in ipairs(group) do
        results[#results + 1] = string.format("  • %-40s [%s]", card.name, card.set)
    end
    
    results[#results + 1] = ""
end

if #similar_groups == 0 then
    results[#results + 1] = "No similar card names found with current threshold"
    results[#results + 1] = "Try lowering the threshold (e.g., 0.6 or 0.7)"
end

return results 