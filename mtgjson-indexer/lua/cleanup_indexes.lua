-- Clean up and optimize search indexes
-- Removes empty index entries and orphaned references

local cleaned = {
    empty_ngrams = 0,
    empty_metaphones = 0,
    empty_words = 0,
    empty_prefixes = 0,
    orphaned_refs = 0
}

-- Function to check if a card UUID exists in MTGJSON structure
local function card_exists(uuid)
    local key = "card:" .. uuid
    return redis.call("EXISTS", key) == 1
end

-- Clean up n-gram indexes
local cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "ngram:*", "COUNT", 100)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        local members = redis.call("SMEMBERS", key)
        local valid_members = {}
        
        for _, member in ipairs(members) do
            if card_exists(member) then
                table.insert(valid_members, member)
            else
                cleaned.orphaned_refs = cleaned.orphaned_refs + 1
            end
        end
        
        if #valid_members == 0 then
            -- Remove empty index
            redis.call("DEL", key)
            cleaned.empty_ngrams = cleaned.empty_ngrams + 1
        elseif #valid_members < #members then
            -- Update with only valid members
            redis.call("DEL", key)
            if #valid_members > 0 then
                redis.call("SADD", key, unpack(valid_members))
            end
        end
    end
until cursor == "0"

-- Clean up metaphone indexes
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "metaphone:*", "COUNT", 100)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        local members = redis.call("SMEMBERS", key)
        local valid_members = {}
        
        for _, member in ipairs(members) do
            if card_exists(member) then
                table.insert(valid_members, member)
            else
                cleaned.orphaned_refs = cleaned.orphaned_refs + 1
            end
        end
        
        if #valid_members == 0 then
            redis.call("DEL", key)
            cleaned.empty_metaphones = cleaned.empty_metaphones + 1
        elseif #valid_members < #members then
            redis.call("DEL", key)
            if #valid_members > 0 then
                redis.call("SADD", key, unpack(valid_members))
            end
        end
    end
until cursor == "0"

-- Clean up word indexes
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "word:*", "COUNT", 100)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        local members = redis.call("SMEMBERS", key)
        local valid_members = {}
        
        for _, member in ipairs(members) do
            if card_exists(member) then
                table.insert(valid_members, member)
            else
                cleaned.orphaned_refs = cleaned.orphaned_refs + 1
            end
        end
        
        if #valid_members == 0 then
            redis.call("DEL", key)
            cleaned.empty_words = cleaned.empty_words + 1
        elseif #valid_members < #members then
            redis.call("DEL", key)
            if #valid_members > 0 then
                redis.call("SADD", key, unpack(valid_members))
            end
        end
    end
until cursor == "0"

-- Clean up prefix indexes
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "auto:prefix:*", "COUNT", 100)
    cursor = result[1]
    local keys = result[2]
    
    for _, key in ipairs(keys) do
        local members = redis.call("SMEMBERS", key)
        local valid_members = {}
        
        for _, member in ipairs(members) do
            if card_exists(member) then
                table.insert(valid_members, member)
            else
                cleaned.orphaned_refs = cleaned.orphaned_refs + 1
            end
        end
        
        if #valid_members == 0 then
            redis.call("DEL", key)
            cleaned.empty_prefixes = cleaned.empty_prefixes + 1
        elseif #valid_members < #members then
            redis.call("DEL", key)
            if #valid_members > 0 then
                redis.call("SADD", key, unpack(valid_members))
            end
        end
    end
until cursor == "0"

return {
    "=== INDEX CLEANUP RESULTS ===",
    "",
    "Empty N-gram indexes removed: " .. cleaned.empty_ngrams,
    "Empty Metaphone indexes removed: " .. cleaned.empty_metaphones,
    "Empty Word indexes removed: " .. cleaned.empty_words,
    "Empty Prefix indexes removed: " .. cleaned.empty_prefixes,
    "Orphaned references cleaned: " .. cleaned.orphaned_refs,
    "",
    "Cleanup completed successfully!"
} 