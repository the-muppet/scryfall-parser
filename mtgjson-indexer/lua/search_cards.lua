-- MTGJSON Card Search Script
-- Searches for cards in the MTGJSON-indexed Redis database
-- Arguments: query, max_results, [filter_key, filter_value, ...]

local query = ARGV[1] or ""
local max_results = tonumber(ARGV[2]) or 50
local filters = {}

-- Parse filter arguments (key-value pairs)
for i = 3, #ARGV, 2 do
    if ARGV[i] and ARGV[i+1] then
        filters[ARGV[i]] = ARGV[i+1]
    end
end

local results = {}

-- Function to search by exact name match
local function search_exact_name(search_term)
    local candidates = {}
    local exact_key = "name:" .. search_term:lower()
    local exact_matches = redis.call('SMEMBERS', exact_key)
    
    for _, uuid in ipairs(exact_matches) do
        table.insert(candidates, {uuid = uuid, score = 100})
    end
    
    return candidates
end

-- Function to search by word matching
local function search_by_words(search_term)
    local candidates = {}
    local words = {}
    
    -- Split search term into words
    for word in search_term:lower():gmatch('%S+') do
        if #word >= 2 then
            table.insert(words, word)
        end
    end
    
    if #words == 0 then return candidates end
    
    -- Find cards containing all words
    local word_matches = {}
    for _, word in ipairs(words) do
        local word_key = "word:" .. word
        local matches = redis.call('SMEMBERS', word_key)
        
        if #word_matches == 0 then
            -- First word - initialize with all its matches
            for _, uuid in ipairs(matches) do
                word_matches[uuid] = 1
            end
        else
            -- Subsequent words - only keep UUIDs that match all previous words
            local new_matches = {}
            for _, uuid in ipairs(matches) do
                if word_matches[uuid] then
                    new_matches[uuid] = word_matches[uuid] + 1
                end
            end
            word_matches = new_matches
        end
    end
    
    -- Convert to candidates with scores
    for uuid, word_count in pairs(word_matches) do
        if word_count == #words then  -- Must match ALL words
            table.insert(candidates, {uuid = uuid, score = 80})
        end
    end
    
    return candidates
end

-- Function to search by prefix matching
local function search_by_prefix(search_term)
    local candidates = {}
    local prefix_key = "auto:prefix:" .. search_term:lower()
    local prefix_matches = redis.call('SMEMBERS', prefix_key)
    
    for _, uuid in ipairs(prefix_matches) do
        table.insert(candidates, {uuid = uuid, score = 60})
    end
    
    return candidates
end

-- Function to search by n-grams (fuzzy)
local function search_by_ngrams(search_term)
    local candidates = {}
    local ngram_counts = {}
    
    if #search_term < 3 then return candidates end
    
    -- Generate n-grams from search term
    local ngrams = {}
    for i = 1, #search_term - 2 do
        local ngram = search_term:sub(i, i + 2):lower()
        table.insert(ngrams, ngram)
    end
    
    -- Find cards containing these n-grams
    for _, ngram in ipairs(ngrams) do
        local ngram_key = "ngram:" .. ngram
        local matches = redis.call('SMEMBERS', ngram_key)
        
        for _, uuid in ipairs(matches) do
            ngram_counts[uuid] = (ngram_counts[uuid] or 0) + 1
        end
    end
    
    -- Score based on n-gram overlap
    local min_score = math.max(1, math.floor(#ngrams * 0.3))  -- At least 30% overlap
    for uuid, count in pairs(ngram_counts) do
        if count >= min_score then
            local score = math.min(40, math.floor((count / #ngrams) * 40))
            table.insert(candidates, {uuid = uuid, score = score})
        end
    end
    
    return candidates
end

-- Function to apply filters to a card
local function apply_filters(card)
    -- Set filter
    if filters.set and card.set_code ~= filters.set then
        return false
    end
    
    -- Color filter
    if filters.color then
        local has_color = false
        for _, color in ipairs(card.colors or {}) do
            if color == filters.color then
                has_color = true
                break
            end
        end
        if not has_color then return false end
    end
    
    -- Color identity filter
    if filters.color_identity then
        local target_identity = {}
        for c in filters.color_identity:gmatch('.') do
            target_identity[c] = true
        end
        
        for _, color in ipairs(card.color_identity or {}) do
            if not target_identity[color] then
                return false
            end
        end
    end
    
    -- Type filter
    if filters.type then
        local has_type = false
        for _, card_type in ipairs(card.types or {}) do
            if card_type:lower() == filters.type:lower() then
                has_type = true
                break
            end
        end
        if not has_type then return false end
    end
    
    -- Subtype filter
    if filters.subtype then
        local has_subtype = false
        for _, subtype in ipairs(card.subtypes or {}) do
            if subtype:lower() == filters.subtype:lower() then
                has_subtype = true
                break
            end
        end
        if not has_subtype then return false end
    end
    
    -- Rarity filter
    if filters.rarity and card.rarity ~= filters.rarity then
        return false
    end
    
    -- Mana value filters
    if filters.mana_value then
        local target_mv = tonumber(filters.mana_value)
        if not target_mv or card.mana_value ~= target_mv then
            return false
        end
    end
    
    if filters.min_mana_value then
        local min_mv = tonumber(filters.min_mana_value)
        if min_mv and card.mana_value < min_mv then
            return false
        end
    end
    
    if filters.max_mana_value then
        local max_mv = tonumber(filters.max_mana_value)
        if max_mv and card.mana_value > max_mv then
            return false
        end
    end
    
    -- Reserved list filter
    if filters.is_reserved and filters.is_reserved == "true" then
        if not card.is_reserved then return false end
    end
    
    -- Promo filter
    if filters.is_promo and filters.is_promo == "true" then
        if not card.is_promo then return false end
    end
    
    -- Format legality filter (disabled - indexes not created by main.rs)
    -- TODO: Add format legality indexes to main.rs if this functionality is needed
    -- if filters.format then
    --     local format_key = "legal:" .. filters.format:lower()
    --     local legal_cards = redis.call('SMEMBERS', format_key)
    --     local is_legal = false
    --     for _, uuid in ipairs(legal_cards) do
    --         if uuid == card.uuid then
    --             is_legal = true
    --             break
    --         end
    --     end
    --     if not is_legal then return false end
    -- end
    
    return true
end

-- Main search logic
if query == "" then
    return {"Error: Empty search query"}
end

local all_candidates = {}

-- Try different search strategies in order of precision
local search_strategies = {
    search_exact_name,
    search_by_words,
    search_by_prefix,
    search_by_ngrams
}

for _, search_func in ipairs(search_strategies) do
    local candidates = search_func(query)
    for _, candidate in ipairs(candidates) do
        table.insert(all_candidates, candidate)
    end
    
    -- If we found exact matches, prioritize them
    if #candidates > 0 and candidates[1].score >= 80 then
        break
    end
end

-- Remove duplicates and sort by score
local seen = {}
local unique_candidates = {}
for _, candidate in ipairs(all_candidates) do
    if not seen[candidate.uuid] then
        seen[candidate.uuid] = true
        table.insert(unique_candidates, candidate)
    end
end

-- Sort by score (descending)
table.sort(unique_candidates, function(a, b) return a.score > b.score end)

-- Process candidates and apply filters
for _, candidate in ipairs(unique_candidates) do
    if #results >= max_results then break end
    
    local card_data = redis.call('GET', 'card:' .. candidate.uuid)
    if card_data then
        local card = cjson.decode(card_data)
        
        if apply_filters(card) then
            -- Build result entry as JSON string
            local result_entry = cjson.encode({
                uuid = candidate.uuid,
                name = card.name,
                set_code = card.set_code,
                set_name = card.set_name,
                collector_number = card.collector_number,
                rarity = card.rarity,
                mana_cost = card.mana_cost or "",
                mana_value = card.mana_value,
                types = table.concat(card.types or {}, " "),
                subtypes = table.concat(card.subtypes or {}, " "),
                colors = table.concat(card.colors or {}, ""),
                color_identity = table.concat(card.color_identity or {}, ""),
                power = card.power or "",
                toughness = card.toughness or "",
                text = card.text or "",
                release_date = card.release_date,
                is_reserved = card.is_reserved,
                is_promo = card.is_promo,
                tcgplayer_product_id = card.tcgplayer_product_id or "",
                score = candidate.score
            })
            
            table.insert(results, result_entry)
        end
    end
end

return results 