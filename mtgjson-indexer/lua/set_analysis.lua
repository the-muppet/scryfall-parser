-- MTGJSON Set Analysis Script
-- Analyzes sets in the database with various metrics
-- Arguments: [set_code] (optional - if not provided, analyzes all sets)

local target_set = ARGV[1]
local results = {}

if target_set then
    -- Analyze specific set
    local set_data = redis.call('GET', 'set:' .. target_set)
    if not set_data then
        return {"Error: Set not found: " .. target_set}
    end
    
    local set_info = cjson.decode(set_data)
    local cards = redis.call('SMEMBERS', 'set:' .. target_set .. ':cards')
    
    -- Initialize counters
    local rarity_counts = {common = 0, uncommon = 0, rare = 0, mythic = 0, special = 0}
    local color_counts = {W = 0, U = 0, B = 0, R = 0, G = 0, C = 0}
    local type_counts = {}
    local mana_value_counts = {}
    local total_cards = 0
    local reserved_count = 0
    local promo_count = 0
    local foil_count = 0
    
    -- Analyze each card
    for _, uuid in ipairs(cards) do
        local card_data = redis.call('GET', 'card:' .. uuid)
        if card_data then
            local card = cjson.decode(card_data)
            total_cards = total_cards + 1
            
            -- Count rarities
            local rarity = card.rarity:lower()
            if rarity_counts[rarity] then
                rarity_counts[rarity] = rarity_counts[rarity] + 1
            else
                rarity_counts.special = rarity_counts.special + 1
            end
            
            -- Count colors
            if #card.colors == 0 then
                color_counts.C = color_counts.C + 1
            else
                for _, color in ipairs(card.colors) do
                    if color_counts[color] then
                        color_counts[color] = color_counts[color] + 1
                    end
                end
            end
            
            -- Count types
            for _, card_type in ipairs(card.types) do
                type_counts[card_type] = (type_counts[card_type] or 0) + 1
            end
            
            -- Count mana values
            local mv = tostring(math.floor(card.mana_value))
            mana_value_counts[mv] = (mana_value_counts[mv] or 0) + 1
            
            -- Count special properties
            if card.is_reserved then reserved_count = reserved_count + 1 end
            if card.is_promo then promo_count = promo_count + 1 end
            if card.has_foil then foil_count = foil_count + 1 end
        end
    end
    
    -- Build result
    table.insert(results, {
        set_code = set_info.code,
        set_name = set_info.name,
        release_date = set_info.release_date,
        set_type = set_info.set_type,
        total_cards = total_cards,
        base_set_size = set_info.base_set_size,
        rarity_breakdown = rarity_counts,
        color_breakdown = color_counts,
        type_breakdown = type_counts,
        mana_value_breakdown = mana_value_counts,
        reserved_cards = reserved_count,
        promo_cards = promo_count,
        foil_available = foil_count
    })
    
else
    -- Analyze all sets
    local all_set_keys = redis.call('KEYS', 'set:*')
    local set_summaries = {}
    
    for _, set_key in ipairs(all_set_keys) do
        if not set_key:match(':cards$') then  -- Skip the :cards keys
            local set_code = set_key:match('set:(.+)')
            local set_data = redis.call('GET', set_key)
            
            if set_data then
                local set_info = cjson.decode(set_data)
                local cards = redis.call('SMEMBERS', 'set:' .. set_code .. ':cards')
                
                -- Quick analysis
                local rare_count = 0
                local mythic_count = 0
                local total_value = 0
                local high_value_cards = 0
                
                for _, uuid in ipairs(cards) do
                    local card_data = redis.call('GET', 'card:' .. uuid)
                    if card_data then
                        local card = cjson.decode(card_data)
                        
                        if card.rarity == "rare" then
                            rare_count = rare_count + 1
                        elseif card.rarity == "mythic" then
                            mythic_count = mythic_count + 1
                        end
                        
                        -- Check for TCGPlayer pricing data
                        if #card.tcgplayer_skus > 0 then
                            high_value_cards = high_value_cards + 1
                        end
                    end
                end
                
                table.insert(set_summaries, {
                    set_code = set_info.code,
                    set_name = set_info.name,
                    release_date = set_info.release_date,
                    set_type = set_info.set_type,
                    total_cards = #cards,
                    base_set_size = set_info.base_set_size,
                    rare_count = rare_count,
                    mythic_count = mythic_count,
                    cards_with_tcg_data = high_value_cards
                })
            end
        end
    end
    
    -- Sort by release date (newest first)
    table.sort(set_summaries, function(a, b) 
        return a.release_date > b.release_date 
    end)
    
    results = set_summaries
end

return results 