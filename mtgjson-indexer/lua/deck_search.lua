-- Deck Search Script for MTGJSON Indexer
-- This script demonstrates various deck search and analysis queries

local function search_decks_by_name(deck_name)
    local name_key = "deck:name:" .. string.lower(deck_name)
    local deck_uuids = redis.call('SMEMBERS', name_key)
    
    local results = {}
    for _, uuid in ipairs(deck_uuids) do
        local deck_json = redis.call('GET', 'deck:' .. uuid)
        if deck_json then
            local deck = cjson.decode(deck_json)
            table.insert(results, deck)
        end
    end
    
    return results
end

local function get_commander_decks()
    local commander_deck_uuids = redis.call('SMEMBERS', 'deck:commander:true')
    
    local results = {}
    for _, uuid in ipairs(commander_deck_uuids) do
        local deck_json = redis.call('GET', 'deck:' .. uuid)
        if deck_json then
            local deck = cjson.decode(deck_json)
            table.insert(results, {
                name = deck.name,
                code = deck.code,
                release_date = deck.release_date,
                commanders = deck.commanders,
                estimated_value = deck.estimated_value
            })
        end
    end
    
    return results
end

local function find_decks_containing_card(card_name)
    -- First find the card UUID by name
    local card_name_key = "name:" .. string.lower(card_name)
    local card_uuids = redis.call('SMEMBERS', card_name_key)
    
    local all_deck_uuids = {}
    for _, card_uuid in ipairs(card_uuids) do
        local deck_uuids = redis.call('SMEMBERS', 'card:' .. card_uuid .. ':decks')
        for _, deck_uuid in ipairs(deck_uuids) do
            all_deck_uuids[deck_uuid] = true
        end
    end
    
    local results = {}
    for deck_uuid, _ in pairs(all_deck_uuids) do
        local deck_json = redis.call('GET', 'deck:' .. deck_uuid)
        if deck_json then
            local deck = cjson.decode(deck_json)
            -- Get card quantity in this deck
            local quantity = redis.call('ZSCORE', 'deck:' .. deck_uuid .. ':cards', card_uuids[1])
            table.insert(results, {
                deck_name = deck.name,
                deck_type = deck.deck_type,
                quantity = quantity or 0,
                estimated_value = deck.estimated_value
            })
        end
    end
    
    return results
end

local function get_deck_statistics()
    local total_decks = #redis.call('KEYS', 'deck:deck_*')
    local commander_decks = #redis.call('SMEMBERS', 'deck:commander:true')
    local constructed_decks = #redis.call('SMEMBERS', 'deck:commander:false')
    
    -- Get deck types
    local deck_types = {}
    local type_keys = redis.call('KEYS', 'deck:type:*')
    for _, key in ipairs(type_keys) do
        local deck_type = string.gsub(key, 'deck:type:', '')
        local count = #redis.call('SMEMBERS', key)
        deck_types[deck_type] = count
    end
    
    -- Get value distribution
    local value_ranges = {}
    local value_keys = redis.call('KEYS', 'deck:value_low:*')
    for _, key in ipairs(value_keys) do
        local range = string.gsub(key, 'deck:value_low:', '')
        local count = #redis.call('SMEMBERS', key)
        value_ranges[range] = count
    end
    
    return {
        total_decks = total_decks,
        commander_decks = commander_decks,
        constructed_decks = constructed_decks,
        deck_types = deck_types,
        value_ranges = value_ranges
    }
end

local function get_deck_composition(deck_uuid)
    local deck_json = redis.call('GET', 'deck:' .. deck_uuid)
    if not deck_json then
        return nil
    end
    
    local deck = cjson.decode(deck_json)
    
    -- Get all cards with quantities
    local card_data = redis.call('ZRANGE', 'deck:' .. deck_uuid .. ':cards', 0, -1, 'WITHSCORES')
    local cards = {}
    
    for i = 1, #card_data, 2 do
        local card_uuid = card_data[i]
        local quantity = tonumber(card_data[i + 1])
        
        local card_json = redis.call('GET', 'card:' .. card_uuid)
        if card_json then
            local card = cjson.decode(card_json)
            table.insert(cards, {
                name = card.name,
                set_code = card.set_code,
                quantity = quantity,
                uuid = card_uuid
            })
        end
    end
    
    return {
        deck_info = deck,
        cards = cards
    }
end

local function find_expensive_decks(min_value)
    min_value = min_value or 100
    
    local expensive_ranges = {}
    if min_value <= 100 then
        table.insert(expensive_ranges, 'deck:value_low:100_to_200')
    end
    if min_value <= 200 then
        table.insert(expensive_ranges, 'deck:value_low:200_to_500')
    end
    if min_value <= 500 then
        table.insert(expensive_ranges, 'deck:value_low:over_500')
    end
    
    local deck_uuids = {}
    for _, range_key in ipairs(expensive_ranges) do
        local range_decks = redis.call('SMEMBERS', range_key)
        for _, uuid in ipairs(range_decks) do
            deck_uuids[uuid] = true
        end
    end
    
    local results = {}
    for deck_uuid, _ in pairs(deck_uuids) do
        local deck_json = redis.call('GET', 'deck:' .. deck_uuid)
        if deck_json then
            local deck = cjson.decode(deck_json)
            if deck.estimated_value and deck.estimated_value.market_total >= min_value then
                table.insert(results, {
                    name = deck.name,
                    deck_type = deck.deck_type,
                    release_date = deck.release_date,
                    estimated_value = deck.estimated_value
                })
            end
        end
    end
    
    -- Sort by value (highest first)
    table.sort(results, function(a, b)
        local val_a = a.estimated_value and a.estimated_value.market_total or 0
        local val_b = b.estimated_value and b.estimated_value.market_total or 0
        return val_a > val_b
    end)
    
    return results
end

-- Main execution based on arguments
local command = ARGV[1]

if command == "search_name" then
    local deck_name = ARGV[2]
    return search_decks_by_name(deck_name)
    
elseif command == "commander_decks" then
    return get_commander_decks()
    
elseif command == "contains_card" then
    local card_name = ARGV[2]
    return find_decks_containing_card(card_name)
    
elseif command == "statistics" then
    return get_deck_statistics()
    
elseif command == "composition" then
    local deck_uuid = ARGV[2]
    return get_deck_composition(deck_uuid)
    
elseif command == "expensive" then
    local min_value = tonumber(ARGV[2]) or 100
    return find_expensive_decks(min_value)
    
else
    return {
        error = "Unknown command. Available commands:",
        commands = {
            "search_name <deck_name>",
            "commander_decks",
            "contains_card <card_name>", 
            "statistics",
            "composition <deck_uuid>",
            "expensive <min_value>"
        }
    }
end 