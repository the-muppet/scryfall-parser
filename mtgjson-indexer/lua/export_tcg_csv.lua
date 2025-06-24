-- Export Deck Data to TCGPlayer CSV Format
-- Usage: redis-cli --eval export_tcg_csv.lua , [deck_uuid] [output_type]
-- output_type: "all" (default), "single", "by_type", "by_set"

-- Function to escape CSV values
local function escape_csv(value)
    if value == nil then return "" end
    local str = tostring(value)
    if string.find(str, '[",\n\r]') then
        str = string.gsub(str, '"', '""')
        return '"' .. str .. '"'
    end
    return str
end

-- Function to format CSV row
local function format_csv_row(fields)
    local row = {}
    for i, field in ipairs(fields) do
        row[i] = escape_csv(field)
    end
    return table.concat(row, ",")
end

-- Function to get TCG pricing data
local function get_tcg_pricing(product_id, condition)
    if not product_id or product_id == "" then
        return {
            market_price = "",
            direct_low = "",
            low_with_shipping = "",
            low_price = "",
            marketplace_price = ""
        }
    end
    
    local pricing_key = "tcg:" .. product_id .. "|" .. condition
    local pricing_data = redis.call('GET', pricing_key)
    
    if pricing_data then
        local pricing = cjson.decode(pricing_data)
        return {
            market_price = pricing.market_price or "",
            direct_low = pricing.direct_low or "",
            low_with_shipping = pricing.low_with_shipping or "",
            low_price = pricing.low_price or "",
            marketplace_price = pricing.marketplace_price or ""
        }
    end
    
    return {
        market_price = "",
        direct_low = "",
        low_with_shipping = "",
        low_price = "",
        marketplace_price = ""
    }
end

-- Function to get set name
local function get_set_name(set_code)
    local set_data = redis.call('GET', 'set:' .. set_code)
    if set_data then
        local set_info = cjson.decode(set_data)
        return set_info.name or set_code
    end
    return set_code
end

-- Function to get card details
local function get_card_details(uuid)
    local card_data = redis.call('GET', 'card:' .. uuid)
    if card_data then
        return cjson.decode(card_data)
    end
    return {}
end

-- Function to convert deck to CSV rows
local function deck_to_csv_rows(deck)
    local rows = {}
    
    -- CSV header (exact TCGPlayer format)
    local header = {
        "TCGplayer Id", "Product Line", "Set Name", "Product Name", "Title",
        "Number", "Rarity", "Condition", "TCG Market Price", "TCG Direct Low",
        "TCG Low Price With Shipping", "TCG Low Price", "TCG Marketplace Price",
        "Total Quantity", "Add to Quantity", "Photo URL"
    }
    table.insert(rows, format_csv_row(header))
    
    -- Process all card sections
    local function process_cards(cards, section_name)
        if not cards then return end
        
        for _, card in ipairs(cards) do
            local card_details = get_card_details(card.uuid)
            local tcg_pricing = get_tcg_pricing(card.tcgplayer_product_id, "Near Mint")
            local set_name = get_set_name(card.set_code)
            
            local row_data = {
                card.tcgplayer_product_id or "",     -- TCGplayer Id
                "Magic",                             -- Product Line
                set_name,                            -- Set Name
                card.name,                           -- Product Name
                card.name .. " [" .. card.set_code .. "]", -- Title
                card_details.number or "",           -- Number
                card_details.rarity or "",           -- Rarity
                "Near Mint",                         -- Condition
                tcg_pricing.market_price,            -- TCG Market Price
                tcg_pricing.direct_low,              -- TCG Direct Low
                tcg_pricing.low_with_shipping,       -- TCG Low Price With Shipping
                tcg_pricing.low_price,               -- TCG Low Price
                tostring(card.count),                -- Total Quantity
                tostring(card.count),                -- Add to Quantity
                tcg_pricing.marketplace_price,       -- TCG Marketplace Price
                "Photo URL"                          -- Photo URL (always empty but required)
            }
            
            table.insert(rows, format_csv_row(row_data))
        end
    end
    
    -- Process each section
    process_cards(deck.commanders, "Commander")
    process_cards(deck.main_board, "Main Board")
    process_cards(deck.side_board, "Side Board")
    
    return rows
end

-- Function to export single deck
local function export_single_deck(deck_uuid)
    local deck_data = redis.call('GET', 'deck:' .. deck_uuid)
    if not deck_data then
        return "ERROR: Deck " .. deck_uuid .. " not found"
    end
    
    local deck = cjson.decode(deck_data)
    local csv_rows = deck_to_csv_rows(deck)
    
    local result = {
        deck_name = deck.name,
        deck_type = deck.deck_type,
        total_cards = #csv_rows - 1, -- Subtract header
        csv_data = table.concat(csv_rows, "\n")
    }
    
    return cjson.encode(result)
end

-- Function to export all decks grouped by type
local function export_by_deck_type()
    local deck_keys = redis.call('KEYS', 'deck:deck_*')
    local deck_types = {}
    
    for _, key in ipairs(deck_keys) do
        local deck_data = redis.call('GET', key)
        if deck_data then
            local deck = cjson.decode(deck_data)
            local deck_type = deck.deck_type or "Unknown"
            
            if not deck_types[deck_type] then
                deck_types[deck_type] = {}
            end
            
            local csv_rows = deck_to_csv_rows(deck)
            table.insert(deck_types[deck_type], {
                deck_uuid = string.match(key, "deck:(.+)"),
                deck_name = deck.name,
                set_code = deck.code,
                csv_data = table.concat(csv_rows, "\n")
            })
        end
    end
    
    return cjson.encode(deck_types)
end

-- Function to export all decks
local function export_all_decks()
    local deck_keys = redis.call('KEYS', 'deck:deck_*')
    local all_csv_rows = {}
    local deck_count = 0
    
    -- Add header once (exact TCGPlayer format)
    local header = {
        "TCGplayer Id", "Product Line", "Set Name", "Product Name", "Title",
        "Number", "Rarity", "Condition", "TCG Market Price", "TCG Direct Low",
        "TCG Low Price With Shipping", "TCG Low Price", "TCG Marketplace Price",
        "Total Quantity", "Add to Quantity", "Photo URL"
    }
    table.insert(all_csv_rows, format_csv_row(header))
    
    for _, key in ipairs(deck_keys) do
        local deck_data = redis.call('GET', key)
        if deck_data then
            local deck = cjson.decode(deck_data)
            local csv_rows = deck_to_csv_rows(deck)
            
            -- Add all rows except header (skip first row)
            for i = 2, #csv_rows do
                table.insert(all_csv_rows, csv_rows[i])
            end
            
            deck_count = deck_count + 1
        end
    end
    
    local result = {
        total_decks = deck_count,
        total_cards = #all_csv_rows - 1, -- Subtract header
        csv_data = table.concat(all_csv_rows, "\n")
    }
    
    return cjson.encode(result)
end

-- Function to get deck statistics
local function get_export_stats()
    local deck_keys = redis.call('KEYS', 'deck:deck_*')
    local stats = {
        total_decks = #deck_keys,
        deck_types = {},
        set_codes = {},
        total_cards = 0
    }
    
    for _, key in ipairs(deck_keys) do
        local deck_data = redis.call('GET', key)
        if deck_data then
            local deck = cjson.decode(deck_data)
            
            -- Count by deck type
            local deck_type = deck.deck_type or "Unknown"
            stats.deck_types[deck_type] = (stats.deck_types[deck_type] or 0) + 1
            
            -- Count by set
            local set_code = deck.code or "UNK"
            stats.set_codes[set_code] = (stats.set_codes[set_code] or 0) + 1
            
            -- Count total cards
            local function count_cards(card_list)
                if not card_list then return 0 end
                local total = 0
                for _, card in ipairs(card_list) do
                    total = total + (card.count or 0)
                end
                return total
            end
            
            stats.total_cards = stats.total_cards + 
                count_cards(deck.commanders) +
                count_cards(deck.main_board) +
                count_cards(deck.side_board)
        end
    end
    
    return cjson.encode(stats)
end

-- Main execution
local deck_uuid = ARGV[1]
local output_type = ARGV[2] or "all"

if output_type == "stats" then
    return get_export_stats()
elseif output_type == "single" and deck_uuid then
    return export_single_deck(deck_uuid)
elseif output_type == "by_type" then
    return export_by_deck_type()
else
    return export_all_decks()
end 