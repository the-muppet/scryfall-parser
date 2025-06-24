-- Script to create oracle_id -> sets mapping from existing data
local cursor = "0"
local oracle_keys_pattern = "card:oracle:*"
local count = 0

repeat
    local result = redis.call("SCAN", cursor, "MATCH", oracle_keys_pattern, "COUNT", 1000)
    cursor = result[1]
    local keys = result[2]
    
    for i, key in ipairs(keys) do
        local oracle_id = string.sub(key, 13)
        local card_data = redis.call("GET", key)
        
        local sets_start = string.find(card_data, '"sets":%[')
        if sets_start then
            local bracket_count = 1
            local sets_end = sets_start + 8
            while bracket_count > 0 and sets_end <= #card_data do
                local char = string.sub(card_data, sets_end, sets_end)
                if char == "[" then
                    bracket_count = bracket_count + 1
                elseif char == "]" then
                    bracket_count = bracket_count - 1
                end
                sets_end = sets_end + 1
            end
            
            local sets_json = string.sub(card_data, sets_start + 7, sets_end - 1)
            
            redis.call("SET", "oracle:sets:" .. oracle_id, sets_json)
            count = count + 1
        end
    end
until cursor == "0"

return count