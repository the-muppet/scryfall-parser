# MTGJSON Index Pattern Analysis & Recommendations

## Executive Summary

After analyzing the index building in `main.rs`, key patterns used in Lua scripts, implementations in `redis_client.rs`, and API exposure in `api_server.rs`, I've identified several critical inconsistencies and missing functionality that need to be addressed for the system to work correctly.

## Key Pattern Analysis

### 1. Index Patterns Built in main.rs

#### Core Card Storage
- ✅ `card:{uuid}` - Main card data storage
- ✅ `set:{set_code}` - Set metadata
- ✅ `set:{set_code}:cards` - Cards in a set

#### Search Indexes
- ✅ `name:{lowercase_name}` - Exact name matches
- ✅ `word:{word}` - Word-based search
- ✅ `auto:prefix:{prefix}` - Autocomplete prefixes  
- ✅ `ngram:{ngram}` - N-gram fuzzy matching
- ✅ `metaphone:{metaphone}` - Phonetic matching

#### SKU & Pricing (Advanced Implementation)
- ✅ `oracle:{oracle_id}` - Oracle ID mapping
- ✅ `tcgplayer:{product_id}` - TCGPlayer product mapping
- ✅ `sku:{sku_id}` - SKU to card mapping
- ✅ `sku:{sku_id}:meta` - SKU metadata
- ✅ `sku:{sku_id}:card` - SKU to card reverse mapping
- ✅ `card:{uuid}:skus` - Card to SKUs mapping
- ✅ `price:sku:{sku_id}:latest` - Latest SKU pricing
- ✅ `price:sku:{sku_id}:history` - SKU price history (ZSET)
- ✅ `price:{uuid}:{condition}` - Legacy pricing pattern
- ✅ `price:range:{bucket}` - Price range indexes

#### Deck Storage
- ✅ `deck:{uuid}` - Main deck data
- ✅ `deck:meta:{uuid}` - Deck metadata
- ✅ `deck:slug:{slug}` - Deck slug mapping
- ✅ `deck:name:{name}` - Deck name index
- ✅ `deck:name_word:{word}` - Deck name word index
- ✅ `deck:type:{type}` - Deck type index
- ✅ `deck:set:{code}` - Deck set index
- ✅ `deck:release:{date}` - Deck release date index
- ✅ `deck:year:{year}` - Deck year index
- ✅ `deck:commander:{true/false}` - Commander deck index
- ✅ `deck:{uuid}:cards` - Deck composition (ZSET)
- ✅ `deck:slug:{slug}:cards` - Deck composition by slug
- ✅ `card:{uuid}:decks` - Card to decks mapping
- ✅ `card:{uuid}:deck_info` - Card deck info (HSET)
- ✅ `deck:{uuid}:commanders` - Deck commanders
- ✅ `commander:{uuid}:decks` - Commander to decks mapping
- ✅ `deck:value_market:{bucket}` - Deck value buckets
- ✅ `deck:sorted_by_market_value` - Sorted deck values

### 2. Lua Script Expectations vs Reality

#### search_cards.lua Issues
❌ **Critical Issue**: Uses `prefix:{term}` but main.rs creates `auto:prefix:{term}`
❌ **Missing**: No RedisSearch integration despite create_redis_indexes.lua expecting it
❌ **Inconsistent**: Uses legacy pattern scanning instead of proper indexes
❌ **Missing**: No format legality indexes (`legal:{format}`) created by main.rs

#### deck_search.lua Issues  
✅ **Good**: Correctly uses most deck patterns from main.rs
❌ **Minor**: Some return type inconsistencies

#### Pricing Script Issues
❌ **Critical**: Many scripts scan with UUID patterns instead of using proper indexes
❌ **Inconsistent**: Some expect `tcg:product:*` patterns that don't exist
❌ **Missing**: SKU-based analysis not fully integrated

#### create_redis_indexes.lua Issues
❌ **Major**: Creates Redis Search indexes that main.rs doesn't utilize
❌ **Missing**: No integration with the fuzzy search script from main.rs
❌ **Structural**: Expects JSON path queries but main.rs stores flat JSON

### 3. redis_client.rs Implementation Gaps

#### Search Methods
❌ **Critical**: `autocomplete_card_names()` correctly uses `auto:prefix:` pattern
❌ **Missing**: No fuzzy search integration using the script from main.rs  
❌ **Limited**: Search relies entirely on Lua scripts instead of using built indexes
❌ **Missing**: No Redis Search integration despite create_redis_indexes.lua

#### Deck Operations
✅ **Good**: Most deck operations work correctly
❌ **Bug**: `get_deck_by_uuid()` has incorrect key format (`deck:meta:deck_{uuid}` should be `deck:meta:{uuid}`)

#### Pricing Operations  
✅ **Good**: SKU pricing methods are well implemented
❌ **Limited**: No integration with price range indexes from main.rs

### 4. api_server.rs API Exposure

#### Card Endpoints
✅ **Good**: Basic card retrieval works
❌ **Limited**: Search functionality depends on broken Lua scripts
❌ **Missing**: No autocomplete endpoint exposure
❌ **Missing**: No fuzzy search capabilities

#### Deck Endpoints
✅ **Good**: Basic deck operations work
❌ **Limited**: Search depends on deck_search.lua patterns

#### Pricing Endpoints
✅ **Good**: Basic pricing works
❌ **Limited**: Advanced pricing analysis limited by script issues

## Critical Issues Requiring Immediate Attention

### 1. Search Pattern Mismatch
**Problem**: `search_cards.lua` uses `prefix:` but main.rs creates `auto:prefix:`
**Impact**: Autocomplete search completely broken
**Fix**: Update search_cards.lua line 77 to use `auto:prefix:`

### 2. Redis Search Integration Missing
**Problem**: `create_redis_indexes.lua` creates FT indexes but nothing uses them
**Impact**: Advanced search capabilities unused, poor performance
**Fix**: Integrate Redis Search in redis_client.rs or remove the script

### 3. Deck Key Format Bug
**Problem**: `get_deck_by_uuid()` uses wrong meta key format
**Impact**: Deck metadata lookups fail
**Fix**: Remove `deck_` prefix from meta key construction

### 4. Format Legality Indexes Missing
**Problem**: search_cards.lua expects `legal:{format}` indexes not created by main.rs
**Impact**: Format filtering completely broken
**Fix**: Add format legality indexes to main.rs or remove from search_cards.lua

### 5. SKU Pattern Inconsistencies
**Problem**: Various scripts expect different SKU/pricing patterns
**Impact**: Pricing analysis scripts may fail
**Fix**: Standardize on the patterns created by main.rs

## Recommendations

### Immediate Fixes (Critical)

1. **Fix search_cards.lua prefix pattern**:
   ```lua
   -- Line 77: Change from
   local prefix_key = "prefix:" .. search_term:lower()
   -- To
   local prefix_key = "auto:prefix:" .. search_term:lower()
   ```

2. **Fix deck metadata key format in redis_client.rs**:
   ```rust
   // Change from
   let meta_key = format!("deck:meta:deck_{}", uuid);
   // To  
   let meta_key = format!("deck:meta:{}", uuid);
   ```

3. **Add format legality indexes to main.rs** or remove from search_cards.lua

### Medium Priority Improvements

1. **Integrate Redis Search**: Either fully implement FT.SEARCH usage or remove create_redis_indexes.lua
2. **Standardize pricing patterns**: Ensure all Lua scripts use the patterns from main.rs
3. **Add fuzzy search endpoint**: Expose the fuzzy search script from main.rs via API
4. **Improve error handling**: Better error messages when patterns don't match

### Long-term Enhancements

1. **Performance optimization**: Use Redis Search for complex queries
2. **Unified search interface**: Single search endpoint supporting all query types
3. **Real-time indexing**: Incremental index updates instead of full rebuilds
4. **Search analytics**: Track query performance and popular searches

## Testing Strategy

### Unit Tests Needed
- Key pattern generation in main.rs
- Search functionality in redis_client.rs
- API endpoint responses in api_server.rs

### Integration Tests Needed
- End-to-end search workflow
- Deck composition and value calculations
- Pricing data consistency

### Performance Tests Needed
- Large dataset indexing performance
- Search response times
- Memory usage during indexing

## Conclusion

The MTGJSON indexing system has a solid foundation but suffers from critical inconsistencies between the indexing logic, Lua scripts, and API layer. The immediate fixes listed above will restore basic functionality, while the medium and long-term improvements will significantly enhance performance and user experience.

The most critical issue is the search pattern mismatch that breaks autocomplete functionality entirely. Once these core issues are resolved, the system should provide robust MTG card search and analysis capabilities.