# Redis Key Management Schema
## MTGJSON Database Organization

**Version**: 1.0  
**Last Updated**: December 2024  
**Total Keys**: ~2.1M keys  

---

## 🏗️ Schema Overview

This document defines the comprehensive key management schema for the MTGJSON Redis database, organizing over 2 million keys into logical, performant, and maintainable patterns.

### Design Principles
1. **Hierarchical Organization** - Clear namespace separation
2. **Predictable Patterns** - Consistent naming conventions
3. **Performance Optimized** - Efficient for common queries
4. **Scalable Structure** - Handles growth gracefully
5. **Type Safety** - Clear data type expectations

---

## 📋 Core Namespace Structure

```
mtg:
├── cards/           # Card data and metadata
├── decks/           # Deck data and compositions  
├── sets/            # Set information and relationships
├── pricing/         # All pricing data (TCG, market, etc.)
├── search/          # Search indexes and autocomplete
├── meta/            # System metadata and statistics
├── cache/           # Temporary/cached data (with TTL)
└── temp/            # Temporary processing data
```

---

## 🃏 Card Namespace (`mtg:cards:`)

### Primary Data
```
mtg:cards:data:{uuid}                    # [STRING] Complete card JSON data
mtg:cards:oracle:{oracle_id}             # [STRING] Oracle card data (unique across printings)
mtg:cards:printing:{uuid}                # [STRING] Printing-specific data
```

### Relationships & Indexes
```
mtg:cards:oracle_to_printings:{oracle_id}  # [SET] All printing UUIDs for this oracle
mtg:cards:set_cards:{set_code}              # [SET] All card UUIDs in this set
mtg:cards:name_to_oracle:{normalized_name}  # [SET] Oracle IDs for cards with this name
```

### SKU & Product Data
```
mtg:cards:skus:{uuid}                    # [SET] TCGPlayer SKU IDs for this card
mtg:cards:tcg_product:{uuid}             # [STRING] Primary TCGPlayer Product ID
```

### Examples
```
mtg:cards:data:a1b2c3d4-...              # Complete Ancestral Recall data
mtg:cards:oracle:f8e7d6c5-...            # Oracle data (shared across printings)
mtg:cards:set_cards:LEA                  # All cards in Limited Edition Alpha
mtg:cards:name_to_oracle:ancestral_recall # Oracle IDs for "Ancestral Recall"
```

---

## 🎴 Deck Namespace (`mtg:decks:`)

### Primary Data
```
mtg:decks:data:{uuid}                    # [STRING] Complete deck JSON data
mtg:decks:meta:{uuid}                    # [STRING] Lightweight metadata (name, type, value, etc.)
```

### Compositions
```
mtg:decks:cards:{uuid}                   # [ZSET] Card UUIDs with quantities (score = quantity)
mtg:decks:commanders:{uuid}              # [SET] Commander card UUIDs
mtg:decks:mainboard:{uuid}               # [ZSET] Mainboard cards with quantities
mtg:decks:sideboard:{uuid}               # [ZSET] Sideboard cards with quantities
```

### Indexes & Classifications
```
mtg:decks:by_type:{deck_type}            # [SET] All deck UUIDs of this type
mtg:decks:by_set:{set_code}              # [SET] All deck UUIDs from this set
mtg:decks:by_format:{format}             # [SET] Legal decks for this format
mtg:decks:by_value:{range}               # [SET] Decks in value range (e.g., "100-500")
mtg:decks:containing_card:{card_uuid}    # [SET] Decks containing this card
```

### Search & Lookup
```
mtg:decks:name_to_uuid:{normalized_name} # [SET] Deck UUIDs with this name
mtg:decks:slug_to_uuid:{slug}            # [STRING] Deck UUID for this slug
```

### Examples
```
mtg:decks:data:deck_a1b2c3d4             # Complete "Atraxa, Voice of the Praetors" deck
mtg:decks:meta:deck_a1b2c3d4             # Quick metadata (name, type, value)
mtg:decks:cards:deck_a1b2c3d4            # All cards with quantities
mtg:decks:by_type:Commander_Deck         # All commander decks
mtg:decks:by_value:500-1000              # Decks valued $500-1000
```

---

## 🏷️ Set Namespace (`mtg:sets:`)

### Primary Data
```
mtg:sets:data:{set_code}                 # [STRING] Complete set JSON data
mtg:sets:meta:{set_code}                 # [STRING] Lightweight metadata
```

### Relationships
```
mtg:sets:cards:{set_code}                # [SET] All card UUIDs in this set
mtg:sets:decks:{set_code}                # [SET] All deck UUIDs from this set
mtg:sets:by_type:{set_type}              # [SET] All sets of this type
mtg:sets:by_year:{year}                  # [SET] All sets released in this year
```

### Statistics
```
mtg:sets:stats:card_count:{set_code}     # [STRING] Total cards in set
mtg:sets:stats:value:{set_code}          # [STRING] Total estimated value
```

---

## 💰 Pricing Namespace (`mtg:pricing:`)

### Current Pricing (Latest Values)
```
mtg:pricing:current:card:{uuid}:{condition}        # [STRING] Latest price JSON
mtg:pricing:current:sku:{sku_id}                   # [STRING] Latest SKU price JSON
mtg:pricing:current:product:{product_id}:{condition} # [STRING] Product-level pricing
```

### Historical Data
```
mtg:pricing:history:sku:{sku_id}                   # [ZSET] Price history (score = timestamp)
mtg:pricing:history:card:{uuid}:{condition}       # [ZSET] Card price history
```

### Market Analysis
```
mtg:pricing:trending:up                            # [ZSET] Cards trending up (score = % change)
mtg:pricing:trending:down                          # [ZSET] Cards trending down
mtg:pricing:expensive:{range}                      # [SET] Cards in price range
```

### TCGPlayer Integration
```
mtg:pricing:tcg:product_to_sku:{product_id}        # [SET] All SKUs for this product
mtg:pricing:tcg:sku_meta:{sku_id}                  # [STRING] SKU metadata (condition, foil, etc.)
mtg:pricing:tcg:last_update                        # [STRING] Last pricing update timestamp
```

### Examples
```
mtg:pricing:current:card:a1b2c3d4:Near_Mint      # Latest Near Mint price
mtg:pricing:history:sku:12345                     # Price history for SKU 12345
mtg:pricing:trending:up                           # Cards with rising prices
mtg:pricing:expensive:100-500                     # Cards worth $100-500
```

---

## 🔍 Search Namespace (`mtg:search:`)

### Text Search Indexes
```
mtg:search:ngrams:{ngram}                # [SET] Card UUIDs containing this n-gram
mtg:search:words:{word}                  # [SET] Card UUIDs containing this word
mtg:search:metaphone:{code}              # [SET] Card UUIDs with this metaphone code
```

### Autocomplete
```
mtg:search:prefixes:{prefix}             # [SET] Card names starting with prefix
mtg:search:suggestions:{partial}         # [ZSET] Suggestions ranked by popularity
```

### Attribute Indexes
```
mtg:search:by_type:{type}                # [SET] Cards of this type
mtg:search:by_color:{colors}             # [SET] Cards with these colors
mtg:search:by_mana_value:{mv}            # [SET] Cards with this mana value
mtg:search:by_rarity:{rarity}            # [SET] Cards of this rarity
mtg:search:by_artist:{artist}            # [SET] Cards by this artist
```

### Format Legality
```
mtg:search:legal:{format}                # [SET] Cards legal in this format
mtg:search:banned:{format}               # [SET] Cards banned in this format
mtg:search:restricted:{format}           # [SET] Cards restricted in this format
```

---

## 📊 Meta Namespace (`mtg:meta:`)

### System Statistics
```
mtg:meta:stats:cards:total               # [STRING] Total card count
mtg:meta:stats:cards:with_pricing        # [STRING] Cards with pricing data
mtg:meta:stats:decks:total               # [STRING] Total deck count
mtg:meta:stats:sets:total                # [STRING] Total set count
mtg:meta:stats:last_update               # [STRING] Last full database update
```

### Performance Metrics
```
mtg:meta:perf:query_cache                # [HASH] Cached query results
mtg:meta:perf:index_sizes                # [HASH] Search index sizes
mtg:meta:perf:hot_searches               # [ZSET] Popular searches (score = frequency)
```

### Data Quality
```
mtg:meta:quality:missing_images          # [SET] Cards missing images
mtg:meta:quality:missing_prices          # [SET] Cards missing pricing
mtg:meta:quality:orphaned_skus           # [SET] SKUs without card references
```

---

## ⚡ Cache Namespace (`mtg:cache:`)

### Query Results (TTL: 1 hour)
```
mtg:cache:search:{hash}                  # [STRING] Cached search results
mtg:cache:deck_analysis:{uuid}           # [STRING] Cached deck analysis
mtg:cache:price_summary:{uuid}           # [STRING] Cached pricing summary
```

### Expensive Computations (TTL: 6 hours)
```
mtg:cache:set_stats:{set_code}           # [STRING] Set statistics
mtg:cache:format_meta:{format}           # [STRING] Format metadata
mtg:cache:trending_analysis              # [STRING] Market trend analysis
```

---

## 🧹 Data Lifecycle Management

### TTL Policies
```
mtg:cache:*           # 1-6 hours depending on computation cost
mtg:temp:*            # 24 hours
mtg:pricing:current:* # 7 days (refreshed daily)
```

### Cleanup Strategies
```sql
-- Remove expired cache entries
SCAN 0 MATCH mtg:cache:* COUNT 1000

-- Clean orphaned references
SCAN 0 MATCH mtg:*:missing_* COUNT 1000

-- Archive old pricing data
SCAN 0 MATCH mtg:pricing:history:* COUNT 1000
```

---

## 🔧 Migration Strategy

### Phase 1: Namespace Mapping
```python
# Current -> New mappings
MIGRATIONS = {
    'card:{uuid}': 'mtg:cards:data:{uuid}',
    'card:oracle:{id}': 'mtg:cards:oracle:{id}',
    'deck:deck_{uuid}': 'mtg:decks:data:{uuid}',
    'deck:meta:deck_{uuid}': 'mtg:decks:meta:{uuid}',
    'price:sku:{sku}:latest': 'mtg:pricing:current:sku:{sku}',
    'set:{code}': 'mtg:sets:data:{code}',
    'name:{name}': 'mtg:search:words:{name}',
    # ... continue for all patterns
}
```

### Phase 2: Batch Migration Script
```python
def migrate_keys_batch(pattern_old, pattern_new, batch_size=1000):
    """Migrate keys from old pattern to new pattern"""
    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor, match=pattern_old, count=batch_size)
        
        if keys:
            pipe = redis.pipeline()
            for old_key in keys:
                new_key = transform_key(old_key, pattern_old, pattern_new)
                pipe.rename(old_key, new_key)
            pipe.execute()
        
        if cursor == 0:
            break
```

---

## 📈 Performance Considerations

### Query Optimization
- **Index Strategy**: Use sets for membership, zsets for ordering
- **Pagination**: SCAN with COUNT for large result sets
- **Caching**: Cache expensive computations in `mtg:cache:`
- **Sharding**: Consider cluster mode for >5M keys

### Memory Optimization
- **Compression**: Use hash encoding for small sets
- **Expiration**: Set TTL on temporary data
- **Monitoring**: Track memory usage per namespace

### Connection Pooling
```python
# Recommended Redis connection settings
REDIS_CONFIG = {
    'max_connections': 50,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
    'retry_on_timeout': True,
    'health_check_interval': 30
}
```

---

## 🛠️ Maintenance Tools

### Schema Validation
```python
def validate_schema():
    """Validate all keys follow schema patterns"""
    valid_patterns = [
        r'^mtg:cards:(data|oracle|printing):[a-f0-9-]{36}$',
        r'^mtg:decks:(data|meta):[a-f0-9-]{36}$',
        r'^mtg:sets:(data|meta):[A-Z0-9]+$',
        # ... add all patterns
    ]
```

### Key Analytics
```python
def analyze_namespace_usage():
    """Generate namespace usage reports"""
    namespaces = {}
    for namespace in ['cards', 'decks', 'sets', 'pricing', 'search', 'meta']:
        pattern = f'mtg:{namespace}:*'
        count = len(redis.keys(pattern))
        namespaces[namespace] = count
    return namespaces
```

---

## 📚 Usage Examples

### Card Operations
```python
# Get complete card data
card_data = redis.get('mtg:cards:data:a1b2c3d4-...')

# Find all printings of a card
printings = redis.smembers('mtg:cards:oracle_to_printings:f8e7d6c5-...')

# Get current price
price = redis.get('mtg:pricing:current:card:a1b2c3d4:Near_Mint')
```

### Deck Operations
```python
# Get deck metadata
deck_meta = redis.get('mtg:decks:meta:deck_a1b2c3d4')

# Get deck composition with quantities
cards = redis.zrange('mtg:decks:cards:deck_a1b2c3d4', 0, -1, withscores=True)

# Find decks containing a specific card
decks = redis.smembers('mtg:decks:containing_card:a1b2c3d4-...')
```

### Search Operations
```python
# Text search
results = redis.sinter('mtg:search:words:lightning', 'mtg:search:words:bolt')

# Autocomplete
suggestions = redis.zrange('mtg:search:suggestions:light', 0, 9)

# Filter by attributes
red_cards = redis.smembers('mtg:search:by_color:R')
```

---

## 🔮 Future Enhancements

### Planned Features
1. **GraphQL Integration** - Direct Redis to GraphQL mapping
2. **Real-time Updates** - WebSocket-based price streaming
3. **ML Integration** - Predictive pricing models
4. **Advanced Analytics** - Market trend analysis
5. **Multi-tenant** - Support for multiple databases

### Schema Versioning
```
mtg:meta:schema:version          # Current schema version
mtg:meta:schema:migrations       # Applied migrations log
mtg:meta:schema:compatibility    # Backward compatibility info
```

---

This comprehensive schema provides a solid foundation for organizing your 2.1M+ Redis keys while maintaining performance, consistency, and scalability. The migration can be done incrementally, and the new structure will make your codebase much more maintainable. 