# Redis Key Analysis Tools 🔍

I've created several tools to help you analyze your Redis database and understand key patterns, types, and structure.

## 🚀 Quick Start

### Super Simple (30 seconds)
```bash
./redis_basic_info.sh
```
**Output**: Basic stats, top patterns, RediSearch indexes

### Quick Scan (1-2 minutes)
```bash
./redis_quick_scan.sh
```
**Output**: Detailed pattern analysis, examples, memory info

### Comprehensive Analysis (2-5 minutes)
```bash
pip install redis
python3 redis_key_analyzer.py
```
**Output**: Full analysis with types, memory, TTL, sample data

## 📁 Files Created

| File | Purpose | Complexity |
|------|---------|-----------|
| `redis_basic_info.sh` | Super quick overview | ⭐ Simple |
| `redis_quick_scan.sh` | Pattern analysis with bash | ⭐⭐ Medium |
| `redis_key_analyzer.py` | Full comprehensive analysis | ⭐⭐⭐ Advanced |
| `redis_patterns_oneliner.md` | One-liner commands | ⭐ Reference |
| `requirements.txt` | Python dependencies | - |

## 🎯 What You'll Learn

### Pattern Discovery
- **Key prefixes and namespaces** (e.g., `mtg:cards:data:`, `card:`)
- **Count of keys per pattern** 
- **Redis data types** used (STRING, JSON, SET, ZSET, etc.)
- **Sample keys and values** for each pattern

### Performance Insights
- **Memory usage** per pattern and total
- **RediSearch indexes** and their document counts
- **TTL information** (which keys expire)
- **Migration status** (old vs new patterns)

### MTGJSON-Specific Analysis
- **New RediSearch patterns** (`mtg:cards:data:`, `mtg:decks:data:`)
- **Old manual patterns** (`card:`, `deck:`, `name:`)
- **TCGPlayer pricing chains** (`mtg:tcg:uuid_to_product:`, etc.)
- **Index alignment status** between Rust code and Lua scripts

## 🔧 Usage Examples

### Check Migration Status
```bash
# Quick check of old vs new patterns
redis-cli --scan --pattern "mtg:*" | wc -l    # New patterns
redis-cli --scan --pattern "card:*" | wc -l   # Old patterns
```

### Performance Comparison
```bash
# Test RediSearch vs manual lookup speed
time redis-cli FT.SEARCH mtg:cards:idx "@name:Lightning" LIMIT 0 5
time redis-cli SMEMBERS "name:lightning bolt"
```

### Analyze Specific Patterns
```bash
# Check TCGPlayer pricing chain
./redis_quick_scan.sh | grep -A5 "mtg:tcg"

# Look at memory usage
python3 redis_key_analyzer.py --format detailed | grep -A3 "Memory:"
```

## 🎨 Sample Output

```
🔍 Basic Redis Analysis
======================
✅ Redis connected

📊 Key Statistics:
   Total Keys: 2,847,392
   Memory Used: 1.2GB

🔑 Top Key Patterns:
   1,245,892 keys: mtg:cards:data
     324,156 keys: mtg:tcg:sku_price
      89,234 keys: card
      45,123 keys: mtg:decks:data
      12,456 keys: price

🔎 RediSearch Indexes:
   • mtg:cards:idx (245,892 documents)
   • mtg:decks:idx (12,456 documents)

✅ Done!
```

## 🚨 Troubleshooting

### Redis Connection Issues
```bash
# Check if Redis is running
redis-cli ping

# Try different port
./redis_quick_scan.sh localhost 9999

# Check with auth
redis-cli -a yourpassword ping
```

### Python Dependencies
```bash
# Install requirements
pip install -r requirements.txt

# Or manually
pip install redis>=4.0.0
```

### No RediSearch
If you see "RediSearch not available", it means:
- Redis Stack is not installed, or
- No RediSearch indexes have been created yet

## 🎯 Use Cases

**Before reindexing**: See current schema and identify patterns to migrate
**After reindexing**: Verify new RediSearch indexes are created and populated  
**Performance monitoring**: Track memory usage and key counts over time
**Schema validation**: Ensure Lua scripts use correct key patterns
**Debugging**: Find sample data when API calls aren't working

---

Choose the tool that matches your needs - from quick 30-second checks to comprehensive analysis! 🚀