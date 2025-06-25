# Redis Key Analysis - Quick Commands

## 🚀 One-Liner Commands

### Basic Key Pattern Analysis
```bash
redis-cli --scan | head -1000 | cut -d: -f1-2 | sort | uniq -c | sort -nr
```

### Count Keys by Prefix
```bash
redis-cli --scan | head -1000 | awk -F: '{print $1}' | sort | uniq -c | sort -nr
```

### Get RediSearch Index Info
```bash
redis-cli FT._LIST | xargs -I{} redis-cli FT.INFO {}
```

### Sample Keys with Types
```bash
redis-cli --scan | head -20 | xargs -I{} redis-cli TYPE {}
```

### Memory Usage by Pattern (if supported)
```bash
redis-cli --scan --pattern "mtg:*" | head -100 | xargs -I{} redis-cli MEMORY USAGE {}
```

## 📊 Comprehensive Analysis Scripts

### Python Script (Detailed Analysis)
```bash
# Install dependency
pip install redis

# Run comprehensive analysis
python3 redis_key_analyzer.py --host localhost --port 6379 --format detailed

# Summary only
python3 redis_key_analyzer.py --format summary

# Export to JSON
python3 redis_key_analyzer.py --export
```

### Bash Script (Quick Analysis)
```bash
# Make executable and run
./redis_quick_scan.sh localhost 6379

# Or for default localhost:6379
./redis_quick_scan.sh
```

## 🔍 Specific MTGJSON Patterns to Check

### Check RediSearch Indexes
```bash
redis-cli FT._LIST
redis-cli FT.INFO mtg:cards:idx
redis-cli FT.INFO mtg:decks:idx
```

### Check New vs Old Patterns
```bash
# New RediSearch patterns
redis-cli --scan --pattern "mtg:*" | wc -l

# Old manual patterns  
redis-cli --scan --pattern "card:*" | wc -l
redis-cli --scan --pattern "deck:*" | wc -l
```

### Sample Data from Each Pattern
```bash
# Get sample RediSearch card data
redis-cli --scan --pattern "mtg:cards:data:*" | head -1 | xargs -I{} redis-cli JSON.GET {}

# Get sample old pattern data
redis-cli --scan --pattern "card:*" | head -1 | xargs -I{} redis-cli GET {}
```

## 📈 Performance Analysis

### Key Count by Database
```bash
redis-cli INFO keyspace
```

### Memory Usage Summary
```bash
redis-cli INFO memory
```

### RediSearch Performance
```bash
redis-cli FT.SEARCH mtg:cards:idx "*" LIMIT 0 0  # Get count only
time redis-cli FT.SEARCH mtg:cards:idx "@name:Lightning" LIMIT 0 5
```