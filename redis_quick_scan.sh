#!/bin/bash
# Quick Redis Key Pattern Scanner
# Usage: ./redis_quick_scan.sh [host] [port]

REDIS_HOST=${1:-localhost}
REDIS_PORT=${2:-6379}

echo "🔍 Redis Key Pattern Analysis"
echo "================================"
echo "Host: $REDIS_HOST:$REDIS_PORT"
echo ""

# Test connection
if ! redis-cli -h $REDIS_HOST -p $REDIS_PORT ping > /dev/null 2>&1; then
    echo "❌ Cannot connect to Redis at $REDIS_HOST:$REDIS_PORT"
    exit 1
fi

echo "✅ Connected to Redis"
echo ""

# Get basic info
echo "📊 REDIS INFO:"
redis-cli -h $REDIS_HOST -p $REDIS_PORT info keyspace | grep "db0"
echo ""

# Sample key patterns
echo "🔑 KEY PATTERN ANALYSIS:"
echo "------------------------"

# Get all keys (limit to first 10000 for performance)
redis-cli -h $REDIS_HOST -p $REDIS_PORT --scan --pattern "*" | head -10000 | \
awk -F: '{
    if (NF >= 2) {
        # Extract pattern
        pattern = $1
        for (i = 2; i < NF; i++) {
            if ($i ~ /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/) {
                # UUID pattern
                pattern = pattern ":{uuid}"
                break
            } else if ($i ~ /^[0-9]{7,}$/) {
                # SKU ID pattern  
                pattern = pattern ":{sku_id}"
                break
            } else if ($i ~ /^[A-Z]{3,4}$/) {
                # Set code pattern
                pattern = pattern ":{set_code}"
                break
            } else {
                pattern = pattern ":" $i
            }
        }
        count[pattern]++
        examples[pattern] = $0
    } else {
        count[$0]++
        examples[$0] = $0
    }
}
END {
    # Sort by count and display
    for (pattern in count) {
        print count[pattern] "\t" pattern "\t" examples[pattern]
    }
}' | sort -nr | head -20 | while read count pattern example; do
    echo "📋 $count keys: $pattern"
    echo "   Example: $example"
    echo ""
done

# RediSearch indexes
echo "🔎 REDISEARCH INDEXES:"
echo "----------------------"
if redis-cli -h $REDIS_HOST -p $REDIS_PORT FT._LIST 2>/dev/null | grep -v "ERR unknown command"; then
    redis-cli -h $REDIS_HOST -p $REDIS_PORT FT._LIST | while read index; do
        echo "🗂️ Index: $index"
        redis-cli -h $REDIS_HOST -p $REDIS_PORT FT.INFO $index | head -20 | grep -E "(num_docs|inverted_sz_mb)" | xargs -n2 echo "   "
        echo ""
    done
else
    echo "❌ RediSearch not available or no indexes found"
fi

echo "✅ Analysis complete!"