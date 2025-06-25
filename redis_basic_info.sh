#!/bin/bash
# Super Simple Redis Info
# Usage: ./redis_basic_info.sh

echo "🔍 Basic Redis Analysis"
echo "======================"

# Connection test
if redis-cli ping > /dev/null 2>&1; then
    echo "✅ Redis connected"
else
    echo "❌ Redis not available"
    exit 1
fi

# Basic stats
echo ""
echo "📊 Key Statistics:"
redis-cli DBSIZE | awk '{print "   Total Keys: " $0}'
redis-cli INFO memory | grep used_memory_human | cut -d: -f2 | awk '{print "   Memory Used: " $0}'

echo ""
echo "🔑 Top Key Patterns:"
redis-cli --scan | head -500 | cut -d: -f1 | sort | uniq -c | sort -nr | head -10 | awk '{printf "   %8s keys: %s\n", $1, $2}'

echo ""
echo "🔎 RediSearch Indexes:"
if redis-cli FT._LIST 2>/dev/null | grep -q "."; then
    redis-cli FT._LIST | while read idx; do
        docs=$(redis-cli FT.INFO $idx 2>/dev/null | grep -A1 "num_docs" | tail -1)
        echo "   • $idx ($docs documents)"
    done
else
    echo "   None found"
fi

echo ""
echo "✅ Done!"