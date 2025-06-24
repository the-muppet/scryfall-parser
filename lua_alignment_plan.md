# Lua Script Alignment Plan for RediSearch

## 🎯 **Priority Scripts to Update**

### **High Priority (Core Analytics)**
1. **find_expensive_cards.lua** ✅ STARTED
   - Replace `card:{uuid}` → `mtg:cards:data:{uuid}` 
   - Replace `GET` → `JSON.GET` with array parsing
   
2. **deck_search.lua**
   - Replace `card:{uuid}:decks` → `mtg:cards:decks:{uuid}`
   - Replace `deck:{uuid}` → `mtg:decks:data:{uuid}`
   - Update to use `JSON.GET`

3. **export_tcg_csv.lua**  
   - Replace `card:{uuid}` → `mtg:cards:data:{uuid}`
   - Update TCG pricing lookups to new patterns

### **Medium Priority (Advanced Analytics)**
4. **sku_price_analysis.lua**
   - Update pricing chain patterns
   - Replace SCAN operations with RediSearch queries
   
5. **pricing_trends.lua**
   - Replace SCAN with FT.SEARCH for card discovery
   - Update price key patterns

### **Low Priority (Maintenance)**
6. **card_stats.lua** - Replace SCAN with FT.AGGREGATE
7. **find_missing_data.lua** - Use RediSearch for data discovery
8. **set_analysis.lua** - Update card data retrieval

## 🚀 **Alternative: Native Rust Implementation**

For better performance, some functions could be rewritten in Rust using RediSearch directly:

```rust
// Example: get_expensive_cards in pure Rust/RediSearch
pub async fn get_expensive_cards_native(&mut self, min_price: f64) -> Result<Vec<Card>> {
    let query = format!("@tcg_market_price:[{} +inf]", min_price);
    let results = redis::cmd("FT.SEARCH")
        .arg("mtg:cards:idx") 
        .arg(&query)
        .arg("SORTBY").arg("tcg_market_price").arg("DESC")
        .query_async(&mut self.client).await?;
    // Parse results...
}
```

## ⚡ **Quick Migration Strategy**

1. **Update 3 key scripts** (find_expensive_cards, deck_search, export_tcg_csv)
2. **Test core functionality** 
3. **Gradually migrate remaining scripts** as needed
4. **Consider Rust rewrites** for performance-critical functions

This maintains 95% functionality while providing the RediSearch performance benefits.