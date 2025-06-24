#!/usr/bin/env python3
"""
MTG Redis Client
Provides clean programmatic interface to the MTGJSON Redis database
"""

import redis
import json
import os
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path

class MTGRedisClient:
    """
    Comprehensive Redis client for MTGJSON database operations.
    Provides clean method names and integrates with Lua scripts.
    """
    
    def __init__(self, host='127.0.0.1', port=9999, db=0, **kwargs):
        """Initialize Redis connection with schema-aware methods"""
        self.client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            decode_responses=True,
            **kwargs
        )
        self._lua_scripts = {}
        self._load_lua_scripts()
    
    def _load_lua_scripts(self):
        """Load and cache all Lua scripts"""
        lua_dir = Path(__file__).parent / 'lua'
        
        script_mappings = {
            'search_cards': 'search_cards.lua',
            'deck_search': 'deck_search.lua',
            'card_stats': 'card_stats.lua',
            'find_expensive_cards': 'find_expensive_cards.lua',
            'price_comparison': 'price_comparison.lua',
            'pricing_trends': 'pricing_trends.lua',
            'sku_price_analysis': 'sku_price_analysis.lua',
            'set_analysis': 'set_analysis.lua',
            'export_tcg_csv': 'export_tcg_csv.lua',
            'cleanup_indexes': 'cleanup_indexes.lua',
            'create_redis_indexes': 'create_redis_indexes.lua'
        }
        
        loaded_count = 0
        for script_name, filename in script_mappings.items():
            script_path = lua_dir / filename
            if script_path.exists():
                try:
                    with open(script_path, 'r') as f:
                        script_content = f.read()
                        script_hash = self.client.script_load(script_content)
                        self._lua_scripts[script_name] = script_hash
                        loaded_count += 1
                except Exception as e:
                    print(f"Warning: Could not load Lua script '{filename}': {e}")
                    # Continue loading other scripts
                    pass
        
        print(f"✓ Loaded {loaded_count}/{len(script_mappings)} Lua scripts")
    
    def _execute_lua(self, script_name: str, *args) -> any:
        """Execute a loaded Lua script"""
        if script_name not in self._lua_scripts:
            raise ValueError(f"Lua script '{script_name}' not loaded. Available scripts: {list(self._lua_scripts.keys())}")
        
        try:
            return self.client.evalsha(self._lua_scripts[script_name], 0, *args)
        except Exception as e:
            raise RuntimeError(f"Error executing Lua script '{script_name}': {e}") from e

    # =============================================================================
    # CARD OPERATIONS
    # =============================================================================
    
    def get_card_by_uuid(self, uuid: str) -> Optional[Dict]:
        """Get complete card data by UUID"""
        data = self.client.get(f'card:{uuid}')
        return json.loads(data) if data else None
    
    def get_card_by_oracle_id(self, oracle_id: str) -> Optional[Dict]:
        """Get oracle card data (shared across printings)"""
        data = self.client.get(f'card:oracle:{oracle_id}')
        return json.loads(data) if data else None
    
    def search_cards_by_name(self, query: str, max_results: int = 50, **filters) -> List[Dict]:
        """Search cards by name with optional filters"""
        # Prepare filter arguments for Lua script
        args = [query, str(max_results)]
        for key, value in filters.items():
            args.extend([key, str(value)])
        
        return self._execute_lua('search_cards', *args)
    
    def get_cards_in_set(self, set_code: str) -> Set[str]:
        """Get all card UUIDs in a set"""
        return self.client.smembers(f'set:{set_code}:cards')
    
    def get_card_printings(self, oracle_id: str) -> Set[str]:
        """Get all printing UUIDs for an oracle card"""
        return self.client.smembers(f'oracle:{oracle_id}:printings')
    
    def get_cards_by_name_fuzzy(self, name: str) -> Set[str]:
        """Get card UUIDs by normalized name"""
        normalized = name.lower().replace(' ', '_').replace("'", '')
        return self.client.smembers(f'name:{normalized}')
    
    def get_expensive_cards(self, min_price: float = 50, max_results: int = 20) -> List[Dict]:
        """Find expensive cards above threshold"""
        return self._execute_lua('find_expensive_cards', str(min_price), str(max_results))
    
    def get_card_skus(self, uuid: str) -> Set[str]:
        """Get TCGPlayer SKU IDs for a card"""
        return self.client.smembers(f'card:{uuid}:skus')
    
    def get_card_by_sku_id(self, sku_id: str) -> Optional[str]:
        """Get card UUID from SKU ID"""
        return self.client.get(f'sku:{sku_id}')
    
    def get_card_by_tcgplayer_id(self, tcgplayer_id: str) -> Optional[str]:
        """Get card UUID from TCGPlayer product ID"""
        return self.client.get(f'tcgplayer:{tcgplayer_id}')
    
    def get_oracle_id_by_uuid(self, uuid: str) -> Optional[str]:
        """Get oracle ID from card UUID"""
        return self.client.get(f'oracle:{uuid}')
    
    def get_card_by_uuid_lookup(self, uuid: str) -> Optional[str]:
        """Get card UUID from uuid lookup (for oracle mappings)"""
        return self.client.get(f'uuid:{uuid}')
    
    def get_printing_info(self, printing_id: str) -> Optional[Dict]:
        """Get detailed printing information"""
        data = self.client.get(f'printing:info:{printing_id}')
        return json.loads(data) if data else None
    
    def get_card_printings_detailed(self, uuid: str) -> List[Dict]:
        """Get detailed printing information for a card"""
        printings = []
        
        # Get oracle ID first
        oracle_id = self.get_oracle_id_by_uuid(uuid)
        if oracle_id:
            # Get all printings for this oracle
            printing_uuids = self.get_card_printings(oracle_id)
            
            for printing_uuid in printing_uuids:
                printing_info = self.get_printing_info(printing_uuid)
                if printing_info:
                    printings.append(printing_info)
        
        return printings

    # =============================================================================
    # DECK OPERATIONS  
    # =============================================================================
    
    def get_deck_by_uuid(self, uuid: str) -> Optional[Dict]:
        """Get complete deck data by UUID"""
        # Try meta first for lightweight operations
        meta_data = self.client.get(f'deck:meta:deck_{uuid}')
        if meta_data:
            return json.loads(meta_data)
        
        # Fall back to full deck data
        full_data = self.client.get(f'deck:deck_{uuid}')
        return json.loads(full_data) if full_data else None
    
    def get_deck_composition(self, uuid: str) -> Optional[Dict]:
        """Get detailed deck composition with card list"""
        formatted_uuid = f"deck_{uuid}" if not uuid.startswith('deck_') else uuid
        return self._execute_lua('deck_search', 'composition', formatted_uuid)
    
    def get_deck_statistics(self) -> Dict:
        """Get overall deck statistics"""
        return self._execute_lua('deck_search', 'statistics')
    
    def get_commander_decks(self) -> List[Dict]:
        """Get all commander decks"""
        return self._execute_lua('deck_search', 'commander_decks')
    
    def find_decks_containing_card(self, card_name: str) -> List[Dict]:
        """Find decks containing a specific card"""
        return self._execute_lua('deck_search', 'contains_card', card_name)
    
    def get_expensive_decks(self, min_value: float = 100) -> List[Dict]:
        """Find expensive decks above value threshold"""
        return self._execute_lua('deck_search', 'expensive', str(min_value))
    
    def search_decks_by_name(self, deck_name: str) -> List[Dict]:
        """Search decks by name"""
        return self._execute_lua('deck_search', 'search_name', deck_name)
    
    def get_decks_by_type(self, deck_type: str) -> Set[str]:
        """Get all deck UUIDs of a specific type"""
        return self.client.smembers(f'deck:type:{deck_type}')
    
    def get_decks_in_set(self, set_code: str) -> Set[str]:
        """Get all deck UUIDs from a set"""
        return self.client.smembers(f'deck:set:{set_code}')
    
    def get_card_deck_info(self, card_uuid: str) -> Dict[str, str]:
        """Get deck information for a card (deck_uuid -> 'name|set|quantity')"""
        return self.client.hgetall(f'card:{card_uuid}:deck_info')
    
    def get_decks_containing_card_direct(self, card_uuid: str) -> Set[str]:
        """Get all deck UUIDs that contain this card (direct lookup)"""
        return self.client.smembers(f'card:{card_uuid}:decks')

    # =============================================================================
    # PRICING OPERATIONS
    # =============================================================================
    
    def get_card_price(self, uuid: str, condition: str = 'Near Mint') -> Optional[Dict]:
        """Get current price for a card in specific condition"""
        price_data = self.client.get(f'price:{uuid}:{condition}')
        return json.loads(price_data) if price_data else None
    
    def get_sku_price_latest(self, sku_id: str) -> Optional[Dict]:
        """Get latest price for a SKU"""
        price_data = self.client.get(f'price:sku:{sku_id}:latest')
        return json.loads(price_data) if price_data else None
    
    def get_sku_price_history(self, sku_id: str, days: int = 30) -> List[Tuple[float, float]]:
        """Get price history for a SKU"""
        end_time = int(datetime.now().timestamp())
        start_time = end_time - (days * 86400)
        
        history = self.client.zrangebyscore(
            f'price:sku:{sku_id}:history',
            start_time, end_time,
            withscores=True
        )
        
        # Return as (price, timestamp) tuples
        return [(float(price), float(timestamp)) for price, timestamp in history]
    
    def get_trending_cards(self, direction: str = 'up', limit: int = 20) -> List[Dict]:
        """Get trending cards (up/down)"""
        return self._execute_lua('sku_price_analysis', 'trending', direction, str(limit))
    
    def get_price_arbitrage_opportunities(self, card_filter: str = '', min_diff: float = 5.0) -> List[Dict]:
        """Find arbitrage opportunities between conditions"""
        return self._execute_lua('sku_price_analysis', 'arbitrage', card_filter, str(min_diff))
    
    def compare_card_prices_by_condition(self, card_name: str) -> List[Dict]:
        """Compare prices across all conditions for a card"""
        return self._execute_lua('sku_price_analysis', 'condition_compare', card_name)
    
    def get_pricing_trends_distribution(self) -> List[str]:
        """Get price distribution analysis"""
        return self._execute_lua('pricing_trends', 'distribution')
    
    def get_pricing_trends_by_set(self, set_code: str = '') -> List[str]:
        """Get price analysis by set"""
        return self._execute_lua('pricing_trends', 'by_set', set_code)
    
    def get_sku_metadata(self, sku_id: str) -> Optional[Dict]:
        """Get SKU metadata (condition, foil, language, product_id)"""
        data = self.client.get(f'sku:{sku_id}:meta')
        return json.loads(data) if data else None
    
    def get_tcgplayer_product_skus(self, product_id: str) -> Set[str]:
        """Get all SKU IDs for a TCGPlayer product Id"""
        return self.client.smembers(f'tcgplayer:{product_id}:skus')
    
    def bulk_get_sku_prices(self, sku_ids: List[str]) -> Dict[str, Dict]:
        """Get latest prices for multiple SKUs efficiently"""
        pipe = self.client.pipeline()
        for sku_id in sku_ids:
            pipe.get(f'price:sku:{sku_id}:latest')
        
        results = pipe.execute()
        prices = {}
        
        for sku_id, data in zip(sku_ids, results):
            if data:
                prices[sku_id] = json.loads(data)
        
        return prices

    # =============================================================================
    # SET OPERATIONS
    # =============================================================================
    
    def get_set_by_code(self, set_code: str) -> Optional[Dict]:
        """Get set information by code"""
        data = self.client.get(f'set:{set_code}')
        return json.loads(data) if data else None
    
    def get_set_analysis(self, set_code: str = '') -> List[Dict]:
        """Get detailed set analysis"""
        args = [set_code] if set_code else []
        return self._execute_lua('set_analysis', *args)
    
    def get_all_sets(self) -> List[str]:
        """Get all set codes"""
        return [key.replace('set:', '') for key in self.client.keys('set:*') 
                if not key.endswith(':cards')]

    # =============================================================================
    # SEARCH OPERATIONS
    # =============================================================================
    
    def autocomplete_card_names(self, prefix: str, limit: int = 10) -> List[str]:
        """Get autocomplete suggestions for card names"""
        suggestions = self.client.smembers(f'auto:prefix:{prefix.lower()}')
        return list(suggestions)[:limit]
    
    def get_autocomplete_prefixes(self, text: str, limit: int = 10) -> List[str]:
        """Get all available autocomplete prefixes matching text"""
        pattern = f'auto:prefix:{text.lower()}*'
        keys = self.client.keys(pattern)
        prefixes = [key.replace('auto:prefix:', '') for key in keys[:limit]]
        return prefixes
    
    def search_cards_by_type(self, card_type: str) -> Set[str]:
        """Get cards by type"""
        return self.client.smembers(f'type:{card_type.lower()}')
        
    def search_cards_by_ngram(self, ngram: str) -> Set[str]:
        """Get cards containing specific n-gram"""
        return self.client.smembers(f'ngram:{ngram.lower()}')
    
    def search_cards_by_metaphone(self, metaphone_code: str) -> Set[str]:
        """Get cards by metaphone code (phonetic matching)"""
        return self.client.smembers(f'metaphone:{metaphone_code}')
    
    def find_similar_card_names(self, name: str, limit: int = 10) -> List[str]:
        """Find similar card names using multiple search strategies"""
        results = set()
        
        # Word-based search
        words = name.lower().split()
        for word in words:
            if len(word) >= 3:
                word_matches = self.client.smembers(f'word:{word}')
                results.update(word_matches)
        
        # N-gram search for fuzzy matching
        if len(name) >= 3:
            for i in range(len(name) - 2):
                ngram = name[i:i+3].lower()
                ngram_matches = self.client.smembers(f'ngram:{ngram}')
                results.update(ngram_matches)
        
        # Convert to card names and return limited results
        card_names = []
        for uuid in list(results)[:limit*2]:  # Get more to filter
            card = self.get_card_by_uuid(uuid)
            if card and card.get('name'):
                card_names.append(card['name'])
                if len(card_names) >= limit:
                    break
        
        return card_names

    # =============================================================================
    # ANALYTICS & STATISTICS
    # =============================================================================
    
    def get_database_stats(self) -> List[str]:
        """Get comprehensive database statistics"""
        return self._execute_lua('card_stats')
    
    def get_missing_data_analysis(self, data_type: str = 'summary', max_results: int = 20) -> List[str]:
        """Analyze missing or incomplete data"""
        return self._execute_lua('find_missing_data', data_type, str(max_results))

    # =============================================================================
    # EXPORT OPERATIONS
    # =============================================================================
    
    def export_deck_to_tcg_csv(self, deck_uuid: str) -> str:
        """Export deck to TCGPlayer CSV format"""
        formatted_uuid = f"deck_{deck_uuid}" if not deck_uuid.startswith('deck_') else deck_uuid
        result = self._execute_lua('export_tcg_csv', formatted_uuid, 'single')
        
        if isinstance(result, str):
            # Parse JSON result
            data = json.loads(result)
            return data.get('csv_data', '')
        return result
    
    def export_all_decks_to_csv(self) -> str:
        """Export all decks to combined CSV"""
        result = self._execute_lua('export_tcg_csv', '', 'all')
        
        if isinstance(result, str):
            data = json.loads(result)
            return data.get('csv_data', '')
        return result

    # =============================================================================
    # MAINTENANCE OPERATIONS
    # =============================================================================
    
    def cleanup_search_indexes(self) -> List[str]:
        """Clean up orphaned search index entries"""
        return self._execute_lua('cleanup_indexes')
    
    def create_search_indexes(self) -> List[str]:
        """Create/rebuild search indexes"""
        return self._execute_lua('create_redis_indexes')
    
    def get_index_statistics(self) -> Dict[str, int]:
        """Get search index size statistics"""
        stats = {}
        
        # Count different index types
        index_patterns = {
            'ngrams': 'ngram:*',
            'words': 'word:*',
            'metaphones': 'metaphone:*', 
            'prefixes': 'auto:prefix:*',
            'types': 'type:*',
            'colors': 'color:*',
            'rarities': 'rarity:*'
        }
        
        for name, pattern in index_patterns.items():
            stats[name] = len(self.client.keys(pattern))
        
        return stats

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            self.client.ping()
            return True
        except:
            return False
    
    def get_key_count(self, pattern: str = '*') -> int:
        """Get count of keys matching pattern"""
        return len(self.client.keys(pattern))
    
    def get_memory_usage(self) -> Dict[str, any]:
        """Get Redis memory usage information"""
        info = self.client.info('memory')
        return {
            'used_memory': info['used_memory'],
            'used_memory_human': info['used_memory_human'],
            'used_memory_peak': info['used_memory_peak'],
            'used_memory_peak_human': info['used_memory_peak_human']
        }
    
    def cache_result(self, key: str, data: any, ttl: int = 3600) -> bool:
        """Cache result with TTL"""
        try:
            cache_key = f"cache:{key}"
            if isinstance(data, (dict, list)):
                data = json.dumps(data)
            self.client.setex(cache_key, ttl, data)
            return True
        except:
            return False
    
    def get_cached_result(self, key: str) -> Optional[any]:
        """Get cached result"""
        cache_key = f"cache:{key}"
        data = self.client.get(cache_key)
        if data:
            try:
                return json.loads(data)
            except:
                return data
        return None

    # =============================================================================
    # BULK OPERATIONS
    # =============================================================================
    
    def bulk_get_cards(self, uuids: List[str]) -> Dict[str, Dict]:
        """Get multiple cards efficiently"""
        pipe = self.client.pipeline()
        for uuid in uuids:
            pipe.get(f'card:{uuid}')
        
        results = pipe.execute()
        cards = {}
        
        for uuid, data in zip(uuids, results):
            if data:
                cards[uuid] = json.loads(data)
        
        return cards
    
    def bulk_get_prices(self, uuids: List[str], condition: str = 'Near Mint') -> Dict[str, Dict]:
        """Get multiple card prices efficiently"""
        pipe = self.client.pipeline()
        for uuid in uuids:
            pipe.get(f'price:{uuid}:{condition}')
        
        results = pipe.execute()
        prices = {}
        
        for uuid, data in zip(uuids, results):
            if data:
                prices[uuid] = json.loads(data)
        
        return prices
    
    def batch_operation(self, operations: List[Tuple[str, List]], pipeline: bool = True) -> List[any]:
        """Execute multiple operations efficiently"""
        if pipeline:
            pipe = self.client.pipeline()
            for method_name, args in operations:
                method = getattr(pipe, method_name)
                method(*args)
            return pipe.execute()
        else:
            results = []
            for method_name, args in operations:
                method = getattr(self.client, method_name)
                results.append(method(*args))
            return results

# =============================================================================
# CONVENIENCE FACTORY FUNCTIONS
# =============================================================================

def create_mtg_client(host='127.0.0.1', port=9999, **kwargs) -> MTGRedisClient:
    """Create MTG Redis client with default settings"""
    return MTGRedisClient(host=host, port=port, **kwargs)

def create_mtg_client_from_env() -> MTGRedisClient:
    """Create MTG Redis client from environment variables"""
    host = os.getenv('REDIS_HOST', '127.0.0.1')
    port = int(os.getenv('REDIS_PORT', '9999'))
    return MTGRedisClient(host=host, port=port)
