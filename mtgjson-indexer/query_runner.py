#!/usr/bin/env python3
"""
MTGJSON Query Runner
Executes Lua scripts against MTGJSON-indexed Redis data
"""

import redis
import json
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

class MTGJSONQueryRunner:
    def __init__(self, redis_host='127.0.0.1', redis_port=9999):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.script_dir = Path(__file__).parent / 'lua'
        self.loaded_scripts = {}
        
    def connect(self) -> bool:
        """Test Redis connection"""
        try:
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {self.redis_client.connection_pool.connection_kwargs['host']}:{self.redis_client.connection_pool.connection_kwargs['port']}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to Redis: {e}")
            return False
    
    def load_script(self, script_name: str) -> Optional[str]:
        """Load and cache a Lua script"""
        if script_name in self.loaded_scripts:
            return self.loaded_scripts[script_name]
        
        script_path = self.script_dir / f"{script_name}.lua"
        if not script_path.exists():
            print(f"✗ Script not found: {script_path}")
            return None
        
        try:
            with open(script_path, 'r') as f:
                script_content = f.read()
            
            # Load script into Redis and get SHA
            script_sha = self.redis_client.script_load(script_content)
            self.loaded_scripts[script_name] = script_sha
            print(f"✓ Loaded script: {script_name}")
            return script_sha
        except Exception as e:
            print(f"✗ Failed to load script {script_name}: {e}")
            return None
    
    def execute_script(self, script_name: str, args: List[str] = None) -> Any:
        """Execute a Lua script with arguments"""
        script_sha = self.load_script(script_name)
        if not script_sha:
            return None
        
        try:
            result = self.redis_client.evalsha(script_sha, 0, *(args or []))
            return result
        except Exception as e:
            print(f"✗ Failed to execute script {script_name}: {e}")
            return None
    
    def search_cards(self, query: str, max_results: int = 50, filters: Dict[str, str] = None) -> List[Dict]:
        """Search for cards using the search_cards Lua script"""
        args = [query, str(max_results)]
        
        # Add filters as key-value pairs
        if filters:
            for key, value in filters.items():
                args.extend([key, value])
        
        results = self.execute_script('search_cards', args)
        return results or []
    
    def analyze_set(self, set_code: str = None) -> List[Dict]:
        """Analyze a specific set or all sets"""
        args = [set_code] if set_code else []
        results = self.execute_script('set_analysis', args)
        return results or []
    
    def get_database_stats(self) -> Dict:
        """Get overall database statistics"""
        try:
            stats_data = self.redis_client.get('mtgjson:stats')
            if stats_data:
                return json.loads(stats_data)
            else:
                return {"error": "No stats found"}
        except Exception as e:
            return {"error": str(e)}
    
    def search_decks(self, deck_name: str) -> List[Dict]:
        """Search for decks by name"""
        args = ['search_name', deck_name]
        results = self.execute_script('deck_search', args)
        return results or []
    
    def get_commander_decks(self) -> List[Dict]:
        """Get all commander decks"""
        args = ['commander_decks']
        results = self.execute_script('deck_search', args)
        return results or []
    
    def find_decks_with_card(self, card_name: str) -> List[Dict]:
        """Find decks containing a specific card"""
        args = ['contains_card', card_name]
        results = self.execute_script('deck_search', args)
        return results or []
    
    def get_deck_statistics(self) -> Dict:
        """Get deck database statistics"""
        args = ['statistics']
        result = self.execute_script('deck_search', args)
        return result or {}
    
    def get_deck_composition(self, deck_uuid: str) -> Dict:
        """Get deck composition"""
        args = ['composition', deck_uuid]
        result = self.execute_script('deck_search', args)
        return result or {}
    
    def find_expensive_decks(self, min_value: float = 100) -> List[Dict]:
        """Find expensive decks"""
        args = ['expensive', str(min_value)]
        results = self.execute_script('deck_search', args)
        return results or []
    
    def analyze_sealed_arbitrage(self, analysis_type: str = 'all', min_diff: float = 5.0, limit: int = 20) -> str:
        """Analyze arbitrage opportunities between sealed products and singles"""
        args = [analysis_type, str(min_diff), str(limit)]
        result = self.execute_script('sealed_arbitrage', args)
        return result or "No arbitrage data available."
    
    def format_search_results(self, results: List[Dict], show_details: bool = False) -> None:
        """Format and display search results"""
        if not results:
            print("No results found.")
            return
        
        if isinstance(results, list) and len(results) == 1 and isinstance(results[0], str) and results[0].startswith("Error:"):
            print(f"❌ {results[0]}")
            return
        
        print(f"\n📋 Found {len(results)} results:")
        print("-" * 80)
        
        for i, card in enumerate(results, 1):
            if isinstance(card, dict):
                name = card.get('name', 'Unknown')
                set_info = f"{card.get('set_code', '???')} #{card.get('collector_number', '???')}"
                rarity = card.get('rarity', 'unknown').upper()
                mana_cost = card.get('mana_cost', '')
                types = card.get('types', '')
                
                print(f"{i:3d}. {name:<30} | {set_info:<8} | {rarity:<7} | {mana_cost:<10}")
                
                if show_details:
                    text = card.get('text', '')
                    if text:
                        print(f"     {text[:100]}{'...' if len(text) > 100 else ''}")
                    
                    power = card.get('power', '')
                    toughness = card.get('toughness', '')
                    if power and toughness:
                        print(f"     Power/Toughness: {power}/{toughness}")
                    
                    tcg_id = card.get('tcgplayer_product_id', '')
                    if tcg_id:
                        print(f"     TCGPlayer ID: {tcg_id}")
                    
                    print()
    
    def format_set_analysis(self, results: List[Dict]) -> None:
        """Format and display set analysis results"""
        if not results:
            print("No set data found.")
            return
        
        if len(results) == 1:
            # Detailed single set analysis
            set_data = results[0]
            print(f"\n🎯 Set Analysis: {set_data['set_name']} ({set_data['set_code']})")
            print("=" * 60)
            print(f"Release Date: {set_data['release_date']}")
            print(f"Set Type: {set_data['set_type']}")
            print(f"Total Cards: {set_data['total_cards']}")
            print(f"Base Set Size: {set_data['base_set_size']}")
            
            if 'rarity_breakdown' in set_data:
                print("\n📊 Rarity Breakdown:")
                for rarity, count in set_data['rarity_breakdown'].items():
                    if count > 0:
                        print(f"  {rarity.title()}: {count}")
                
                print("\n🎨 Color Breakdown:")
                colors = {'W': 'White', 'U': 'Blue', 'B': 'Black', 'R': 'Red', 'G': 'Green', 'C': 'Colorless'}
                for color_code, count in set_data['color_breakdown'].items():
                    if count > 0:
                        color_name = colors.get(color_code, color_code)
                        print(f"  {color_name}: {count}")
                
                print(f"\n🔢 Special Properties:")
                print(f"  Reserved List Cards: {set_data['reserved_cards']}")
                print(f"  Promo Cards: {set_data['promo_cards']}")
                print(f"  Foil Available: {set_data['foil_available']}")
        else:
            # Multi-set summary
            print(f"\n📋 Set Summary ({len(results)} sets):")
            print("-" * 90)
            print(f"{'Code':<6} {'Name':<35} {'Date':<12} {'Type':<15} {'Cards':<6} {'R':<3} {'M':<3}")
            print("-" * 90)
            
            for set_data in results[:50]:  # Limit to first 50 sets
                code = set_data['set_code']
                name = set_data['set_name'][:34]
                date = set_data['release_date']
                set_type = set_data['set_type'][:14]
                total = set_data['total_cards']
                rares = set_data.get('rare_count', 0)
                mythics = set_data.get('mythic_count', 0)
                
                print(f"{code:<6} {name:<35} {date:<12} {set_type:<15} {total:<6} {rares:<3} {mythics:<3}")
            
            if len(results) > 50:
                print(f"\n... and {len(results) - 50} more sets")
    
    def format_deck_results(self, results: List[Dict], show_details: bool = False) -> None:
        """Format and display deck search results"""
        if not results:
            print("No decks found.")
            return
        
        print(f"\n🃏 Found {len(results)} deck(s):")
        print("-" * 100)
        
        for i, deck in enumerate(results, 1):
            if isinstance(deck, dict):
                name = deck.get('name', 'Unknown')
                deck_type = deck.get('deck_type', 'Unknown')
                release_date = deck.get('release_date', 'Unknown')
                is_commander = "Commander" if deck.get('is_commander') else "Constructed"
                
                print(f"{i:3d}. {name:<35} | {deck_type:<15} | {is_commander:<11} | {release_date}")
                
                if show_details and deck.get('estimated_value'):
                    value = deck['estimated_value']
                    market_total = value.get('market_total', 0)
                    print(f"     Estimated Value: ${market_total:.2f} ({value.get('cards_with_pricing', 0)} cards priced)")
                
                if show_details and deck.get('commanders'):
                    commanders = [c.get('name', 'Unknown') for c in deck['commanders']]
                    if commanders:
                        print(f"     Commanders: {', '.join(commanders)}")
                
                if show_details:
                    print()
    
    def format_deck_statistics(self, stats: Dict) -> None:
        """Format and display deck statistics"""
        if not stats or 'error' in stats:
            print("No deck statistics available.")
            return
        
        print("\n🃏 Deck Database Statistics:")
        print("=" * 50)
        print(f"Total Decks: {stats.get('total_decks', 0)}")
        print(f"Commander Decks: {stats.get('commander_decks', 0)}")
        print(f"Constructed Decks: {stats.get('constructed_decks', 0)}")
        
        if 'deck_types' in stats:
            print(f"\n📊 Deck Types:")
            for deck_type, count in stats['deck_types'].items():
                print(f"  {deck_type}: {count}")
        
        if 'value_ranges' in stats:
            print(f"\n💰 Value Distribution:")
            for range_name, count in stats['value_ranges'].items():
                range_display = range_name.replace('_', '-').title()
                print(f"  ${range_display}: {count} decks")
    
    def format_deck_composition(self, composition: Dict) -> None:
        """Format and display deck composition"""
        if not composition or 'deck_info' not in composition:
            print("Deck not found or no composition available.")
            return
        
        deck_info = composition['deck_info']
        cards = composition.get('cards', [])
        
        print(f"\n🃏 Deck: {deck_info['name']}")
        print(f"Code: {deck_info['code']}")
        print(f"Type: {deck_info['deck_type']}")
        print(f"Release Date: {deck_info['release_date']}")
        print(f"Total Cards: {deck_info['total_cards']}")
        print(f"Unique Cards: {deck_info['unique_cards']}")
        
        if deck_info.get('estimated_value'):
            value = deck_info['estimated_value']
            print(f"Estimated Value: ${value['market_total']:.2f}")
        
        if deck_info.get('is_commander') and deck_info.get('commanders'):
            print(f"\nCommanders:")
            for commander in deck_info['commanders']:
                print(f"  • {commander['name']} ({commander['set_code']})")
        
        print(f"\n📋 Card List ({len(cards)} cards):")
        print("-" * 60)
        
        # Group cards by quantity
        sorted_cards = sorted(cards, key=lambda x: (-x['quantity'], x['name']))
        
        for card in sorted_cards:
            quantity = card['quantity']
            name = card['name']
            set_code = card['set_code']
            print(f"{quantity:2d}x {name:<30} ({set_code})")
    
    def format_expensive_decks(self, results: List[Dict]) -> None:
        """Format and display expensive deck results"""
        if not results:
            print("No expensive decks found.")
            return
        
        print(f"\n💰 Found {len(results)} expensive deck(s):")
        print("-" * 120)
        print(f"{'Name':<35} {'Type':<15} {'Release':<12} {'Market Value':<12} {'Direct':<10} {'Low':<10}")
        print("-" * 120)
        
        for deck in results:
            name = deck['name'][:34]
            deck_type = deck['deck_type'][:14]
            release = deck['release_date']
            
            if deck.get('estimated_value'):
                value = deck['estimated_value']
                market = f"${value['market_total']:.2f}"
                direct = f"${value['direct_total']:.2f}"
                low = f"${value['low_total']:.2f}"
            else:
                market = direct = low = "N/A"
            
            print(f"{name:<35} {deck_type:<15} {release:<12} {market:<12} {direct:<10} {low:<10}")
        
        if results and results[0].get('estimated_value'):
            total_value = sum(d['estimated_value']['market_total'] for d in results if d.get('estimated_value'))
            print(f"\nTotal Portfolio Value: ${total_value:.2f}")

def main():
    parser = argparse.ArgumentParser(description='MTGJSON Query Runner')
    parser.add_argument('--redis-host', default='127.0.0.1', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=9999, help='Redis port')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for cards')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--max-results', type=int, default=20, help='Maximum results')
    search_parser.add_argument('--set', help='Filter by set code')
    search_parser.add_argument('--color', help='Filter by color (W/U/B/R/G)')
    search_parser.add_argument('--type', help='Filter by card type')
    search_parser.add_argument('--rarity', help='Filter by rarity')
    search_parser.add_argument('--mana-value', type=int, help='Filter by exact mana value')
    search_parser.add_argument('--min-mana-value', type=int, help='Filter by minimum mana value')
    search_parser.add_argument('--max-mana-value', type=int, help='Filter by maximum mana value')
    search_parser.add_argument('--reserved', action='store_true', help='Show only reserved list cards')
    search_parser.add_argument('--promo', action='store_true', help='Show only promo cards')
    search_parser.add_argument('--details', action='store_true', help='Show detailed card information')
    
    # Set analysis command
    set_parser = subparsers.add_parser('analyze-set', help='Analyze set(s)')
    set_parser.add_argument('set_code', nargs='?', help='Set code to analyze (if not provided, analyzes all sets)')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    
    # Deck commands
    deck_search_parser = subparsers.add_parser('deck-search', help='Search for decks by name')
    deck_search_parser.add_argument('deck_name', help='Deck name to search for')
    
    deck_list_parser = subparsers.add_parser('commander-decks', help='List all commander decks')
    
    deck_card_parser = subparsers.add_parser('decks-with-card', help='Find decks containing a specific card')
    deck_card_parser.add_argument('card_name', help='Card name to search for')
    
    deck_stats_parser = subparsers.add_parser('deck-stats', help='Show deck database statistics')
    
    deck_comp_parser = subparsers.add_parser('deck-composition', help='Show composition of specific deck')
    deck_comp_parser.add_argument('deck_uuid', help='Deck UUID')
    
    deck_expensive_parser = subparsers.add_parser('expensive-decks', help='Find expensive decks')
    deck_expensive_parser.add_argument('--min-value', type=float, default=100, help='Minimum deck value')
    
    # Sealed arbitrage command
    arbitrage_parser = subparsers.add_parser('sealed-arbitrage', help='Find arbitrage opportunities between sealed products and singles')
    arbitrage_parser.add_argument('--type', choices=['all', 'profitable', 'losing', 'commander', 'theme'], 
                                default='all', help='Type of arbitrage analysis')
    arbitrage_parser.add_argument('--min-diff', type=float, default=5.0, help='Minimum price difference ($)')
    arbitrage_parser.add_argument('--limit', type=int, default=20, help='Maximum results to show')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize query runner
    runner = MTGJSONQueryRunner(args.redis_host, args.redis_port)
    
    if not runner.connect():
        sys.exit(1)
    
    # Execute command
    if args.command == 'search':
        filters = {}
        if args.set:
            filters['set'] = args.set
        if args.color:
            filters['color'] = args.color.upper()
        if args.type:
            filters['type'] = args.type
        if args.rarity:
            filters['rarity'] = args.rarity
        if args.mana_value is not None:
            filters['mana_value'] = str(args.mana_value)
        if args.min_mana_value is not None:
            filters['min_mana_value'] = str(args.min_mana_value)
        if args.max_mana_value is not None:
            filters['max_mana_value'] = str(args.max_mana_value)
        if args.reserved:
            filters['is_reserved'] = 'true'
        if args.promo:
            filters['is_promo'] = 'true'
        
        results = runner.search_cards(args.query, args.max_results, filters)
        runner.format_search_results(results, args.details)
    
    elif args.command == 'analyze-set':
        results = runner.analyze_set(args.set_code)
        runner.format_set_analysis(results)
    
    elif args.command == 'stats':
        stats = runner.get_database_stats()
        print("\n📊 Database Statistics:")
        print("=" * 40)
        for key, value in stats.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
    
    elif args.command == 'deck-search':
        results = runner.search_decks(args.deck_name)
        runner.format_deck_results(results, show_details=True)
    
    elif args.command == 'commander-decks':
        results = runner.get_commander_decks()
        runner.format_deck_results(results, show_details=True)
    
    elif args.command == 'decks-with-card':
        results = runner.find_decks_with_card(args.card_name)
        if results:
            print(f"\n🃏 Decks containing '{args.card_name}':")
            print("-" * 80)
            for deck in results:
                name = deck.get('deck_name', 'Unknown')
                deck_type = deck.get('deck_type', 'Unknown')
                quantity = deck.get('quantity', 0)
                value = deck.get('estimated_value', {}).get('market_total', 0) if deck.get('estimated_value') else 0
                print(f"  • {name} ({deck_type}) - {quantity}x cards - ${value:.2f}")
        else:
            print(f"No decks found containing '{args.card_name}'")
    
    elif args.command == 'deck-stats':
        stats = runner.get_deck_statistics()
        runner.format_deck_statistics(stats)
    
    elif args.command == 'deck-composition':
        composition = runner.get_deck_composition(args.deck_uuid)
        runner.format_deck_composition(composition)
    
    elif args.command == 'expensive-decks':
        results = runner.find_expensive_decks(args.min_value)
        runner.format_expensive_decks(results)
    
    elif args.command == 'sealed-arbitrage':
        result = runner.analyze_sealed_arbitrage(args.type, args.min_diff, args.limit)
        print(result)

if __name__ == "__main__":
    main() 