#!/usr/bin/env python3
"""
Interactive Deck Browser for MTGJSON Database
Provides hierarchical navigation: Deck Types -> Set Codes -> Individual Decks -> Actions
"""

import redis
import os
import json
import sys
from datetime import datetime

class DeckBrowser:
    def __init__(self):
        # Connect to Redis
        self.redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
        self.redis_port = int(os.getenv('REDIS_PORT', '9999'))
        self.client = None
        self.page_size = 20
        
    def connect(self):
        """Connect to Redis"""
        try:
            self.client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            self.client.ping()
            print(f"✓ Connected to Redis at {self.redis_host}:{self.redis_port}")
            return True
        except:
            print(f"✗ Could not connect to Redis at {self.redis_host}:{self.redis_port}")
            return False
    
    def get_deck_types(self):
        """Get all deck types with counts"""
        deck_types = {}
        
        # Get all deck meta keys
        deck_keys = self.client.keys('deck:meta:*')
        
        for key in deck_keys:
            try:
                deck_data = self.client.get(key)
                if deck_data:
                    deck = json.loads(deck_data)
                    deck_type = deck.get('type', 'Unknown')  # Note: 'type' not 'deck_type' in meta
                    if deck_type not in deck_types:
                        deck_types[deck_type] = {
                            'count': 0,
                            'sets': set()
                        }
                    deck_types[deck_type]['count'] += 1
                    deck_types[deck_type]['sets'].add(deck.get('code', 'UNK'))
            except Exception as e:
                continue
        
        return deck_types
    
    def get_sets_for_type(self, deck_type):
        """Get all set codes for a specific deck type with counts"""
        sets = {}
        
        deck_keys = self.client.keys('deck:meta:*')
        
        for key in deck_keys:
            try:
                deck_data = self.client.get(key)
                if deck_data:
                    deck = json.loads(deck_data)
                    if deck.get('type') == deck_type:  # Note: 'type' not 'deck_type' in meta
                        set_code = deck.get('code', 'UNK')
                        if set_code not in sets:
                            sets[set_code] = []
                        sets[set_code].append({
                            'uuid': deck.get('uuid', '').replace('deck_', ''),  # Extract UUID properly
                            'name': deck.get('name', 'Unknown'),
                            'release_date': deck.get('release_date', ''),
                            'estimated_value': deck.get('estimated_value', {})
                        })
            except Exception as e:
                continue
        
        return sets
    
    def display_paginated_list(self, items, title, page=0):
        """Display a paginated list of items"""
        start_idx = page * self.page_size
        end_idx = start_idx + self.page_size
        total_pages = (len(items) + self.page_size - 1) // self.page_size
        
        print(f"\n{title}")
        print("=" * len(title))
        print(f"Page {page + 1} of {total_pages} (Total items: {len(items)})")
        print()
        
        if isinstance(items[0], tuple):
            # For key-value pairs (like deck types)
            for i, (key, value) in enumerate(items[start_idx:end_idx], start_idx + 1):
                if isinstance(value, dict) and 'count' in value:
                    set_count = len(value.get('sets', set()))
                    print(f"{i:3d}. {key:<25} ({value['count']} decks, {set_count} sets)")
                else:
                    print(f"{i:3d}. {key:<25} ({len(value)} decks)")
        else:
            # For simple items (like deck names)
            for i, item in enumerate(items[start_idx:end_idx], start_idx + 1):
                if isinstance(item, dict):
                    value_str = ""
                    if 'estimated_value' in item and item['estimated_value']:
                        market_value = item['estimated_value'].get('market_total', 0)
                        if market_value > 0:
                            value_str = f" (${market_value:.0f})"
                    print(f"{i:3d}. {item['name']:<40}{value_str}")
                else:
                    print(f"{i:3d}. {item}")
        
        print()
        if total_pages > 1:
            nav_options = []
            if page > 0:
                nav_options.append("p: Previous page")
            if page < total_pages - 1:
                nav_options.append("n: Next page")
            if nav_options:
                print("Navigation: " + " | ".join(nav_options))
        print("b: Back | q: Quit")
        print()
        
        return total_pages
    
    def get_user_choice(self, max_choice, allow_nav=True):
        """Get user input with validation"""
        while True:
            try:
                choice = input("Enter choice: ").strip().lower()
                
                if choice == 'q':
                    return 'quit'
                elif choice == 'b':
                    return 'back'
                elif allow_nav and choice == 'p':
                    return 'prev'
                elif allow_nav and choice == 'n':
                    return 'next'
                else:
                    choice_num = int(choice)
                    if 1 <= choice_num <= max_choice:
                        return choice_num - 1  # Convert to 0-based index
                    else:
                        print(f"Please enter a number between 1 and {max_choice}")
            except ValueError:
                print("Please enter a valid number or command")
            except (KeyboardInterrupt, EOFError):
                return 'quit'
    
    def show_deck_actions(self, deck_uuid, deck_name):
        """Show available actions for a selected deck"""
        print(f"\nDeck: {deck_name}")
        print("=" * (len(deck_name) + 6))
        print("Available actions:")
        print()
        print("1. View Composition (all cards with quantities)")
        print("2. Show Deck Statistics (summary info)")
        print("3. Show Card Distribution (by type, rarity, etc.)")
        print("4. Export to TCG CSV")
        print("5. Show Most Expensive Cards")
        print()
        print("b: Back to deck list | q: Quit")
        
        while True:
            choice = input("\nEnter action: ").strip().lower()
            
            if choice == 'q':
                return 'quit'
            elif choice == 'b':
                return 'back'
            elif choice in ['1', '2', '3', '4', '5']:
                return self.execute_deck_action(deck_uuid, deck_name, choice)
            else:
                print("Please enter 1-5, 'b' for back, or 'q' to quit")
    
    def execute_deck_action(self, deck_uuid, deck_name, action):
        """Execute the selected deck action using Lua scripts"""
        print(f"\nExecuting action for: {deck_name}")
        print("-" * 50)
        
        try:
            if action == '1':  # Composition
                # The Lua script expects deck_{uuid} format
                formatted_uuid = f"deck_{deck_uuid}" if not deck_uuid.startswith('deck_') else deck_uuid
                result = self.client.eval(self.get_deck_search_script(), 0, 'composition', formatted_uuid)
                self.display_deck_composition(result)
            
            elif action == '2':  # Statistics  
                # Try meta first, then full deck data
                deck_data = self.client.get(f'deck:meta:deck_{deck_uuid}')
                if not deck_data:
                    deck_data = self.client.get(f'deck:deck_{deck_uuid}')
                if deck_data:
                    deck = json.loads(deck_data)
                    self.display_deck_statistics(deck)
            
            elif action == '3':  # Card Distribution
                formatted_uuid = f"deck_{deck_uuid}" if not deck_uuid.startswith('deck_') else deck_uuid
                result = self.client.eval(self.get_deck_search_script(), 0, 'composition', formatted_uuid)
                self.display_card_distribution(result)
            
            elif action == '4':  # Export CSV
                formatted_uuid = f"deck_{deck_uuid}" if not deck_uuid.startswith('deck_') else deck_uuid
                result = self.client.eval(self.get_export_csv_script(), 0, formatted_uuid, 'single')
                print("CSV Export generated (this would normally save to file)")
                print("First few lines:")
                if isinstance(result, str):
                    lines = result.split('\n')[:10]
                    for line in lines:
                        print(line)
            
            elif action == '5':  # Expensive Cards
                formatted_uuid = f"deck_{deck_uuid}" if not deck_uuid.startswith('deck_') else deck_uuid
                result = self.client.eval(self.get_deck_search_script(), 0, 'composition', formatted_uuid)
                self.display_expensive_cards(result)
        
        except Exception as e:
            print(f"Error executing action: {e}")
        
        input("\nPress Enter to continue...")
        return 'continue'
    
    def get_deck_search_script(self):
        """Load the deck search Lua script"""
        with open('lua/deck_search.lua', 'r') as f:
            return f.read()
    
    def get_export_csv_script(self):
        """Load the export CSV Lua script"""
        with open('lua/export_tcg_csv.lua', 'r') as f:
            return f.read()
    
    def display_deck_composition(self, result):
        """Display deck composition from Lua script result"""
        if not result:
            print("No composition data available (deck not found or no card data)")
            return
            
        if not isinstance(result, dict):
            print(f"Unexpected result format: {type(result)}")
            print(f"Result: {result}")
            return
        
        deck_info = result.get('deck_info', {})
        cards = result.get('cards', [])
        
        print(f"Deck: {deck_info.get('name', 'Unknown')}")
        print(f"Type: {deck_info.get('type', deck_info.get('deck_type', 'Unknown'))}")
        print(f"Release Date: {deck_info.get('release_date', 'Unknown')}")
        print()
        
        if cards and len(cards) > 0:
            print(f"Cards ({len(cards)} unique):")
            print("-" * 70)
            print(f"{'Qty':<4} {'Card Name':<40} {'Set':<6}")
            print("-" * 70)
            
            for card in cards[:50]:  # Limit display
                qty = card.get('quantity', 0)
                name = card.get('name', 'Unknown')[:39]  # Truncate long names
                set_code = card.get('set_code', 'UNK')[:5]
                print(f"{qty:<4} {name:<40} {set_code:<6}")
            
            if len(cards) > 50:
                print(f"... and {len(cards) - 50} more cards")
            print("-" * 70)
        else:
            print("No card composition data available for this deck")
            print("(This might be a metadata-only deck or the detailed data wasn't imported)")
    
    def display_deck_statistics(self, deck):
        """Display deck statistics"""
        print(f"Name: {deck.get('name', 'Unknown')}")
        print(f"Code: {deck.get('code', 'Unknown')}")
        print(f"Type: {deck.get('type', deck.get('deck_type', 'Unknown'))}")  # Handle both meta and full deck
        print(f"Release Date: {deck.get('release_date', 'Unknown')}")
        print(f"Is Commander: {deck.get('is_commander', 'Unknown')}")
        print()
        
        # Card counts - check both meta format and full deck format
        if 'total_cards' in deck and 'unique_cards' in deck:
            # Meta format
            print("Card Counts (from meta):")
            print(f"  Total Cards: {deck.get('total_cards', 0)}")
            print(f"  Unique Cards: {deck.get('unique_cards', 0)}")
        else:
            # Full deck format
            commanders = deck.get('commanders', [])
            main_board = deck.get('main_board', [])
            side_board = deck.get('side_board', [])
            
            print("Card Counts:")
            print(f"  Commanders: {len(commanders)}")
            print(f"  Main Board: {len(main_board)}")
            print(f"  Side Board: {len(side_board)}")
        print()
        
        # Estimated values
        estimated_value = deck.get('estimated_value', {})
        if estimated_value:
            print("Estimated Values:")
            for key, value in estimated_value.items():
                if isinstance(value, (int, float)) and value > 0:
                    print(f"  {key.replace('_', ' ').title()}: ${value:.2f}")
        
        # Additional meta information
        pricing_info = estimated_value.get('cards_with_pricing', None)
        if pricing_info is not None:
            print()
            print("Pricing Coverage:")
            total_pricing_cards = estimated_value.get('cards_with_pricing', 0) + estimated_value.get('cards_without_pricing', 0)
            if total_pricing_cards > 0:
                coverage_pct = (estimated_value.get('cards_with_pricing', 0) / total_pricing_cards) * 100
                print(f"  Cards with pricing: {estimated_value.get('cards_with_pricing', 0)}/{total_pricing_cards} ({coverage_pct:.1f}%)")
    
    def display_card_distribution(self, result):
        """Display card type/rarity distribution"""
        if not result or not isinstance(result, dict):
            print("No composition data available")
            return
        
        cards = result.get('cards', [])
        if not cards:
            print("No card data available")
            return
        
        # Get card details and analyze distribution
        type_counts = {}
        rarity_counts = {}
        
        for card in cards:
            # This would need card details from Redis
            # For now, just show basic info
            print("Card distribution analysis would go here")
            print("(Would need to fetch full card details from Redis)")
            break
    
    def display_expensive_cards(self, result):
        """Display most expensive cards in deck"""
        print("Expensive cards analysis would go here")
        print("(Would need pricing data integration)")
    
    def browse_deck_types(self):
        """Main deck type browser"""
        page = 0
        
        while True:
            deck_types = self.get_deck_types()
            if not deck_types:
                print("No decks found in database")
                return
            
            # Convert to sorted list
            type_list = [(k, v) for k, v in sorted(deck_types.items(), key=lambda x: x[1]['count'], reverse=True)]
            
            total_pages = self.display_paginated_list(type_list, "📦 Browse Decks by Type", page)
            
            choice = self.get_user_choice(len(type_list[page * self.page_size:(page + 1) * self.page_size]), 
                                        allow_nav=(total_pages > 1))
            
            if choice == 'quit':
                break
            elif choice == 'back':
                break
            elif choice == 'prev' and page > 0:
                page -= 1
            elif choice == 'next' and page < total_pages - 1:
                page += 1
            elif isinstance(choice, int):
                actual_index = page * self.page_size + choice
                if actual_index < len(type_list):
                    selected_type = type_list[actual_index][0]
                    self.browse_sets_for_type(selected_type)
    
    def browse_sets_for_type(self, deck_type):
        """Browse sets within a deck type"""
        page = 0
        
        while True:
            sets_data = self.get_sets_for_type(deck_type)
            if not sets_data:
                print(f"No sets found for deck type: {deck_type}")
                input("Press Enter to continue...")
                return
            
            # Convert to sorted list
            set_list = [(k, v) for k, v in sorted(sets_data.items(), key=lambda x: len(x[1]), reverse=True)]
            
            total_pages = self.display_paginated_list(set_list, f"📁 Sets in '{deck_type}' Decks", page)
            
            choice = self.get_user_choice(len(set_list[page * self.page_size:(page + 1) * self.page_size]), 
                                        allow_nav=(total_pages > 1))
            
            if choice == 'quit':
                return 'quit'
            elif choice == 'back':
                return
            elif choice == 'prev' and page > 0:
                page -= 1
            elif choice == 'next' and page < total_pages - 1:
                page += 1
            elif isinstance(choice, int):
                actual_index = page * self.page_size + choice
                if actual_index < len(set_list):
                    selected_set = set_list[actual_index][0]
                    decks = set_list[actual_index][1]
                    result = self.browse_decks_in_set(deck_type, selected_set, decks)
                    if result == 'quit':
                        return 'quit'
    
    def browse_decks_in_set(self, deck_type, set_code, decks):
        """Browse individual decks within a set"""
        page = 0
        
        while True:
            # Sort decks by estimated value (if available)
            sorted_decks = sorted(decks, key=lambda x: x.get('estimated_value', {}).get('market_total', 0), reverse=True)
            
            total_pages = self.display_paginated_list(sorted_decks, f"🃏 Decks in {deck_type} - {set_code}", page)
            
            choice = self.get_user_choice(len(sorted_decks[page * self.page_size:(page + 1) * self.page_size]), 
                                        allow_nav=(total_pages > 1))
            
            if choice == 'quit':
                return 'quit'
            elif choice == 'back':
                return
            elif choice == 'prev' and page > 0:
                page -= 1
            elif choice == 'next' and page < total_pages - 1:
                page += 1
            elif isinstance(choice, int):
                actual_index = page * self.page_size + choice
                if actual_index < len(sorted_decks):
                    selected_deck = sorted_decks[actual_index]
                    result = self.show_deck_actions(selected_deck['uuid'], selected_deck['name'])
                    if result == 'quit':
                        return 'quit'
    
    def run(self):
        """Main entry point"""
        if not self.connect():
            return
        
        print("\n🎴 MTGJSON Deck Browser")
        print("Navigate through deck types, sets, and individual decks")
        print("=" * 60)
        
        self.browse_deck_types()
        
        print("\nThanks for using the Deck Browser! 👋")

def main():
    browser = DeckBrowser()
    browser.run()

if __name__ == "__main__":
    main() 