#!/usr/bin/env python3
"""
Comprehensive API Testing Script for MTGJSON API
Tests all endpoints, validates responses, measures performance, and reports results.
"""

import requests
import json
import time
import sys
import argparse
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import statistics
import random
import concurrent.futures
import threading
from urllib.parse import urljoin, urlencode

# ANSI color codes for pretty output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'

@dataclass
class TestResult:
    endpoint: str
    method: str
    status_code: int
    response_time: float
    success: bool
    error_message: Optional[str] = None
    response_size: Optional[int] = None
    data_count: Optional[int] = None

@dataclass
class TestSuite:
    name: str
    results: List[TestResult]
    total_time: float

class MTGAPITester:
    def __init__(self, base_url: str = "http://localhost:8888", timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.test_results: List[TestResult] = []
        self.sample_uuids: List[str] = []
        self.sample_deck_uuids: List[str] = []
        self.sample_set_codes: List[str] = []
        self.sample_sku_ids: List[str] = []
        
        # Test data cache
        self._test_data_cache = {}
        
        # Statistics
        self.stats = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'total_time': 0.0,
            'avg_response_time': 0.0,
            'fastest_response': float('inf'),
            'slowest_response': 0.0
        }

    def log(self, message: str, color: str = Colors.WHITE):
        """Log a message with optional color"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{Colors.CYAN}[{timestamp}]{Colors.END} {color}{message}{Colors.END}")

    def log_success(self, message: str):
        self.log(f"✅ {message}", Colors.GREEN)

    def log_error(self, message: str):
        self.log(f"❌ {message}", Colors.RED)

    def log_warning(self, message: str):
        self.log(f"⚠️  {message}", Colors.YELLOW)

    def log_info(self, message: str):
        self.log(f"ℹ️  {message}", Colors.BLUE)

    def make_request(self, endpoint: str, method: str = "GET", params: Dict = None, 
                    expected_status: int = 200) -> TestResult:
        """Make a request and return test result"""
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        start_time = time.time()
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=self.timeout)
            else:
                response = self.session.request(method, url, params=params, timeout=self.timeout)
            
            response_time = time.time() - start_time
            
            # Try to get response size and data count
            response_size = len(response.content) if response.content else 0
            data_count = None
            
            try:
                json_data = response.json()
                if isinstance(json_data, dict):
                    if 'data' in json_data:
                        data = json_data['data']
                        if isinstance(data, list):
                            data_count = len(data)
                        elif isinstance(data, dict):
                            # Check for common count fields
                            for count_field in ['count', 'total', 'length']:
                                if count_field in data:
                                    data_count = data[count_field]
                                    break
            except:
                pass
            
            success = response.status_code == expected_status
            error_message = None if success else f"Expected {expected_status}, got {response.status_code}"
            
            if not success and response.content:
                try:
                    error_data = response.json()
                    if isinstance(error_data, dict) and 'error' in error_data:
                        error_message += f": {error_data['error']}"
                except:
                    error_message += f": {response.text[:200]}"
            
            return TestResult(
                endpoint=endpoint,
                method=method,
                status_code=response.status_code,
                response_time=response_time,
                success=success,
                error_message=error_message,
                response_size=response_size,
                data_count=data_count
            )
            
        except requests.exceptions.RequestException as e:
            response_time = time.time() - start_time
            return TestResult(
                endpoint=endpoint,
                method=method,
                status_code=0,
                response_time=response_time,
                success=False,
                error_message=f"Request failed: {str(e)}"
            )

    def collect_sample_data(self):
        """Collect sample UUIDs and IDs for testing"""
        self.log_info("Collecting sample data for testing...")
        
        # Get sample card UUIDs
        try:
            response = self.session.get(f"{self.base_url}/cards/search/name", 
                                      params={"q": "Lightning Bolt", "limit": 5}, 
                                      timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data', {}).get('results'):
                    self.sample_uuids = [card.get('uuid') for card in data['data']['results'][:5] 
                                       if card.get('uuid')]
        except:
            pass
        
        # Get sample deck UUIDs
        try:
            response = self.session.get(f"{self.base_url}/decks/commanders", timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data', {}).get('decks'):
                    self.sample_deck_uuids = [deck.get('uuid') for deck in data['data']['decks'][:5] 
                                            if deck.get('uuid')]
        except:
            pass
        
        # Get sample set codes
        try:
            response = self.session.get(f"{self.base_url}/sets", timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data', {}).get('sets'):
                    self.sample_set_codes = data['data']['sets'][:10]
        except:
            pass
        
        self.log_info(f"Collected {len(self.sample_uuids)} card UUIDs, {len(self.sample_deck_uuids)} deck UUIDs, {len(self.sample_set_codes)} set codes")

    def test_health_endpoints(self) -> TestSuite:
        """Test health and status endpoints"""
        self.log_info("Testing health and status endpoints...")
        results = []
        start_time = time.time()
        
        # Health check
        result = self.make_request("/health")
        results.append(result)
        if result.success:
            self.log_success("Health check passed")
        else:
            self.log_error(f"Health check failed: {result.error_message}")
        
        # API stats
        result = self.make_request("/stats")
        results.append(result)
        if result.success:
            self.log_success("API stats endpoint working")
        else:
            self.log_error(f"API stats failed: {result.error_message}")
        
        return TestSuite("Health & Status", results, time.time() - start_time)

    def test_card_endpoints(self) -> TestSuite:
        """Test card-related endpoints"""
        self.log_info("Testing card endpoints...")
        results = []
        start_time = time.time()
        
        # Search cards by name
        test_queries = ["Lightning Bolt", "Black Lotus", "Counterspell", "Sol Ring"]
        for query in test_queries:
            result = self.make_request("/cards/search/name", params={"q": query, "limit": 10})
            results.append(result)
            if result.success:
                self.log_success(f"Card search for '{query}' successful ({result.data_count} results)")
            else:
                self.log_error(f"Card search for '{query}' failed: {result.error_message}")
        
        # Autocomplete
        autocomplete_queries = ["light", "black", "counter", "sol"]
        for query in autocomplete_queries:
            result = self.make_request("/cards/autocomplete", params={"q": query, "limit": 5})
            results.append(result)
            if result.success:
                self.log_success(f"Autocomplete for '{query}' successful")
            else:
                self.log_error(f"Autocomplete for '{query}' failed: {result.error_message}")
        
        # Fuzzy search
        fuzzy_queries = ["lightningbolt", "blak lotus", "counterspel"]
        for query in fuzzy_queries:
            result = self.make_request("/cards/search/fuzzy", params={"q": query, "limit": 5})
            results.append(result)
            if result.success:
                self.log_success(f"Fuzzy search for '{query}' successful")
            else:
                self.log_error(f"Fuzzy search for '{query}' failed: {result.error_message}")
        
        # Get expensive cards
        for min_price in [50, 100, 500]:
            result = self.make_request("/cards/expensive", params={"min_price": min_price, "limit": 10})
            results.append(result)
            if result.success:
                self.log_success(f"Expensive cards (>${min_price}) successful ({result.data_count} results)")
            else:
                self.log_error(f"Expensive cards (>${min_price}) failed: {result.error_message}")
        
        # Get specific cards by UUID
        for uuid in self.sample_uuids[:3]:
            result = self.make_request(f"/cards/{uuid}")
            results.append(result)
            if result.success:
                self.log_success(f"Get card by UUID successful")
            else:
                self.log_error(f"Get card by UUID failed: {result.error_message}")
        
        return TestSuite("Card Endpoints", results, time.time() - start_time)

    def test_deck_endpoints(self) -> TestSuite:
        """Test deck-related endpoints"""
        self.log_info("Testing deck endpoints...")
        results = []
        start_time = time.time()
        
        # Get commander decks
        result = self.make_request("/decks/commanders")
        results.append(result)
        if result.success:
            self.log_success(f"Commander decks successful ({result.data_count} decks)")
        else:
            self.log_error(f"Commander decks failed: {result.error_message}")
        
        # Search decks by name
        deck_queries = ["Commander", "Planeswalker", "Duel"]
        for query in deck_queries:
            result = self.make_request("/decks/search/name", params={"q": query})
            results.append(result)
            if result.success:
                self.log_success(f"Deck search for '{query}' successful ({result.data_count} results)")
            else:
                self.log_error(f"Deck search for '{query}' failed: {result.error_message}")
        
        # Find decks containing specific cards
        card_queries = ["Sol Ring", "Lightning Bolt", "Counterspell"]
        for query in card_queries:
            result = self.make_request("/decks/containing-card", params={"q": query})
            results.append(result)
            if result.success:
                self.log_success(f"Decks containing '{query}' successful ({result.data_count} decks)")
            else:
                self.log_error(f"Decks containing '{query}' failed: {result.error_message}")
        
        # Get expensive decks
        for min_value in [100, 500, 1000]:
            result = self.make_request("/decks/expensive", params={"min_price": min_value})
            results.append(result)
            if result.success:
                self.log_success(f"Expensive decks (>${min_value}) successful ({result.data_count} decks)")
            else:
                self.log_error(f"Expensive decks (>${min_value}) failed: {result.error_message}")
        
        # Get specific decks by UUID and their composition
        for uuid in self.sample_deck_uuids[:3]:
            # Get deck
            result = self.make_request(f"/decks/{uuid}")
            results.append(result)
            if result.success:
                self.log_success(f"Get deck by UUID successful")
            else:
                self.log_error(f"Get deck by UUID failed: {result.error_message}")
            
            # Get deck composition
            result = self.make_request(f"/decks/{uuid}/composition")
            results.append(result)
            if result.success:
                self.log_success(f"Get deck composition successful")
            else:
                self.log_error(f"Get deck composition failed: {result.error_message}")
        
        return TestSuite("Deck Endpoints", results, time.time() - start_time)

    def test_pricing_endpoints(self) -> TestSuite:
        """Test pricing-related endpoints"""
        self.log_info("Testing pricing endpoints...")
        results = []
        start_time = time.time()
        
        # Get trending cards
        for direction in ["up", "down"]:
            result = self.make_request("/pricing/trending", params={"direction": direction, "limit": 10})
            results.append(result)
            if result.success:
                self.log_success(f"Trending cards ({direction}) successful ({result.data_count} cards)")
            else:
                self.log_error(f"Trending cards ({direction}) failed: {result.error_message}")
        
        # Get arbitrage opportunities
        result = self.make_request("/pricing/arbitrage", params={"card_filter": "rare", "min_diff": 10})
        results.append(result)
        if result.success:
            self.log_success(f"Arbitrage opportunities successful ({result.data_count} opportunities)")
        else:
            self.log_error(f"Arbitrage opportunities failed: {result.error_message}")
        
        # Get card prices for sample cards
        for uuid in self.sample_uuids[:3]:
            for condition in ["Near Mint", "Lightly Played"]:
                result = self.make_request(f"/pricing/card/{uuid}", params={"condition": condition})
                results.append(result)
                if result.success:
                    self.log_success(f"Card price for condition '{condition}' successful")
                else:
                    self.log_warning(f"Card price for condition '{condition}' not found (expected)")
        
        return TestSuite("Pricing Endpoints", results, time.time() - start_time)

    def test_set_endpoints(self) -> TestSuite:
        """Test set-related endpoints"""
        self.log_info("Testing set endpoints...")
        results = []
        start_time = time.time()
        
        # Get all sets
        result = self.make_request("/sets")
        results.append(result)
        if result.success:
            self.log_success(f"Get all sets successful ({result.data_count} sets)")
        else:
            self.log_error(f"Get all sets failed: {result.error_message}")
        
        # Get specific sets
        for set_code in self.sample_set_codes[:5]:
            result = self.make_request(f"/sets/{set_code}")
            results.append(result)
            if result.success:
                self.log_success(f"Get set '{set_code}' successful")
            else:
                self.log_error(f"Get set '{set_code}' failed: {result.error_message}")
        
        return TestSuite("Set Endpoints", results, time.time() - start_time)

    def test_analytics_endpoints(self) -> TestSuite:
        """Test analytics endpoints"""
        self.log_info("Testing analytics endpoints...")
        results = []
        start_time = time.time()
        
        # Database statistics
        result = self.make_request("/analytics/database-stats")
        results.append(result)
        if result.success:
            self.log_success("Database statistics successful")
        else:
            self.log_error(f"Database statistics failed: {result.error_message}")
        
        # Memory usage
        result = self.make_request("/analytics/memory-usage")
        results.append(result)
        if result.success:
            self.log_success("Memory usage successful")
        else:
            self.log_error(f"Memory usage failed: {result.error_message}")
        
        return TestSuite("Analytics Endpoints", results, time.time() - start_time)

    def test_error_handling(self) -> TestSuite:
        """Test error handling and edge cases"""
        self.log_info("Testing error handling and edge cases...")
        results = []
        start_time = time.time()
        
        # Test 404 cases
        error_tests = [
            ("/cards/nonexistent-uuid", 404),
            ("/decks/nonexistent-uuid", 404),
            ("/sets/INVALID", 404),
            ("/nonexistent-endpoint", 404),
        ]
        
        for endpoint, expected_status in error_tests:
            result = self.make_request(endpoint, expected_status=expected_status)
            results.append(result)
            if result.success:
                self.log_success(f"Error handling for {endpoint} correct")
            else:
                self.log_error(f"Error handling for {endpoint} failed: expected {expected_status}, got {result.status_code}")
        
        # Test malformed requests
        malformed_tests = [
            ("/cards/search/name", {"q": ""}),  # Empty query
            ("/cards/expensive", {"min_price": "invalid"}),  # Invalid price
            ("/cards/autocomplete", {"limit": "invalid"}),  # Invalid limit
        ]
        
        for endpoint, params in malformed_tests:
            result = self.make_request(endpoint, params=params, expected_status=400)
            results.append(result)
            # Note: Some endpoints might handle these gracefully, so we don't fail the test
            if result.status_code in [200, 400]:
                self.log_success(f"Malformed request handling for {endpoint} acceptable")
            else:
                self.log_warning(f"Malformed request for {endpoint} returned {result.status_code}")
        
        return TestSuite("Error Handling", results, time.time() - start_time)

    def performance_test(self, num_concurrent: int = 10, num_requests: int = 100) -> TestSuite:
        """Run performance tests with concurrent requests"""
        self.log_info(f"Running performance test with {num_concurrent} concurrent users, {num_requests} total requests...")
        results = []
        start_time = time.time()
        
        # Define test endpoints for performance testing
        endpoints = [
            "/health",
            "/cards/search/name?q=Lightning&limit=10",
            "/cards/autocomplete?q=light&limit=5",
            "/decks/commanders",
            "/sets",
        ]
        
        def make_performance_request():
            endpoint = random.choice(endpoints)
            return self.make_request(endpoint)
        
        # Run concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(make_performance_request) for _ in range(num_requests)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.log_error(f"Performance test request failed: {e}")
        
        total_time = time.time() - start_time
        
        # Calculate performance metrics
        if results:
            response_times = [r.response_time for r in results if r.success]
            success_rate = (sum(1 for r in results if r.success) / len(results)) * 100
            
            self.log_info(f"Performance test completed:")
            self.log_info(f"  • Total requests: {len(results)}")
            self.log_info(f"  • Success rate: {success_rate:.1f}%")
            self.log_info(f"  • Total time: {total_time:.2f}s")
            self.log_info(f"  • Requests/second: {len(results)/total_time:.1f}")
            
            if response_times:
                self.log_info(f"  • Avg response time: {statistics.mean(response_times):.3f}s")
                self.log_info(f"  • Min response time: {min(response_times):.3f}s")
                self.log_info(f"  • Max response time: {max(response_times):.3f}s")
                self.log_info(f"  • 95th percentile: {statistics.quantiles(response_times, n=20)[18]:.3f}s")
        
        return TestSuite("Performance Test", results, total_time)

    def update_stats(self, results: List[TestResult]):
        """Update test statistics"""
        for result in results:
            self.stats['total_tests'] += 1
            if result.success:
                self.stats['passed_tests'] += 1
            else:
                self.stats['failed_tests'] += 1
            
            self.stats['total_time'] += result.response_time
            self.stats['fastest_response'] = min(self.stats['fastest_response'], result.response_time)
            self.stats['slowest_response'] = max(self.stats['slowest_response'], result.response_time)
        
        if self.stats['total_tests'] > 0:
            self.stats['avg_response_time'] = self.stats['total_time'] / self.stats['total_tests']

    def print_suite_summary(self, suite: TestSuite):
        """Print summary for a test suite"""
        passed = sum(1 for r in suite.results if r.success)
        failed = len(suite.results) - passed
        success_rate = (passed / len(suite.results)) * 100 if suite.results else 0
        
        color = Colors.GREEN if failed == 0 else Colors.YELLOW if failed < len(suite.results) // 2 else Colors.RED
        
        print(f"\n{Colors.BOLD}{suite.name}{Colors.END}")
        print(f"  {color}✓ {passed} passed, ❌ {failed} failed ({success_rate:.1f}% success rate){Colors.END}")
        print(f"  ⏱️  Total time: {suite.total_time:.2f}s")
        
        if suite.results:
            avg_time = statistics.mean([r.response_time for r in suite.results])
            print(f"  📊 Average response time: {avg_time:.3f}s")

    def print_final_summary(self):
        """Print final test summary"""
        print(f"\n{Colors.BOLD}=== FINAL TEST SUMMARY ==={Colors.END}")
        print(f"Total Tests: {self.stats['total_tests']}")
        print(f"{Colors.GREEN}✓ Passed: {self.stats['passed_tests']}{Colors.END}")
        print(f"{Colors.RED}❌ Failed: {self.stats['failed_tests']}{Colors.END}")
        
        success_rate = (self.stats['passed_tests'] / self.stats['total_tests']) * 100 if self.stats['total_tests'] > 0 else 0
        color = Colors.GREEN if success_rate >= 90 else Colors.YELLOW if success_rate >= 70 else Colors.RED
        print(f"{color}Success Rate: {success_rate:.1f}%{Colors.END}")
        
        print(f"\n{Colors.BOLD}Performance Metrics:{Colors.END}")
        print(f"Total Time: {self.stats['total_time']:.2f}s")
        print(f"Average Response Time: {self.stats['avg_response_time']:.3f}s")
        print(f"Fastest Response: {self.stats['fastest_response']:.3f}s")
        print(f"Slowest Response: {self.stats['slowest_response']:.3f}s")

    def run_all_tests(self, include_performance: bool = True):
        """Run all test suites"""
        self.log(f"{Colors.BOLD}🚀 Starting comprehensive API tests...{Colors.END}")
        self.log_info(f"Target API: {self.base_url}")
        
        # Collect sample data first
        self.collect_sample_data()
        
        # Run all test suites
        test_suites = [
            self.test_health_endpoints(),
            self.test_card_endpoints(),
            self.test_deck_endpoints(),
            self.test_pricing_endpoints(),
            self.test_set_endpoints(),
            self.test_analytics_endpoints(),
            self.test_error_handling(),
        ]
        
        if include_performance:
            test_suites.append(self.performance_test())
        
        # Update statistics and print summaries
        for suite in test_suites:
            self.update_stats(suite.results)
            self.print_suite_summary(suite)
        
        # Print final summary
        self.print_final_summary()
        
        # Return overall success
        return self.stats['failed_tests'] == 0

def main():
    parser = argparse.ArgumentParser(description="Comprehensive API Testing for MTGJSON API")
    parser.add_argument("--url", default="http://localhost:8888", 
                       help="API base URL (default: http://localhost:8888)")
    parser.add_argument("--timeout", type=int, default=30, 
                       help="Request timeout in seconds (default: 30)")
    parser.add_argument("--no-performance", action="store_true", 
                       help="Skip performance tests")
    
    args = parser.parse_args()
    
    tester = MTGAPITester(base_url=args.url, timeout=args.timeout)
    
    try:
        success = tester.run_all_tests(include_performance=not args.no_performance)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Tests interrupted by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}Test runner error: {e}{Colors.END}")
        sys.exit(1)

if __name__ == "__main__":
    main() 