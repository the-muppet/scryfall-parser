#!/usr/bin/env python3
"""
Redis Key Pattern Analyzer
Analyzes all keys in Redis database and provides comprehensive statistics
"""

import redis
import json
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Set, Any
import argparse
import time

class RedisKeyAnalyzer:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            password=password,
            decode_responses=True
        )
        self.patterns = defaultdict(list)
        self.type_counts = defaultdict(int)
        self.memory_usage = {}
        self.ttl_info = {}
        
    def analyze_keys(self, sample_size=10):
        """Analyze all keys in Redis database"""
        print("🔍 Scanning Redis database...")
        start_time = time.time()
        
        # Get all keys
        all_keys = []
        cursor = 0
        total_keys = 0
        
        while True:
            cursor, keys = self.redis_client.scan(cursor=cursor, count=1000)
            all_keys.extend(keys)
            total_keys += len(keys)
            if cursor == 0:
                break
                
        print(f"📊 Found {total_keys:,} total keys in {time.time() - start_time:.2f}s")
        
        # Group keys by patterns
        pattern_groups = defaultdict(list)
        
        for key in all_keys:
            pattern = self._extract_pattern(key)
            pattern_groups[pattern].append(key)
            
        # Analyze each pattern
        results = {}
        for pattern, keys in pattern_groups.items():
            print(f"🔑 Analyzing pattern: {pattern} ({len(keys)} keys)")
            results[pattern] = self._analyze_pattern(pattern, keys, sample_size)
            
        return results
    
    def _extract_pattern(self, key: str) -> str:
        """Extract pattern from key"""
        parts = key.split(':')
        
        # Handle various MTGJSON patterns
        if len(parts) >= 2:
            # UUID patterns (like card:uuid or mtg:cards:data:uuid)
            if self._is_uuid(parts[-1]):
                return ':'.join(parts[:-1]) + ':{uuid}'
            # SKU ID patterns  
            elif parts[-1].isdigit() and len(parts[-1]) > 6:
                return ':'.join(parts[:-1]) + ':{sku_id}'
            # Set code patterns (3-4 char codes)
            elif len(parts[-1]) in [3, 4] and parts[-1].isupper():
                return ':'.join(parts[:-1]) + ':{set_code}'
            # Generic ID patterns
            elif parts[-1].replace('_', '').replace('-', '').isalnum():
                return ':'.join(parts[:-1]) + ':{id}'
                
        return key
    
    def _is_uuid(self, s: str) -> bool:
        """Check if string looks like a UUID"""
        if len(s) == 36 and s.count('-') == 4:
            parts = s.split('-')
            return (len(parts) == 5 and 
                   len(parts[0]) == 8 and len(parts[1]) == 4 and 
                   len(parts[2]) == 4 and len(parts[3]) == 4 and 
                   len(parts[4]) == 12)
        return False
    
    def _analyze_pattern(self, pattern: str, keys: List[str], sample_size: int) -> Dict[str, Any]:
        """Analyze a specific key pattern"""
        if not keys:
            return {}
            
        # Sample keys for analysis
        sample_keys = keys[:sample_size] if len(keys) > sample_size else keys
        
        # Get types and sample data
        types = Counter()
        sample_data = []
        total_memory = 0
        ttl_info = {'with_ttl': 0, 'no_ttl': 0, 'expired': 0}
        
        for key in sample_keys:
            try:
                # Get type
                key_type = self.redis_client.type(key)
                types[key_type] += 1
                
                # Get memory usage (if available)
                try:
                    memory = self.redis_client.memory_usage(key)
                    total_memory += memory or 0
                except:
                    memory = None
                
                # Get TTL
                ttl = self.redis_client.ttl(key)
                if ttl == -1:
                    ttl_info['no_ttl'] += 1
                elif ttl == -2:
                    ttl_info['expired'] += 1
                else:
                    ttl_info['with_ttl'] += 1
                
                # Get sample data based on type
                sample_value = self._get_sample_value(key, key_type)
                
                sample_data.append({
                    'key': key,
                    'type': key_type,
                    'memory_bytes': memory,
                    'ttl': ttl,
                    'sample_value': sample_value
                })
                
            except Exception as e:
                print(f"⚠️ Error analyzing key {key}: {e}")
                continue
        
        return {
            'pattern': pattern,
            'total_keys': len(keys),
            'sample_size': len(sample_data),
            'types': dict(types),
            'total_memory_bytes': total_memory,
            'avg_memory_bytes': total_memory / len(sample_data) if sample_data else 0,
            'ttl_info': ttl_info,
            'sample_keys': [k['key'] for k in sample_data[:5]],
            'sample_data': sample_data[:3]  # First 3 for detailed view
        }
    
    def _get_sample_value(self, key: str, key_type: str) -> Any:
        """Get sample value based on Redis data type"""
        try:
            if key_type == 'string':
                value = self.redis_client.get(key)
                # Try to parse as JSON
                try:
                    return {'type': 'json', 'preview': json.loads(value)[:200] if isinstance(json.loads(value), str) else str(json.loads(value))[:200]}
                except:
                    return {'type': 'string', 'preview': str(value)[:200]}
                    
            elif key_type == 'hash':
                fields = self.redis_client.hgetall(key)
                return {'type': 'hash', 'field_count': len(fields), 'sample_fields': dict(list(fields.items())[:3])}
                
            elif key_type == 'list':
                length = self.redis_client.llen(key)
                sample = self.redis_client.lrange(key, 0, 2)
                return {'type': 'list', 'length': length, 'sample_items': sample}
                
            elif key_type == 'set':
                size = self.redis_client.scard(key)
                sample = list(self.redis_client.sscan_iter(key, count=3))[:3]
                return {'type': 'set', 'size': size, 'sample_members': sample}
                
            elif key_type == 'zset':
                size = self.redis_client.zcard(key)
                sample = self.redis_client.zrange(key, 0, 2, withscores=True)
                return {'type': 'zset', 'size': size, 'sample_members': sample}
                
            elif key_type == 'stream':
                length = self.redis_client.xlen(key)
                return {'type': 'stream', 'length': length}
                
            else:
                return {'type': key_type, 'preview': 'Unknown type'}
                
        except Exception as e:
            return {'type': key_type, 'error': str(e)}

def print_analysis_report(analysis: Dict[str, Any]):
    """Print formatted analysis report"""
    print("\n" + "="*80)
    print("🎯 REDIS KEY PATTERN ANALYSIS REPORT")
    print("="*80)
    
    # Summary
    total_keys = sum(data['total_keys'] for data in analysis.values())
    total_patterns = len(analysis)
    total_memory = sum(data['total_memory_bytes'] for data in analysis.values())
    
    print(f"\n📊 SUMMARY:")
    print(f"   • Total Keys: {total_keys:,}")
    print(f"   • Unique Patterns: {total_patterns}")
    print(f"   • Total Memory: {total_memory:,} bytes ({total_memory / (1024*1024):.2f} MB)")
    
    # Sort patterns by key count
    sorted_patterns = sorted(analysis.items(), key=lambda x: x[1]['total_keys'], reverse=True)
    
    print(f"\n🔑 KEY PATTERNS (sorted by count):")
    print("-" * 80)
    
    for pattern, data in sorted_patterns:
        print(f"\n📋 Pattern: {pattern}")
        print(f"   📊 Count: {data['total_keys']:,} keys")
        print(f"   🗂️  Types: {data['types']}")
        
        if data['avg_memory_bytes'] > 0:
            print(f"   💾 Memory: {data['total_memory_bytes']:,} bytes (avg: {data['avg_memory_bytes']:.1f} bytes/key)")
        
        # TTL info
        ttl = data['ttl_info']
        if ttl['with_ttl'] > 0:
            print(f"   ⏰ TTL: {ttl['with_ttl']} with TTL, {ttl['no_ttl']} permanent, {ttl['expired']} expired")
        
        # Sample keys
        print(f"   🔍 Sample keys:")
        for sample_key in data['sample_keys'][:3]:
            print(f"      • {sample_key}")
        
        # Sample data
        if data['sample_data']:
            print(f"   📄 Sample data:")
            for sample in data['sample_data'][:2]:
                print(f"      • {sample['key']} ({sample['type']})")
                if isinstance(sample['sample_value'], dict):
                    if 'preview' in sample['sample_value']:
                        preview = str(sample['sample_value']['preview'])[:100]
                        print(f"        → {preview}{'...' if len(preview) >= 100 else ''}")
                    elif 'sample_fields' in sample['sample_value']:
                        print(f"        → {sample['sample_value']['sample_fields']}")
                    elif 'sample_members' in sample['sample_value']:
                        print(f"        → {sample['sample_value']['sample_members']}")

def print_redisearch_info(redis_client):
    """Print RediSearch index information if available"""
    print("\n" + "="*80)
    print("🔎 REDISEARCH INDEX INFORMATION")
    print("="*80)
    
    # Try to get RediSearch indexes
    try:
        # List all indexes
        indexes = redis_client.execute_command("FT._LIST")
        
        if indexes:
            print(f"\n📚 Found {len(indexes)} RediSearch indexes:")
            
            for index in indexes:
                print(f"\n🗂️ Index: {index}")
                try:
                    # Get index info
                    info = redis_client.execute_command("FT.INFO", index)
                    
                    # Parse info (it's a flat list of key-value pairs)
                    info_dict = {}
                    for i in range(0, len(info), 2):
                        if i + 1 < len(info):
                            info_dict[info[i]] = info[i + 1]
                    
                    print(f"   📊 Documents: {info_dict.get('num_docs', 'N/A')}")
                    print(f"   📏 Index size: {info_dict.get('inverted_sz_mb', 'N/A')} MB")
                    print(f"   🔍 Fields: {len(info_dict.get('attributes', []))}")
                    
                    # Show fields
                    if 'attributes' in info_dict:
                        print(f"   📋 Field types:")
                        for attr in info_dict['attributes'][:5]:  # Show first 5 fields
                            if isinstance(attr, list) and len(attr) >= 2:
                                print(f"      • {attr[1]} ({attr[3] if len(attr) > 3 else 'unknown'})")
                    
                except Exception as e:
                    print(f"   ⚠️ Error getting info: {e}")
        else:
            print("\n❌ No RediSearch indexes found")
            
    except Exception as e:
        print(f"\n❌ RediSearch not available: {e}")

def main():
    parser = argparse.ArgumentParser(description='Analyze Redis key patterns and structure')
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', default=6379, type=int, help='Redis port (default: 6379)')
    parser.add_argument('--db', default=0, type=int, help='Redis database (default: 0)')
    parser.add_argument('--password', help='Redis password')
    parser.add_argument('--sample-size', default=10, type=int, help='Sample size per pattern (default: 10)')
    parser.add_argument('--format', choices=['detailed', 'summary'], default='detailed', help='Output format')
    
    args = parser.parse_args()
    
    try:
        # Connect to Redis
        print(f"🔌 Connecting to Redis at {args.host}:{args.port} (db {args.db})")
        analyzer = RedisKeyAnalyzer(
            host=args.host,
            port=args.port, 
            db=args.db,
            password=args.password
        )
        
        # Test connection
        analyzer.redis_client.ping()
        print("✅ Connected successfully!")
        
        # Analyze keys
        analysis = analyzer.analyze_keys(sample_size=args.sample_size)
        
        # Print results
        if args.format == 'detailed':
            print_analysis_report(analysis)
            print_redisearch_info(analyzer.redis_client)
        else:
            # Summary format
            total_keys = sum(data['total_keys'] for data in analysis.values())
            print(f"\nSUMMARY: {len(analysis)} patterns, {total_keys:,} total keys")
            for pattern, data in sorted(analysis.items(), key=lambda x: x[1]['total_keys'], reverse=True):
                print(f"  {data['total_keys']:>8,} keys: {pattern}")
        
        # Export JSON if requested
        if '--export' in sys.argv:
            with open('redis_analysis.json', 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            print(f"\n💾 Analysis exported to redis_analysis.json")
            
    except redis.ConnectionError:
        print(f"❌ Failed to connect to Redis at {args.host}:{args.port}")
        print("   Make sure Redis is running and accessible")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()