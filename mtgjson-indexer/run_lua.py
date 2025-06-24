#!/usr/bin/env python3

import redis
import os
import sys
import glob
import json
from datetime import datetime

def find_lua_scripts(directory="."):
    """Find all .lua files in the specified directory"""
    pattern = os.path.join(directory, "**/*.lua")
    return glob.glob(pattern)

def save_results_to_redis(client, script_name, results, ttl_seconds=3600):
    """Save script results to Redis with metadata"""
    try:
        # Create unique key with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_key = f"script_results:{script_name}:{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Prepare metadata
        metadata = {
            'script': script_name,
            'timestamp': timestamp,
            'total_lines': len(results)
        }
        
        # Use a hash to store all the data
        pipe = client.pipeline()
        
        # Store metadata
        pipe.hset(result_key, "metadata", json.dumps(metadata))
        
        # Store each line with a numbered key
        for i, line in enumerate(results):
            pipe.hset(result_key, f"line_{i}", str(line))
        
        # Set expiration
        pipe.expire(result_key, ttl_seconds)
        
        # Execute all commands
        pipe.execute()
        
        print(f"✓ Results saved to Redis (key: {result_key})")
        print(f"✓ Results will expire in {ttl_seconds//3600} hour(s)")
        print("✓ Use 'python result_viewer.py' to browse saved results")
        
        return result_key
        
    except Exception as e:
        print(f"✗ Error saving results to Redis: {e}")
        return None

def select_lua_script():
    """Let user select a Lua script from available options"""
    scripts = find_lua_scripts()
    
    if not scripts:
        print("✗ No .lua files found in current directory")
        return None
    
    print("Available Lua scripts:")
    for i, script in enumerate(scripts, 1):
        script_name = os.path.basename(script)
        print(f"  {i}. {script_name}")
    
    while True:
        try:
            choice = input(f"\nSelect script (1-{len(scripts)}) or 'q' to quit: ").strip()
            if choice.lower() == 'q':
                return None
            
            idx = int(choice) - 1
            if 0 <= idx < len(scripts):
                return scripts[idx]
            else:
                print(f"Please enter a number between 1 and {len(scripts)}")
        except ValueError:
            print("Please enter a valid number or 'q' to quit")
        except KeyboardInterrupt:
            print("\nCancelled by user")
            return None

def run_lua_script(script_path=None, script_args=None):
    # Connect to Redis
    redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
    redis_port = int(os.getenv('REDIS_PORT', '9999'))
    
    try:
        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Test connection
        client.ping()
        print(f"✓ Connected to Redis at {redis_host}:{redis_port}")
        
        # Determine which script to run
        if script_path is None:
            script_path = select_lua_script()
            if script_path is None:
                print("No script selected. Exiting.")
                return
        
        if not os.path.exists(script_path):
            print(f"✗ Script file not found: {script_path}")
            return
        
        # Read the Lua script
        with open(script_path, 'r') as f:
            lua_script = f.read()
        
        script_name = os.path.basename(script_path)
        print(f"Executing Lua script: {script_name}")
        
        # Prepare arguments for the script
        if script_args is None:
            script_args = []
        
        # Special handling for search scripts that need to be loaded persistently
        if ('search' in script_name.lower() or 'fuzzy' in script_name.lower()) and 'deck_search' not in script_name.lower():
            # Load the script and store its SHA for future use
            script_sha = client.script_load(lua_script)
            client.set("mtg:script:fuzzy_search", script_sha)
            print(f"✓ Search script loaded and stored with SHA: {script_sha}")
            
            # Test the search script
            if 'improved' in script_name.lower():
                print("\nTesting improved search...")
                test_queries = ["Wrath of God", "Lightning Bolt"]
                
                for query in test_queries:
                    print(f"\nSearching for: '{query}'")
                    oracle_ids = client.evalsha(script_sha, 0, query, 2, 3)
                    print(f"Found {len(oracle_ids)} results:")
                    
                    for i, oracle_id in enumerate(oracle_ids, 1):
                        card_data = client.get(f"card:oracle:{oracle_id}")
                        if card_data:
                            import json
                            card = json.loads(card_data)
                            sets_str = ', '.join(card['sets'][:3])
                            print(f"  {i}. {card['name']:<25} [{sets_str}]")
        else:
            # Execute the script normally with arguments
            print(f"Script arguments: {script_args}")
            result = client.eval(lua_script, 0, *script_args)
            print("✓ Script executed successfully!")
            
            if isinstance(result, int):
                print(f"✓ Processed {result} items")
            elif isinstance(result, list):
                print(f"✓ Returned {len(result)} results")
                
                # Save results to Redis
                result_key = save_results_to_redis(client, script_name, result)
                
                if result_key and len(result) > 0:
                    print()
                    # Give user options
                    if len(result) <= 50:  # Show small results immediately
                        print("Results:")
                        print("-" * 60)
                        for i, line in enumerate(result, 1):
                            print(f"{i:3d}: {line}")
                        print("-" * 60)
                    else:
                        print(f"Large result set ({len(result)} lines) saved for pagination.")
                    
                    print("\nOptions:")
                    print("  v: View results with pagination")
                    print("  s: Show first 20 lines now") 
                    print("  Enter: Continue")
                    
                    try:
                        choice = input("Choice: ").strip().lower()
                        if choice == 'v':
                            # Import and launch result viewer
                            try:
                                from result_viewer import ResultViewer
                                viewer = ResultViewer()
                                viewer.view_results(result_key)
                            except ImportError:
                                print("Result viewer not available. Use 'python result_viewer.py'")
                        elif choice == 's':
                            print("\nFirst 20 lines:")
                            print("-" * 60)
                            for i, line in enumerate(result[:20], 1):
                                print(f"{i:3d}: {line}")
                            if len(result) > 20:
                                print(f"... and {len(result) - 20} more lines (use 'python result_viewer.py' to see all)")
                            print("-" * 60)
                    except (KeyboardInterrupt, EOFError):
                        pass
                        
            else:
                print(f"✓ Result: {result}")
        
    except redis.ConnectionError:
        print(f"✗ Could not connect to Redis at {redis_host}:{redis_port}")
        print("Make sure Redis is running (e.g., in Docker)")
    except FileNotFoundError as e:
        print(f"✗ File not found: {e}")
    except Exception as e:
        print(f"✗ Error executing script: {e}")

if __name__ == "__main__":
    # Check if a script path was provided as argument
    if len(sys.argv) > 1:
        if sys.argv[1] == "--view" or sys.argv[1] == "-v":
            # Launch result viewer
            try:
                from result_viewer import ResultViewer
                viewer = ResultViewer()
                viewer.interactive_browser()
            except ImportError:
                print("Result viewer not available. Make sure result_viewer.py exists.")
        else:
            script_path = sys.argv[1]
            # Pass any additional arguments to the script
            script_args = sys.argv[2:] if len(sys.argv) > 2 else []
            run_lua_script(script_path, script_args)
    else:
        run_lua_script() 