#!/usr/bin/env python3

from io import BytesIO
import pycurl
import json
import csv

def download_tcg_pricing_data(output_file="tcg_pricing_clean.csv"):
    """Download TCGPlayer pricing data and clean up the CSV format"""
    
    print("=== TCGPlayer Pricing Data Downloader ===")
    print("Downloading pricing data from TCGPlayer...")
    
    # Setup curl request
    b_obj = BytesIO()
    crl = pycurl.Curl()
    
    crl.setopt(crl.URL, "https://store.tcgplayer.com/admin/pricing/downloadexportcsv")
    
    headers = [
        "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language: en-US,en;q=0.9",
        "cache-control: max-age=0",
        "content-type: application/x-www-form-urlencoded",
        "origin: https://store.tcgplayer.com",
        "priority: u=0, i",
        "referer: https://store.tcgplayer.com/admin/pricing",
        'sec-ch-ua: "Microsoft Edge";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-mobile: ?0",
        'sec-ch-ua-platform: "Windows"',
        "sec-fetch-dest: document",
        "sec-fetch-mode: navigate",
        "sec-fetch-site: same-origin",
        "sec-fetch-user: ?1",
        "upgrade-insecure-requests: 1",
        "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
    ]
    crl.setopt(crl.HTTPHEADER, headers)
    
    # Note: You'll need to update these cookies with your own session cookies
    cookies = '_sn_m={"r":{"n":0,"r":"tcgplayer"}}; _sn_a={"a":{"s":1737410731349,"e":1737410731349}}; _sn_n={"a":{"i":"c09e8400-48cf-4713-a922-3208350a7f3b"}}; _gcl_au=1.1.239811035.1749691747; tcg-uuid=206f594a-3ca9-4a80-b90c-43a2b12e25b4; tracking-preferences={%22version%22:1%2C%22destinations%22:{%22Actions%20Amplitude%22:true%2C%22AdWords%22:true%2C%22Google%20AdWords%20New%22:true%2C%22Google%20Enhanced%20Conversions%22:true%2C%22Google%20Tag%20Manager%22:true%2C%22Impact%20Partnership%20Cloud%22:true%2C%22Optimizely%22:true}%2C%22custom%22:{%22advertising%22:true%2C%22functional%22:true%2C%22marketingAndAnalytics%22:true}}; product-display-settings=sort=price+shipping&size=10; __ssid=cfed9f7f2d85343013aaaac5426f2c4; QSI_SI_b3liNtCHnCOlcKW_intercept=true; TCG_VisitorKey=16b370a2-9ee4-42ff-ac65-39325fb0dfbe; SellerProximity=ZipCode=&MaxSellerDistance=1000&IsActive=false; _ga_KK8XBGNYRB=GS2.1.s1749873041$o8$g0$t1749874506$j60$l0$h0; OAuthLoginSessionId=34301848-4592-4bca-94f9-fe7972d72d9b; TCGAuthTicket_Production=B0C8EE412FE8005ACC48E44C94FD5B375E7AEEE06358D4FFA4960452D1391B2B72C1C9DF5688FF5A180C0824AA6719BBFF48CD3CA839C34C5B98E632CABBABD0DF54A58C7D5E7476FE2D24A887E14A0E6A882B5C24C13C20EF5623E34B782F78FD3F7C3AB7B0121B775EAAAE2FFCB1E56A203B5D; setting=CD=US&M=1; ASP.NET_SessionId=czlvt1rhnndsprf5k5hzh2d1; LastSeller=19d98323; __RequestVerificationToken_L2FkbWlu0=8IK3OnlaN40CB25tuvd291YjKc_xzsHDDJ_D6Azl2HJwzzx7jDQBgvvr99_KqMOtg2p-l6uFk5QxxbtCUGoyBA4EpMI1; _gid=GA1.2.521990547.1749874534; _drip_client_4160913=vid%253D6c498a45bcb44dc68d022e9e40562efa%2526pageViews%253D17%2526sessionPageCount%253D2%2526lastVisitedAt%253D1749877554627%2526weeklySessionCount%253D6%2526lastSessionAt%253D1749874501850; StoreSaveForLater_PRODUCTION=SFLK=a57a9037a6af4973baca1e559f658ed9&Ignore=false; ajs_user_id=925a1984-8fb7-4a12-8feb-4fad4bc1ca7b; SearchSortSettings=M=1&ProductSortOption=BestMatch&ProductSortDesc=False&PriceSortOption=Shipping&ProductResultDisplay=grid; _ga_VS9BE2Z3GY=GS2.1.s1749877556$o9$g1$t1749877582$j34$l0$h1837068264; tcg-segment-session=1749877555502%257C1749877624280; ajs_anonymous_id=50bb7283-906e-4b77-a8ac-f0b5fa4093a4; analytics_session_id=1749896933091; _ga=GA1.2.1465856315.1749691757; _gat_UA-620217-1=1; _ga_N5CWV2Q5WR=GS2.2.s1749896933$o3$g1$t1749896957$j36$l0$h0; AWSALB=+EVbbA/h+xDD9fS3K7TbuvVSHJS3R4wv46FWZLgmirMmaoTljfXTuwW2cDheYzXvegpM6rkORgSvuD/3lOs0udb4mZN1ZpwTF5D3uwgYxufVX0pVSPhqPIDTtiZu3Vs+nHtt8LfMjXPwEGo0rMu99r8kZ9UrMsKybzLdDPl7LWszSzf3xkVRymoEY10Wuw==; AWSALBCORS=+EVbbA/h+xDD9fS3K7TbuvVSHJS3R4wv46FWZLgmirMmaoTljfXTuwW2cDheYzXvegpM6rkORgSvuD/3lOs0udb4mZN1ZpwTF5D3uwgYxufVX0pVSPhqPIDTtiZu3Vs+nHtt8LfMjXPwEGo0rMu99r8kZ9UrMsKybzLdDPl7LWszSzf3xkVRymoEY10Wuw==; analytics_session_id.last_access=1749896984947'
    
    crl.setopt(crl.COOKIE, cookies)
    
    # Request parameters - you can modify these as needed
    post_data = {
        "PricingType": "Pricing",
        "CategoryId": "1",  # Magic: The Gathering
        "SetNameIds": ["0"],  # All sets
        "ConditionIds": ["1","6"],  # Near Mint, Unopened
        "RarityIds": ["0"],  # All rarities
        "LanguageIds": ["1"],  # English
        "PrintingIds": ["0"],  # All printings
        "CompareAgainstPrice": False,
        "PriceToCompare": 3,
        "ValueToCompare": 1,
        "PriceValueToCompare": None,
        "MyInventory": False,
        "ExcludeListos": False,
        "ExportLowestListingNotMe": False,
    }
    
    post_data_str = f"model={json.dumps(post_data)}"
    crl.setopt(crl.POSTFIELDS, post_data_str)
    crl.setopt(crl.WRITEFUNCTION, b_obj.write)
    
    try:
        print("Sending request to TCGPlayer...")
        crl.perform()
        response_code = crl.getinfo(crl.RESPONSE_CODE)
        
        if response_code != 200:
            print(f"✗ Request failed with status code: {response_code}")
            return False
            
        print("✓ Data downloaded successfully")
        
    except Exception as e:
        print(f"✗ Error downloading data: {e}")
        return False
    finally:
        crl.close()
    
    # Process and clean the CSV data
    raw_response = b_obj.getvalue().decode("utf-8")
    b_obj.close()
    
    print("Cleaning CSV data (removing empty rows)...")
    
    # Split into lines and filter out empty lines
    lines = raw_response.split('\n')
    clean_lines = [line.strip() for line in lines if line.strip()]
    
    print(f"✓ Removed {len(lines) - len(clean_lines)} empty rows")
    print(f"✓ Clean data has {len(clean_lines)} rows")
    
    # Write cleaned CSV
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        f.write('\n'.join(clean_lines))
    
    print(f"✓ Clean CSV saved to: {output_file}")
    
    # Analyze the data structure
    analyze_csv_structure(output_file)
    
    return True

def analyze_csv_structure(csv_file):
    """Analyze the structure of the cleaned CSV file"""
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Read first few lines to understand structure
            reader = csv.reader(f)
            lines = []
            for i, row in enumerate(reader):
                lines.append(row)
                if i >= 10:  # Just analyze first 10 rows
                    break
        
        if lines:
            print("\n=== CSV Structure Analysis ===")
            print(f"Header row: {lines[0]}")
            print(f"Columns: {len(lines[0])}")
            
            if len(lines) > 1:
                print(f"Sample data row: {lines[1]}")
                
                # Look for key columns
                header = [col.lower() for col in lines[0]]
                key_columns = {}
                
                for i, col in enumerate(header):
                    if 'name' in col or 'title' in col:
                        key_columns['card_name'] = i
                    elif 'tcg' in col and ('id' in col or 'product' in col):
                        key_columns['tcgplayer_id'] = i
                    elif 'price' in col:
                        key_columns['price'] = i
                    elif 'set' in col:
                        key_columns['set'] = i
                
                print(f"Key columns identified: {key_columns}")
                
        print(f"Total rows (including header): {len(lines)}")
        
    except Exception as e:
        print(f"Error analyzing CSV: {e}")

def create_price_update_script(csv_file="tcg_pricing_clean.csv"):
    """Create a script to update Redis with TCGPlayer pricing data"""
    
    script_content = f'''#!/usr/bin/env python3
"""
TCGPlayer Price Update Script
Updates Redis with current TCGPlayer pricing data
"""

import redis
import csv
import os
import json
from datetime import datetime

def update_redis_prices(csv_file="{csv_file}"):
    """Update Redis with TCGPlayer pricing data"""
    
    # Connect to Redis
    redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
    redis_port = int(os.getenv('REDIS_PORT', '9999'))
    
    try:
        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        client.ping()
        print(f"✓ Connected to Redis at {{redis_host}}:{{redis_port}}")
    except:
        print(f"✗ Could not connect to Redis at {{redis_host}}:{{redis_port}}")
        return False
    
    if not os.path.exists(csv_file):
        print(f"✗ CSV file not found: {{csv_file}}")
        return False
    
    print(f"Reading pricing data from {{csv_file}}...")
    
    updated_count = 0
    error_count = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Extract key fields (you'll need to adjust column names based on actual CSV structure)
                tcgplayer_id = row.get('TCGPlayer ID') or row.get('Product ID')
                card_name = row.get('Product Name') or row.get('Card Name')
                price = row.get('Price') or row.get('Market Price')
                set_name = row.get('Set Name') or row.get('Set')
                
                if tcgplayer_id and price:
                    # Try to find the card by TCGPlayer ID
                    oracle_id = client.get(f"tcg:{{tcgplayer_id}}")
                    
                    if oracle_id:
                        # Update the price
                        try:
                            price_value = float(price.replace('$', '').replace(',', ''))
                            client.set(f"price:tcg:{{oracle_id}}", price_value)
                            
                            # Update latest price if this is higher
                            current_latest = client.get(f"price:latest:{{oracle_id}}")
                            if not current_latest or price_value > float(current_latest):
                                client.set(f"price:latest:{{oracle_id}}", price_value)
                            
                            updated_count += 1
                            
                            if updated_count % 1000 == 0:
                                print(f"Updated {{updated_count}} prices...")
                                
                        except ValueError:
                            error_count += 1
                    
            except Exception as e:
                error_count += 1
                if error_count <= 10:  # Only show first 10 errors
                    print(f"Error processing row: {{e}}")
    
    # Store update timestamp
    client.set("tcg:last_price_update", datetime.now().isoformat())
    
    print(f"\\n=== TCGPlayer Price Update Complete ===")
    print(f"✓ Updated prices: {{updated_count}}")
    print(f"✗ Errors: {{error_count}}")
    
    return True

if __name__ == "__main__":
    update_redis_prices()
'''
    
    with open("update_tcg_prices.py", "w", encoding="utf-8") as f:
        f.write(script_content)
    
    print("✓ Created price update script: update_tcg_prices.py")

if __name__ == "__main__":
    import sys
    
    output_file = sys.argv[1] if len(sys.argv) > 1 else "tcg_pricing_clean.csv"
    
    if download_tcg_pricing_data(output_file):
        print("\\n" + "="*50)
        create_price_update_script(output_file)
        
        print("\\n=== Next Steps ===")
        print("1. Review the cleaned CSV file")
        print("2. Update column names in update_tcg_prices.py if needed")
        print("3. Run: python update_tcg_prices.py")
        print("4. Check updated prices with your Lua scripts")
        
    else:
        print("\\n✗ Download failed. Check your cookies and try again.") 