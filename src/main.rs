use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use redis::{Client, Commands, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use chrono::Utc;

const BATCH_SIZE: usize = 2000;     
const CHUNK_SIZE: usize = 10000;
const MAX_PREFIX_LENGTH: usize = 12;

#[derive(Deserialize, Debug, Clone)]
struct ScryfallCard {
    id: String,
    #[serde(default)]
    oracle_id: Option<String>,
    name: String,
    #[serde(default)]
    layout: String,
    set: String,
    set_name: String,
    collector_number: String,
    #[serde(default)]
    tcgplayer_id: Option<i64>,
    #[serde(default)]
    prices: Option<Prices>,
    #[serde(default)]
    image_uris: Option<ImageUris>,
    #[serde(default)]
    card_faces: Option<Vec<CardFace>>,
    #[serde(default)]
    released_at: Option<String>,
    #[serde(default)]
    rarity: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
struct CardFace {
    name: String,
    #[serde(default)]
    image_uris: Option<ImageUris>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
struct ImageUris {
    #[serde(default)]
    small: String,
    #[serde(default)]
    normal: String,
    #[serde(default)]
    large: String,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
struct Prices {
    #[serde(default)]
    usd: Option<String>,
    #[serde(default)]
    usd_foil: Option<String>,
    #[serde(default)]
    eur: Option<String>,
}

#[derive(Serialize, Debug)]
struct IndexedCard {
    id: String,
    oracle_id: String,
    name: String,
    sets: Vec<String>,      
    layout: String,
    tcgplayer_ids: Vec<i64>,
    main_image: Option<String>,
    prices: Vec<PrintingPrice>,
}

#[derive(Serialize, Debug)]
struct PrintingPrice {
    set: String, 
    set_name: Option<String>,
    collector_number: String,
    tcgplayer_id: Option<i64>,
    prices: Prices,
    released_at: Option<String>,
    rarity: Option<String>,
}

#[derive(Serialize, Debug)]
struct PrintingInfo {
    id: String,
    set: String,
    set_name: String,
    collector_number: String,
    tcgplayer_id: Option<i64>,
    prices: Option<Prices>,
    image_uris: Option<ImageUris>,
    released_at: Option<String>,
    rarity: Option<String>,
}

fn download_scryfall_data() -> Result<Vec<ScryfallCard>, Box<dyn std::error::Error>> {
    println!("Downloading Scryfall default_cards.json (this may take a while)...");

    let client = reqwest::blocking::Client::builder()
        .user_agent("MTGPriceAnalyzer/2.0")
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    let bulk_data_url = "https://api.scryfall.com/bulk-data";
    println!("Fetching metadata from: {}", bulk_data_url);

    let response = client
        .get(bulk_data_url)
        .header("Accept", "application/json")
        .send()?;

    if !response.status().is_success() {
        return Err(format!("Failed to get bulk data: HTTP {}", response.status()).into());
    }

    let bulk_data: serde_json::Value = response.json()?;

    if !bulk_data.is_object() {
        return Err("API response is not a JSON object".into());
    }

    let data_array = bulk_data
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or("'data' field not found or is not an array")?;

    println!("Found {} bulk data entries", data_array.len());

    let default_cards_entry = data_array
        .iter()
        .find(|item| item.get("type").and_then(|t| t.as_str()) == Some("default_cards"))
        .ok_or("default_cards entry not found")?;

    let download_uri = default_cards_entry
        .get("download_uri")
        .and_then(|u| u.as_str())
        .ok_or("download_uri field not found or not a string")?;

    println!("Found download URI: {}", download_uri);
    println!("Downloading card data (~500MB compressed, might take several minutes)...");

    let download_start = Instant::now();
    
    let cards_response = client
        .get(download_uri)
        .header("Accept", "application/json")
        .send()?;

    if !cards_response.status().is_success() {
        return Err(format!("Failed to download cards: HTTP {}", cards_response.status()).into());
    }

    println!("Download complete, parsing JSON (this might take a while)...");
    
    let body = cards_response.text()?;
    let cards: Vec<ScryfallCard> = serde_json::from_str(&body)?;

    let elapsed = download_start.elapsed();
    println!(
        "Download and parsing completed in {:.2} seconds",
        elapsed.as_secs_f32()
    );
    println!("Downloaded {} cards", cards.len());

    Ok(cards)
}

fn build_card_index(
    cards: &[ScryfallCard],
) -> Result<(HashMap<String, IndexedCard>, HashSet<String>), Box<dyn std::error::Error>> {
    println!("Building card index in parallel...");
    let start_time = Instant::now();
    
    let pb = ProgressBar::new(cards.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards ({eta})")?
        .progress_chars("#>-"));
    
    let skipped_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    let oracle_map = Arc::new(Mutex::new(HashMap::new()));
    let set_codes = Arc::new(Mutex::new(HashSet::new()));
    
    cards.par_chunks(CHUNK_SIZE)
        .for_each(|chunk| {
            let mut local_oracle_map: HashMap<String, IndexedCard> = HashMap::new();
            let mut local_set_codes = HashSet::new();
            let mut local_skipped = 0;
            
            for card in chunk {
                if card.oracle_id.is_none() {
                    local_skipped += 1;
                    continue;
                }
                
                let oracle_id = card.oracle_id.as_ref().unwrap().clone();
                let card_name = card.name.clone();
                local_set_codes.insert(card.set.clone());
                
                let printing_price = PrintingPrice {
                    set: card.set.clone(),
                    set_name: Some(card.set_name.clone()),
                    collector_number: card.collector_number.clone(),
                    tcgplayer_id: card.tcgplayer_id,
                    prices: card.prices.clone().unwrap_or_default(),
                    released_at: card.released_at.clone(),
                    rarity: card.rarity.clone(),
                };
                
                let indexed_card = local_oracle_map.entry(oracle_id.clone()).or_insert_with(|| {
                    let main_image = card
                        .image_uris
                        .as_ref()
                        .map(|uris| uris.normal.clone())
                        .or_else(|| {
                            card.card_faces.as_ref().and_then(|faces| {
                                faces.get(0).and_then(|face| {
                                    face.image_uris.as_ref().map(|uris| uris.normal.clone())
                                })
                            })
                        });
                    
                    IndexedCard {
                        id: card.id.clone(),
                        oracle_id: oracle_id.clone(),
                        name: card_name,
                        sets: Vec::new(),
                        layout: card.layout.clone(),
                        tcgplayer_ids: Vec::new(),
                        main_image,
                        prices: Vec::new(),
                    }
                });
                
                if !indexed_card.sets.contains(&card.set) {
                    indexed_card.sets.push(card.set.clone());
                }
                
                if let Some(tcgplayer_id) = card.tcgplayer_id {
                    if !indexed_card.tcgplayer_ids.contains(&tcgplayer_id) {
                        indexed_card.tcgplayer_ids.push(tcgplayer_id);
                    }
                }
                
                indexed_card.prices.push(printing_price);
            }
            
            let mut oracle_map_lock = oracle_map.lock().unwrap();
            for (k, v) in local_oracle_map {
                oracle_map_lock.entry(k).or_insert(v);
            }
            
            let mut set_codes_lock = set_codes.lock().unwrap();
            for set_code in local_set_codes {
                set_codes_lock.insert(set_code);
            }
            
            skipped_count.fetch_add(local_skipped, std::sync::atomic::Ordering::Relaxed);
            
            pb.inc(chunk.len() as u64);
        });
    
    let skipped = skipped_count.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "Skipped {} cards without oracle_id (tokens, emblems, etc.)",
        skipped
    );
    
    let elapsed = start_time.elapsed();
    println!(
        "Index building completed in {:.2} seconds",
        elapsed.as_secs_f32()
    );
    
    let oracle_map_result = Arc::try_unwrap(oracle_map)
        .map_err(|_| "Failed to unwrap oracle_map")?
        .into_inner()
        .map_err(|e| format!("Failed to unwrap oracle_map mutex: {:?}", e))?;
    
    let set_codes_result = Arc::try_unwrap(set_codes)
        .map_err(|_| "Failed to unwrap set_codes")?
        .into_inner()
        .map_err(|e| format!("Failed to unwrap set_codes mutex: {:?}", e))?;
    
    pb.finish_with_message(format!("Card indexing completed: {} unique cards", oracle_map_result.len()));
    
    Ok((oracle_map_result, set_codes_result))
}

fn store_card_index(
    con: &mut Connection,
    oracle_id_map: HashMap<String, IndexedCard>, 
    all_set_codes: HashSet<String>,
    cards: &[ScryfallCard],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Storing {} unique cards in Redis", oracle_id_map.len());
    
    let mp = MultiProgress::new();
    let overall_pb = mp.add(ProgressBar::new(oracle_id_map.len() as u64));
    overall_pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards ({eta})")?
        .progress_chars("#>-"));
    
    let mut card_id_map: HashMap<&str, &ScryfallCard> = HashMap::new();
    for card in cards {
        if card.oracle_id.is_some() {
            card_id_map.insert(&card.id, card);
        }
    }

    let oracle_map_len = oracle_id_map.len();
    
    let entries: Vec<(String, IndexedCard)> = oracle_id_map.into_iter().collect();
    
    for (i, batch) in entries.chunks(BATCH_SIZE).enumerate() {
        let batch_pb = mp.add(ProgressBar::new(batch.len() as u64));
        batch_pb.set_style(ProgressStyle::default_bar()
            .template(&format!("Batch #{} {{bar:30.blue}} {{pos}}/{{len}}", i + 1))?
            .progress_chars("=> "));
        
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for (oracle_id, card) in batch {
            let card_json = serde_json::to_string(&card)?;
            pipe.cmd("SET").arg(format!("card:oracle:{}", oracle_id)).arg(&card_json);
            
            pipe.cmd("SET").arg(format!("card:name:{}", card.name.to_lowercase())).arg(oracle_id);
            
            let name_lower = card.name.to_lowercase();
            let chars: Vec<char> = name_lower.chars().collect();
            let prefix_len = std::cmp::min(chars.len(), MAX_PREFIX_LENGTH);
            
            for i in 1..=prefix_len {
                let prefix: String = chars[0..i].iter().collect();
                pipe.cmd("SADD")
                    .arg(format!("auto:prefix:{}", prefix))
                    .arg(oracle_id);
            }
            
            for set_code in &card.sets {
                pipe.cmd("SADD")
                    .arg(format!("set:{}", set_code))
                    .arg(oracle_id);
            }
            
            for tcgplayer_id in &card.tcgplayer_ids {
                pipe.cmd("SET").arg(format!("tcg:{}", tcgplayer_id)).arg(oracle_id);
            }
            
            for price_data in &card.prices {
                if let Some(usd_price) = &price_data.prices.usd {
                    if let Ok(price_value) = usd_price.parse::<f32>() {
                        if price_value > 0.0 {
                            let price_bucket = (price_value * 100.0).round() as i32;
                            pipe.cmd("ZADD")
                                .arg("prices:usd")
                                .arg(price_bucket)
                                .arg(oracle_id);
                        }
                    }
                }
            }
            
            let latest_price = card.prices.iter()
                .filter_map(|p| p.prices.usd.as_ref().and_then(|price| price.parse::<f32>().ok()))
                .fold(0.0f32, |a, b| a.max(b));
                
            if latest_price > 0.0 {
                pipe.cmd("SET").arg(format!("price:latest:{}", oracle_id)).arg(latest_price.to_string());
            }
            
            let card_id = &card.id;
            if let Some(source_card) = card_id_map.get(card_id.as_str()) {
                pipe.cmd("SADD")
                    .arg(format!("printings:{}", oracle_id))
                    .arg(&source_card.id);
                
                pipe.cmd("SET").arg(format!("printing:{}", source_card.id)).arg(oracle_id);
                
                let printing_info = PrintingInfo {
                    id: source_card.id.clone(),
                    set: source_card.set.clone(),
                    set_name: source_card.set_name.clone(),
                    collector_number: source_card.collector_number.clone(),
                    tcgplayer_id: source_card.tcgplayer_id,
                    prices: source_card.prices.clone(),
                    image_uris: source_card.image_uris.clone(),
                    released_at: source_card.released_at.clone(),
                    rarity: source_card.rarity.clone(),
                };
                
                pipe.cmd("SET")
                    .arg(format!("printing:info:{}", source_card.id))
                    .arg(serde_json::to_string(&printing_info)?);
            }
            
            for other_card in cards {
                if other_card.oracle_id.as_ref() == Some(oracle_id) && &other_card.id != card_id {
                    pipe.cmd("SADD")
                        .arg(format!("printings:{}", oracle_id))
                        .arg(&other_card.id);
                    
                    pipe.cmd("SET").arg(format!("printing:{}", other_card.id)).arg(oracle_id);
                    
                    let printing_info = PrintingInfo {
                        id: other_card.id.clone(),
                        set: other_card.set.clone(),
                        set_name: other_card.set_name.clone(),
                        collector_number: other_card.collector_number.clone(),
                        tcgplayer_id: other_card.tcgplayer_id,
                        prices: other_card.prices.clone(),
                        image_uris: other_card.image_uris.clone(),
                        released_at: other_card.released_at.clone(),
                        rarity: other_card.rarity.clone(),
                    };
                    
                    pipe.cmd("SET")
                        .arg(format!("printing:info:{}", other_card.id))
                        .arg(serde_json::to_string(&printing_info)?);
                }
            }
        }
        
        pipe.execute(con);
        
        overall_pb.inc(batch.len() as u64);
        batch_pb.finish_with_message(format!("Batch #{} completed", i + 1));
    }
    
    let set_codes: Vec<String> = all_set_codes.into_iter().collect();
    let _: () = con.set("mtg:sets", serde_json::to_string(&set_codes)?)?;
    let _: () = con.set("mtg:stats:card_count", oracle_map_len)?;
    let _: () = con.set("mtg:stats:last_update", Utc::now().to_rfc3339())?;
    
    overall_pb.finish_with_message("All cards stored in Redis");
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    println!("=== Starting Scryfall Indexer ===");
    println!("System configuration:");
    println!("- Batch size: {}", BATCH_SIZE);
    println!("- Chunk size: {}", CHUNK_SIZE);
    println!("- Max prefix length: {}", MAX_PREFIX_LENGTH);
    
    let cards = download_scryfall_data()?;
    let (oracle_id_map, all_set_codes) = build_card_index(&cards)?;
    
    println!("Connecting to Redis...");
    let client = Client::open("redis://127.0.0.1:6379")?;
    let mut con = client.get_connection()?;
    
    let ping: String = redis::cmd("PING").query(&mut con)?;
    if ping != "PONG" {
        return Err("Redis connection failed".into());
    }
    
    store_card_index(&mut con, oracle_id_map, all_set_codes, &cards)?;
    
    let total_time = start_time.elapsed();
    println!(
        "=== Total execution time: {:.2} seconds ===",
        total_time.as_secs_f32()
    );
    println!("Scryfall data successfully downloaded and stored in Redis");
    
    let key_types = [
        "card:oracle:*", "card:name:*", "auto:prefix:*", 
        "set:*", "tcg:*", "prices:*", "printings:*", "printing:*"
    ];
    
    println!("\nRedis Memory Usage:");
    for key_type in key_types.iter() {
        let key_count: i64 = redis::cmd("EVAL")
            .arg("return #redis.call('keys', ARGV[1])")
            .arg(0)
            .arg(key_type)
            .query(&mut con)?;
        
        println!("  {}: {} keys", key_type, key_count);
    }
    
    Ok(())
}