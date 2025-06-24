use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use redis::{Client, Commands, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use chrono::Utc;

const BATCH_SIZE: usize = 2000;     
const CHUNK_SIZE: usize = 8000;  // Reduced for larger all_cards dataset
const MAX_PREFIX_LENGTH: usize = 30;
const NGRAM_SIZE: usize = 3; 

#[derive(Deserialize, Debug, Clone)]
pub struct ScryfallCard {
    pub id: String,
    #[serde(default)]
    pub oracle_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub layout: String,
    pub set: String,
    pub set_name: String,
    pub collector_number: String,
    #[serde(default)]
    pub tcgplayer_id: Option<i64>,
    #[serde(default)]
    pub prices: Option<Prices>,
    #[serde(default)]
    pub image_uris: Option<ImageUris>,
    #[serde(default)]
    pub card_faces: Option<Vec<CardFace>>,
    #[serde(default)]
    pub released_at: Option<String>,
    #[serde(default)]
    pub rarity: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct CardFace {
    pub name: String,
    #[serde(default)]
    pub image_uris: Option<ImageUris>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct ImageUris {
    #[serde(default)]
    pub small: String,
    #[serde(default)]
    pub normal: String,
    #[serde(default)]
    pub large: String,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct Prices {
    #[serde(default)]
    pub usd: Option<String>,
    #[serde(default)]
    pub usd_foil: Option<String>,
    #[serde(default)]
    pub eur: Option<String>,
}

#[derive(Default)]
pub struct SearchIndexes {
    pub ngrams: HashMap<String, HashSet<String>>,
    pub metaphones: HashMap<String, HashSet<String>>,
    pub words: HashMap<String, HashSet<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexedCard {
    pub id: String,
    pub oracle_id: String,
    pub name: String,
    pub sets: Vec<String>,      
    pub layout: String,
    pub tcgplayer_ids: Vec<i64>,
    pub main_image: Option<String>,
    pub prices: Vec<PrintingPrice>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrintingPrice {
    pub set: String, 
    pub set_name: Option<String>,
    pub collector_number: String,
    pub tcgplayer_id: Option<i64>,
    pub prices: Prices,
    pub released_at: Option<String>,
    pub rarity: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PrintingInfo {
    pub id: String,
    pub set: String,
    pub set_name: String,
    pub collector_number: String,
    pub tcgplayer_id: Option<i64>,
    pub prices: Option<Prices>,
    pub image_uris: Option<ImageUris>,
    pub released_at: Option<String>,
    pub rarity: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexStats {
    pub card_count: usize,
    pub set_count: usize,
    pub last_update: String,
}

// Public API functions for Python bindings

pub fn run_indexer(redis_url: &String) -> Result<IndexStats, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    println!("=== Starting Enhanced Scryfall Indexer ===");
    println!("System configuration:");
    println!("- Batch size: {}", BATCH_SIZE);
    println!("- Chunk size: {}", CHUNK_SIZE);
    println!("- Max prefix length: {}", MAX_PREFIX_LENGTH);
    println!("- N-gram size: {}", NGRAM_SIZE);
    
    let cards = download_scryfall_data()?;
    let (oracle_id_map, all_set_codes, search_indexes) = build_card_index(&cards)?;
    
    println!("Connecting to Redis...");
    let client = Client::open(redis_url.to_string())?;
    let mut con = client.get_connection()?;
    
    let ping: String = redis::cmd("PING").query(&mut con)?;
    if ping != "PONG" {
        return Err("Redis connection failed".into());
    }
    
    let card_count = oracle_id_map.len();
    let set_count = all_set_codes.len();
    
    store_card_index(&mut con, oracle_id_map, all_set_codes, search_indexes, &cards)?;
    
    let total_time = start_time.elapsed();
    println!(
        "=== Total execution time: {:.2} seconds ===",
        total_time.as_secs_f32()
    );
    
    Ok(IndexStats {
        card_count,
        set_count,
        last_update: Utc::now().to_rfc3339(),
    })
}

pub fn search_cards_internal(
    query: &str,
    max_results: usize,
    redis_url: &str,
) -> Result<Vec<IndexedCard>, Box<dyn std::error::Error>> {
    let client = Client::open(redis_url.to_string())?;
    let mut con = client.get_connection()?;
    
    // Use the fuzzy search Lua script
    let script_sha: String = con.get("mtg:script:fuzzy_search")?;
    
    let oracle_ids: Vec<String> = redis::cmd("EVALSHA")
        .arg(&script_sha)
        .arg(0)
        .arg(query)
        .arg(2) // max_distance
        .arg(max_results)
        .query(&mut con)?;
    
    let mut results = Vec::new();
    for oracle_id in oracle_ids {
        if let Ok(card_data) = con.get::<_, String>(format!("card:oracle:{}", oracle_id)) {
            if let Ok(card) = serde_json::from_str::<IndexedCard>(&card_data) {
                results.push(card);
            }
        }
    }
    
    Ok(results)
}

pub fn get_card_by_oracle_id_internal(
    oracle_id: &str,
    redis_url: &str,
) -> Result<Option<IndexedCard>, Box<dyn std::error::Error>> {
    let client = Client::open(redis_url.to_string())?;
    let mut con = client.get_connection()?;
    
    match con.get::<_, String>(format!("card:oracle:{}", oracle_id)) {
        Ok(card_data) => {
            let card = serde_json::from_str::<IndexedCard>(&card_data)?;
            Ok(Some(card))
        }
        Err(_) => Ok(None),
    }
}

pub fn get_autocomplete_internal(
    prefix: &str,
    max_results: usize,
    redis_url: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = Client::open(redis_url.to_string())?;
    let mut con = client.get_connection()?;
    
    let prefix_lower = prefix.to_lowercase();
    let oracle_ids: Vec<String> = con.smembers(format!("auto:prefix:{}", prefix_lower))?;
    
    let mut card_names = Vec::new();
    for oracle_id in oracle_ids.into_iter().take(max_results) {
        if let Ok(card_data) = con.get::<_, String>(format!("card:oracle:{}", oracle_id)) {
            if let Ok(card) = serde_json::from_str::<IndexedCard>(&card_data) {
                card_names.push(card.name);
            }
        }
    }
    
    Ok(card_names)
}

pub fn get_stats_internal(redis_url: &str) -> Result<IndexStats, Box<dyn std::error::Error>> {
    let client = Client::open(redis_url.to_string())?;
    let mut con = client.get_connection()?;
    
    let card_count: usize = con.get("mtg:stats:card_count").unwrap_or(0);
    let last_update: String = con.get("mtg:stats:last_update").unwrap_or_else(|_| "Never".to_string());
    
    // Count unique sets
    let sets_data: String = con.get("mtg:sets").unwrap_or_else(|_| "[]".to_string());
    let sets: Vec<String> = serde_json::from_str(&sets_data).unwrap_or_default();
    
    Ok(IndexStats {
        card_count,
        set_count: sets.len(),
        last_update,
    })
}

fn download_scryfall_data() -> Result<Vec<ScryfallCard>, Box<dyn std::error::Error>> {
    println!("Downloading Scryfall all_cards.json (this may take a while)...");

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
        .find(|item| item.get("type").and_then(|t| t.as_str()) == Some("all_cards"))
        .ok_or("all_cards entry not found")?;

    let download_uri = default_cards_entry
        .get("download_uri")
        .and_then(|u| u.as_str())
        .ok_or("download_uri field not found or not a string")?;

    let compressed_size = default_cards_entry.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
    println!("Found download URI: {}", download_uri);
    println!("Downloading ALL card data (~{}MB compressed, includes ALL printings)", compressed_size / 1024 / 1024);
    println!("This is significantly larger than default_cards and will take longer to process...");

    let download_start = Instant::now();
    
    let cards_response = client
        .get(download_uri)
        .header("Accept", "application/json")
        .send()?;

    if !cards_response.status().is_success() {
        return Err(format!("Failed to download cards: HTTP {}", cards_response.status()).into());
    }

    println!("Download complete, parsing JSON");
    
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

fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    
    let len1 = s1_chars.len();
    let len2 = s2_chars.len();
    
    // Create a matrix of size (len1+1) x (len2+1)
    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];
    
    // Initialize the first row and column
    for i in 0..=len1 {
        matrix[i][0] = i;
    }
    for j in 0..=len2 {
        matrix[0][j] = j;
    }
    
    // Fill the matrix
    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            
            matrix[i][j] = std::cmp::min(
                std::cmp::min(
                    matrix[i - 1][j] + 1,     // deletion
                    matrix[i][j - 1] + 1      // insertion
                ),
                matrix[i - 1][j - 1] + cost   // substitution
            );
        }
    }
    
    matrix[len1][len2]
}

fn generate_metaphone(text: &str) -> String {
    let text = text.to_lowercase();
    let mut result = String::new();
    let mut prev_char: Option<char> = None;
    
    for c in text.chars() {
        let code = match c {
            'b' | 'p' | 'f' | 'v' => "B",
            'c' | 'k' | 'q' => "K",
            'd' | 't' => "T",
            'g' | 'j' => "J",
            'l' => "L",
            'm' | 'n' => "M",
            'r' => "R",
            's' | 'z' => "S",
            'x' => "KS",
            'a' | 'e' | 'i' | 'o' | 'u' | 'y' | 'w' | 'h' => "",
            _ => "",
        };
        
        if !code.is_empty() {
            let code_chars: Vec<char> = code.chars().collect();
            
            if code_chars.len() == 1 {
                let code_char = code_chars[0];
                if prev_char != Some(code_char) {
                    result.push(code_char);
                    prev_char = Some(code_char);
                }
            } else {
                result.push_str(code);
                if let Some(first_char) = code_chars.first() {
                    prev_char = Some(*first_char);
                }
            }
        }
    }
    
    result
}

fn generate_ngrams(text: &str, n: usize) -> Vec<String> {
    let text = text.to_lowercase();
    let chars: Vec<char> = text.chars().collect();
    let mut ngrams = Vec::new();
    
    if chars.len() < n {
        ngrams.push(text);
        return ngrams;
    }
    
    for i in 0..=(chars.len() - n) {
        let ngram: String = chars[i..(i + n)].iter().collect();
        ngrams.push(ngram);
    }
    
    ngrams
}

fn tokenize_words(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() >= 2)
        .map(|s| s.to_string())
        .collect()
}

fn build_card_index(
    cards: &[ScryfallCard],
) -> Result<(HashMap<String, IndexedCard>, HashSet<String>, SearchIndexes), Box<dyn std::error::Error>> {
    println!("Building card index in parallel...");
    let start_time = Instant::now();
    
    let pb = ProgressBar::new(cards.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards ({eta})")?
        .progress_chars("#>-"));
    
    let skipped_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    let oracle_map = Arc::new(Mutex::new(HashMap::new()));
    let set_codes = Arc::new(Mutex::new(HashSet::new()));
    let search_indexes = Arc::new(Mutex::new(SearchIndexes::default()));
    
    cards.par_chunks(CHUNK_SIZE)
        .for_each(|chunk| {
            let mut local_oracle_map: HashMap<String, IndexedCard> = HashMap::new();
            let mut local_set_codes = HashSet::new();
            let mut local_ngrams = HashMap::new();
            let mut local_metaphones = HashMap::new();
            let mut local_words = HashMap::new();
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
                        name: card_name.clone(),
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
                
                let name_lower = card_name.to_lowercase();
                
                for ngram in generate_ngrams(&name_lower, NGRAM_SIZE) {
                    local_ngrams.entry(ngram)
                        .or_insert_with(HashSet::new)
                        .insert(oracle_id.clone());
                }
                
                // Build metaphone indexes for phonetic matching
                let metaphone = generate_metaphone(&name_lower);
                local_metaphones.entry(metaphone)
                    .or_insert_with(HashSet::new)
                    .insert(oracle_id.clone());
                
                // Build word indexes
                for word in tokenize_words(&name_lower) {
                    local_words.entry(word)
                        .or_insert_with(HashSet::new)
                        .insert(oracle_id.clone());
                }
            }
            
            let mut oracle_map_lock = oracle_map.lock().unwrap();
            for (oracle_id, mut new_card) in local_oracle_map {
                oracle_map_lock.entry(oracle_id).and_modify(|existing_card| {
                    // Merge printings from multiple threads
                    existing_card.prices.append(&mut new_card.prices);
                    
                    // Merge sets
                    for set in new_card.sets {
                        if !existing_card.sets.contains(&set) {
                            existing_card.sets.push(set);
                        }
                    }
                    
                    // Merge TCGPlayer IDs
                    for tcg_id in new_card.tcgplayer_ids {
                        if !existing_card.tcgplayer_ids.contains(&tcg_id) {
                            existing_card.tcgplayer_ids.push(tcg_id);
                        }
                    }
                }).or_insert(new_card);
            }
            
            let mut set_codes_lock = set_codes.lock().unwrap();
            for set_code in local_set_codes {
                set_codes_lock.insert(set_code);
            }
            

            let mut search_indexes_lock = search_indexes.lock().unwrap();
            for (ngram, ids) in local_ngrams {
                let entry = search_indexes_lock.ngrams.entry(ngram).or_insert_with(HashSet::new);
                entry.extend(ids);
            }
            
            for (metaphone, ids) in local_metaphones {
                let entry = search_indexes_lock.metaphones.entry(metaphone).or_insert_with(HashSet::new);
                entry.extend(ids);
            }
            
            for (word, ids) in local_words {
                let entry = search_indexes_lock.words.entry(word).or_insert_with(HashSet::new);
                entry.extend(ids);
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
    
    let search_indexes_result = Arc::try_unwrap(search_indexes)
        .map_err(|_| "Failed to unwrap search_indexes")?
        .into_inner()
        .map_err(|e| format!("Failed to unwrap search_indexes mutex: {:?}", e))?;
    
    println!("Card indexing statistics:");
    println!("- Unique cards: {}", oracle_map_result.len());
    println!("- N-gram indexes: {}", search_indexes_result.ngrams.len());
    println!("- Metaphone indexes: {}", search_indexes_result.metaphones.len());
    println!("- Word indexes: {}", search_indexes_result.words.len());
    
    pb.finish_with_message(format!("Card indexing completed: {} unique cards", oracle_map_result.len()));
    
    Ok((oracle_map_result, set_codes_result, search_indexes_result))
}

fn store_card_index(
    con: &mut Connection,
    oracle_id_map: HashMap<String, IndexedCard>, 
    all_set_codes: HashSet<String>,
    search_indexes: SearchIndexes,
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
            
            for word in tokenize_words(&name_lower) {
                let word_chars: Vec<char> = word.chars().collect();
                let word_len = word_chars.len();

                let prefix_limit = std::cmp::min(word_len, MAX_PREFIX_LENGTH);
                
                // Add word-level prefixes for each word in the name
                for i in 1..=prefix_limit {
                    let word_prefix: String = word_chars[0..i].iter().collect();
                    pipe.cmd("SADD")
                        .arg(format!("auto:word:{}", word_prefix))
                        .arg(oracle_id);
                }
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
            
            // With all_cards, we get multiple printings per card
            // Using max price to represent the highest-value printing for this card
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
        
        let _: () = pipe.query(con)?;
        
        overall_pb.inc(batch.len() as u64);
        batch_pb.finish_with_message(format!("Batch #{} completed", i + 1));
    }
    
    // Store search indexes
    println!("Storing n-gram indexes...");
    let ngram_pb = mp.add(ProgressBar::new(search_indexes.ngrams.len() as u64));
    ngram_pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} n-grams")?
        .progress_chars("#>-"));

    for (ngram, ids) in search_indexes.ngrams {
        let ids_vec: Vec<String> = ids.into_iter().collect();

        const CHUNK_SIZE: usize = 1000;
        for chunk in ids_vec.chunks(CHUNK_SIZE) {
            let _: () = con.sadd(format!("ngram:{}", ngram), chunk)?;
        }

        ngram_pb.inc(1);
    }
    ngram_pb.finish_with_message("N-gram indexes stored");
    
    println!("Storing metaphone indexes...");
    let mp_pb = mp.add(ProgressBar::new(search_indexes.metaphones.len() as u64));
    mp_pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} metaphones")?
        .progress_chars("#>-"));
    
    for (metaphone, ids) in search_indexes.metaphones {
        let ids_vec: Vec<String> = ids.into_iter().collect();

        const CHUNK_SIZE: usize = 1000;
        for chunk in ids_vec.chunks(CHUNK_SIZE) {
            let _: () = con.sadd(format!("metaphone:{}", metaphone), chunk)?;
        }

        mp_pb.inc(1);
    }   
    mp_pb.finish_with_message("Metaphone indexes stored");
    
    println!("Storing word indexes...");
    let word_pb = mp.add(ProgressBar::new(search_indexes.words.len() as u64));
    word_pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} words")?
        .progress_chars("#>-"));
    
    for (word, ids) in search_indexes.words {
        let ids_vec: Vec<String> = ids.into_iter().collect();
        
        const CHUNK_SIZE: usize = 1000;
        for chunk in ids_vec.chunks(CHUNK_SIZE) {
            let _: () = con.sadd(format!("word:{}", word), chunk)?;
        }
        
        word_pb.inc(1);
    }
    word_pb.finish_with_message("Word indexes stored");
    
    let set_codes: Vec<String> = all_set_codes.into_iter().collect();
    let _: () = con.set("mtg:sets", serde_json::to_string(&set_codes)?)?;
    let _: () = con.set("mtg:stats:card_count", oracle_map_len)?;
    let _: () = con.set("mtg:stats:last_update", Utc::now().to_rfc3339())?;
    
    // Store fuzzy search scripts in Redis
    println!("Loading fuzzy search Lua scripts...");
    
    // Script for fuzzy searching by Levenshtein distance
    let fuzzy_search_script = r#"
    local query = ARGV[1]
    local max_distance = tonumber(ARGV[2]) or 2
    local max_results = tonumber(ARGV[3]) or 20
    
    local candidates = {}
    local results = {}
    
    -- First get exact prefix matches
    local prefix_key = 'auto:prefix:' .. query
    local prefix_matches = redis.call('SMEMBERS', prefix_key)
    for _, id in ipairs(prefix_matches) do
        table.insert(results, id)
        if #results >= max_results then
            return results
        end
    end
    
    -- Get word matches
    local words = {}
    for word in string.gmatch(query:lower(), '%S+') do
        table.insert(words, word)
    end
    
    -- For each word, find cards containing that word
    for _, word in ipairs(words) do
        if #word >= 3 then
            local word_key = 'word:' .. word
            local word_matches = redis.call('SMEMBERS', word_key)
            for _, id in ipairs(word_matches) do
                if not candidates[id] then
                    candidates[id] = 0
                end
                candidates[id] = candidates[id] + 1
            end
        end
    end
    
    -- If we didn't find matches with words, try with n-grams
    if next(candidates) == nil and #query >= 3 then
        -- Break query into n-grams
        for i = 1, #query - 2 do
            local ngram = query:sub(i, i + 2):lower()
            local ngram_key = 'ngram:' .. ngram
            local ngram_matches = redis.call('SMEMBERS', ngram_key)
            
            for _, id in ipairs(ngram_matches) do
                if not candidates[id] then
                    candidates[id] = 0
                end
                candidates[id] = candidates[id] + 1
            end
        end
    end
    
    -- If we still don't have candidates, try metaphone match
    if next(candidates) == nil then
        -- Simple metaphone implementation directly in Lua
        local function simplify_metaphone(text)
            local result = ""
            local map = {
                ['b'] = 'B', ['p'] = 'B', ['f'] = 'B', ['v'] = 'B',
                ['c'] = 'K', ['k'] = 'K', ['q'] = 'K',
                ['d'] = 'T', ['t'] = 'T',
                ['g'] = 'J', ['j'] = 'J',
                ['l'] = 'L',
                ['m'] = 'M', ['n'] = 'M',
                ['r'] = 'R',
                ['s'] = 'S', ['z'] = 'S',
                ['x'] = 'KS'
            }
            
            text = string.lower(text)
            for i = 1, #text do
                local char = text:sub(i, i)
                local code = map[char] or ""
                result = result .. code
            end
            
            return result
        end
        
        local metaphone = simplify_metaphone(query)
        if #metaphone > 0 then
            local metaphone_key = 'metaphone:' .. metaphone
            local metaphone_matches = redis.call('SMEMBERS', metaphone_key)
            
            for _, id in ipairs(metaphone_matches) do
                candidates[id] = 2  -- Give metaphone matches a good score
            end
        end
    end
    
    -- Convert candidates to sorted array
    local candidate_array = {}
    for id, score in pairs(candidates) do
        table.insert(candidate_array, {id = id, score = score})
    end
    
    -- Sort by score (higher is better)
    table.sort(candidate_array, function(a, b) return a.score > b.score end)
    
    -- Take top candidates
    for i = 1, math.min(#candidate_array, max_results) do
        table.insert(results, candidate_array[i].id)
    end
    
    return results
    "#;
    
    let fuzzy_search_sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(fuzzy_search_script)
        .query(con)?;
    
    let _: () = con.set("mtg:script:fuzzy_search", fuzzy_search_sha)?;
    
    overall_pb.finish_with_message("All cards and indexes stored in Redis");
    
    Ok(())
}

fn get_redis_url() -> String {
    let host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("REDIS_PORT").unwrap_or_else(|_| "9999".to_string());
    format!("redis://{}:{}", host, port)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = get_redis_url();
    println!("Using Redis URL: {}", redis_url);
    
    let stats = run_indexer(&redis_url)?;
    
    println!("Scryfall ALL CARDS data successfully downloaded and indexed with enhanced autocomplete and fuzzy search");
    println!("Stats: {} unique cards (ALL printings included), {} sets", stats.card_count, stats.set_count);
    
    // Display key usage statistics
    let client = Client::open(redis_url.clone())?;
    let mut con = client.get_connection()?;
    
    let key_types = [
        "card:oracle:*", "card:name:*", "auto:prefix:*", "auto:word:*",
        "ngram:*", "metaphone:*", "word:*",
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
