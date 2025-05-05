use std::collections::HashSet;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use indicatif::{ProgressBar, ProgressStyle};
use redis::{Client, Commands, Connection};

#[derive(Deserialize, Debug)]
struct ScryfallCard {
    id: String,
    oracle_id: String,
    name: String,
    layout: String,
    set: String,
    collector_number: String,
    tcgplayer_id: Option<i64>,
    prices: Prices,
    #[serde(default)]
    image_uris: Option<ImageUris>,
    #[serde(default)]
    card_faces: Option<Vec<CardFace>>,
}

#[derive(Deserialize, Debug)]
struct CardFace {
    name: String,
    #[serde(default)]
    image_uris: Option<ImageUris>,
}

#[derive(Deserialize, Debug)]
struct ImageUris {
    small: String,
    normal: String,
    large: String,
}

#[derive(Deserialize, Debug)]
struct Prices {
    usd: Option<String>,
    usd_foil: Option<String>,
    eur: Option<String>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
struct PrintingPrice {
    set: String,
    collector_number: String,
    tcgplayer_id: Option<i64>,
    prices: Prices,
}

fn download_scryfall_data() -> Result<Vec<ScryfallCard>, Box<dyn std::error::Error>> {
    println!("Downloading Scryfall default_cards.json (this may take a while)...");
    
    let bulk_data_url = "https://api.scryfall.com/bulk-data";
    let response = reqwest::blocking::get(bulk_data_url)?;
    let bulk_data: serde_json::Value = response.json()?;
    
    let default_cards_uri = bulk_data["data"]
        .as_array()
        .and_then(|data| {
            data.iter()
                .find(|item| item["type"].as_str().unwrap_or("") == "default_cards")
                .and_then(|item| item["download_uri"].as_str())
        })
        .ok_or("Default cards download URL not found")?;
    
    println!("Download URL found: {}", default_cards_uri);
    println!("Downloading card data...");
    
    let download_start = Instant::now();
    let response = reqwest::blocking::get(default_cards_uri)?;
    let cards: Vec<ScryfallCard> = response.json()?;
    
    println!("Download and decompression completed in {:.2} seconds", download_start.elapsed().as_secs_f32());
    println!("Downloaded {} cards", cards.len());
    
    Ok(cards)
}

fn index_and_store_in_redis(cards: Vec<ScryfallCard>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to Redis...");
    let client = Client::open("redis://localhost:6379")?;
    let mut con = client.get_connection()?;
    
    let ping: String = redis::cmd("PING").query(&mut con)?;
    if ping != "PONG" {
        return Err("Redis connection failed".into());
    }
    
    println!("Processing and indexing cards...");
    let start_time = Instant::now();
    let total_cards = cards.len();
    
    let pb = ProgressBar::new(total_cards as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards ({eta})")?
        .progress_chars("#>-"));
    
    let mut oracle_id_map: std::collections::HashMap<String, IndexedCard> = std::collections::HashMap::new();
    let mut all_set_codes = HashSet::new();
    
    for card in &cards {
        pb.inc(1);
        
        let card_name = card.name.clone();
        let oracle_id = card.oracle_id.clone();
        all_set_codes.insert(card.set.clone());

        let printing_price = PrintingPrice {
            set: card.set.clone(),
            collector_number: card.collector_number.clone(),
            tcgplayer_id: card.tcgplayer_id,
            prices: card.prices.clone(),
        };
        
        let indexed_card = oracle_id_map.entry(oracle_id.clone()).or_insert_with(|| {
            let main_image = card.image_uris.as_ref().map(|uris| uris.normal.clone())
                .or_else(|| card.card_faces.as_ref().and_then(|faces| 
                    faces.get(0).and_then(|face| face.image_uris.as_ref().map(|uris| uris.normal.clone()))));
                
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

        if let Some(usd_price) = &card.prices.usd {
            if let Ok(price_value) = usd_price.parse::<f32>() {
                if price_value > 0.0 {
                    let price_bucket = (price_value * 100.0).round() as i32;
                    
                    let _: () = redis::cmd("ZADD")
                        .arg("prices:usd")
                        .arg(price_bucket)
                        .arg(oracle_id.clone())
                        .query(&mut con)?;
                }
            }
        }
    }
    
    pb.finish_with_message("Card indexing completed");
    
    println!("Storing {} unique cards in Redis...", oracle_id_map.len());
    let pb = ProgressBar::new(oracle_id_map.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} oracle IDs ({eta})")?
        .progress_chars("#>-"));
    
    store_indexed_cards(&mut con, oracle_id_map, &pb)?;
    pb.finish_with_message("Cards stored in Redis");
    
    let set_codes: Vec<String> = all_set_codes.into_iter().collect();
    let _: () = con.set("mtg:sets", serde_json::to_string(&set_codes)?)?;
    
    println!("Processing completed in {:.2} seconds", start_time.elapsed().as_secs_f32());
    println!("All data successfully stored in Redis");
    
    Ok(())
}

fn store_indexed_cards(
    con: &mut Connection, 
    oracle_id_map: std::collections::HashMap<String, IndexedCard>,
    pb: &ProgressBar
) -> Result<(), Box<dyn std::error::Error>> {
    for (oracle_id, card) in &oracle_id_map {
        let card_json = serde_json::to_string(&card)?;
        
        let _: () = con.set(format!("card:oracle:{}", oracle_id), &card_json)?;
        
        let _: () = con.set(format!("card:name:{}", card.name.to_lowercase()), oracle_id)?;
        
        let name_lower = card.name.to_lowercase();
        for i in 1..=name_lower.len() {
            let prefix = &name_lower[0..i];
            let _: () = redis::cmd("SADD")
                .arg(format!("autocomplete:prefix:{}", prefix))
                .arg(oracle_id)
                .query(con)?;
        }
        
        for set_code in &card.sets {
            let _: () = redis::cmd("SADD")
                .arg(format!("set:{}", set_code))
                .arg(oracle_id)
                .query(con)?;
        }
        
        for tcgplayer_id in &card.tcgplayer_ids {
            let _: () = con.set(format!("tcgplayer:{}", tcgplayer_id), oracle_id)?;
        }

        if let Some(latest_price) = card.prices.iter()
            .filter_map(|p| p.prices.usd.as_ref().and_then(|price| price.parse::<f32>().ok()))
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)) {
            
            let _: () = con.set(format!("price:latest:{}", oracle_id), latest_price.to_string())?;
        }
        
        pb.inc(1);
    }
    
    let _: () = con.set("mtg:stats:card_count", oracle_id_map.len())?;
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    let cards = download_scryfall_data()?;
    
    index_and_store_in_redis(cards)?;
    
    println!("Total execution time: {:.2} seconds", start_time.elapsed().as_secs_f32());
    println!("Scryfall data successfully downloaded and stored in Redis");
    
    Ok(())
}