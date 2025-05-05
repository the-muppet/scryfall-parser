use indicatif::{ProgressBar, ProgressStyle};
use redis::{Client, Commands, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;

#[derive(Deserialize, Debug)]
struct ScryfallCard {
    id: String,
    #[serde(default)] // Make oracle_id optional
    oracle_id: Option<String>,
    name: String,
    #[serde(default)] // Make layout optional with a default
    layout: String,
    set: String,
    collector_number: String,
    #[serde(default)] // Make tcgplayer_id optional
    tcgplayer_id: Option<i64>,
    #[serde(default)] // Make prices optional with a default
    prices: Option<Prices>,
    #[serde(default)]
    image_uris: Option<ImageUris>,
    #[serde(default)]
    card_faces: Option<Vec<CardFace>>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
struct CardFace {
    name: String,
    #[serde(default)]
    image_uris: Option<ImageUris>,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
struct ImageUris {
    small: String,
    normal: String,
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

    let client = reqwest::blocking::Client::builder()
        .user_agent("MTGPriceAnalyzer/1.0")
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

    println!(
        "Response keys: {:?}",
        bulk_data
            .as_object()
            .map(|obj| obj.keys().collect::<Vec<_>>())
    );

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
    let cards: Vec<ScryfallCard> = cards_response.json()?;

    let elapsed = download_start.elapsed();
    println!(
        "Download and parsing completed in {:.2} seconds",
        elapsed.as_secs_f32()
    );
    println!("Downloaded {} cards", cards.len());

    Ok(cards)
}

fn index_and_store_in_redis(cards: Vec<ScryfallCard>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to Redis...");
    let client = Client::open("redis://127.0.0.1:6379")?;
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

    let mut oracle_id_map: std::collections::HashMap<String, IndexedCard> =
        std::collections::HashMap::new();
    let mut all_set_codes = HashSet::new();
    let mut skipped_count = 0;

    for card in &cards {
        pb.inc(1);

        if card.oracle_id.is_none() {
            skipped_count += 1;
            continue;
        }

        let oracle_id = card.oracle_id.as_ref().unwrap().clone();
        let card_name = card.name.clone();
        all_set_codes.insert(card.set.clone());

        let printing_price = PrintingPrice {
            set: card.set.clone(),
            collector_number: card.collector_number.clone(),
            tcgplayer_id: card.tcgplayer_id,
            prices: card.prices.clone().unwrap_or_default(),
        };

        let indexed_card = oracle_id_map.entry(oracle_id.clone()).or_insert_with(|| {
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

        if let Some(prices) = &card.prices {
            if let Some(usd_price) = &prices.usd {
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
    }

    println!(
        "Skipped {} cards without oracle_id (tokens, emblems, etc.)",
        skipped_count
    );
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

    println!(
        "Processing completed in {:.2} seconds",
        start_time.elapsed().as_secs_f32()
    );
    println!("All data successfully stored in Redis");

    Ok(())
}

fn store_indexed_cards(
    con: &mut Connection,
    oracle_id_map: std::collections::HashMap<String, IndexedCard>,
    pb: &ProgressBar,
) -> Result<(), Box<dyn std::error::Error>> {
    for (oracle_id, card) in &oracle_id_map {

        let card_json = serde_json::to_string(&card)?;
        let _: () = con.set(format!("card:oracle:{}", oracle_id), &card_json)?;
        let _: () = con.set(format!("card:name:{}", card.name.to_lowercase()), oracle_id)?;

        let name_lower = card.name.to_lowercase();
        let chars: Vec<char> = name_lower.chars().collect();

        for i in 1..=chars.len() {
            let prefix: String = chars[0..i].iter().collect();
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

        let latest_price = card
            .prices
            .iter()
            .filter_map(|p| {
                p.prices
                    .usd
                    .as_ref()
                    .and_then(|price| price.parse::<f32>().ok())
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if let Some(price) = latest_price {
            let _: () = con.set(format!("price:latest:{}", oracle_id), price.to_string())?;
        }

        pb.inc(1);
    }

    let _: () = con.set("mtg:stats:card_count", oracle_id_map.len())?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    // Download Scryfall data
    let cards = download_scryfall_data()?;

    // Index and store in Redis
    index_and_store_in_redis(cards)?;

    println!(
        "Total execution time: {:.2} seconds",
        start_time.elapsed().as_secs_f32()
    );
    println!("Scryfall data successfully downloaded and stored in Redis");

    Ok(())
}
