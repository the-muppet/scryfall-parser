mod types;
mod sku_pricing;
mod redis_client;
mod api_server;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use redis::{Client, Commands, Connection};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use memmap2::Mmap;
use rayon::prelude::*;

use types::*;
use sku_pricing::SkuPricingManager;
use uuid;
use walkdir;
use xz2::read::XzDecoder;

const BATCH_SIZE: usize = 2000;           // Larger batches for Redis
const DECK_BATCH_SIZE: usize = 100;       // Parallel deck processing batches  
const MEMORY_MAP_THRESHOLD: u64 = 50 * 1024 * 1024; // 50MB threshold for memory mapping
const MAX_PREFIX_LENGTH: usize = 30;      // Max length for autocomplete prefixes
const NGRAM_SIZE: usize = 3;              // N-gram size for fuzzy matching

// Advanced search indexes structure
#[derive(Default)]
pub struct SearchIndexes {
    pub ngrams: HashMap<String, HashSet<String>>,
    pub metaphones: HashMap<String, HashSet<String>>,
    pub words: HashMap<String, HashSet<String>>,
}

#[derive(Parser)]
#[command(name = "mtgjson-indexer")]
#[command(about = "Downloads and indexes MTGJSON data into Redis")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    redis_host: String,

    #[arg(long, default_value = "9999")]
    redis_port: u16,

    #[arg(long)]
    download_only: bool,

    #[arg(long)]
    index_only: bool,

    #[arg(long, default_value = "data")]
    data_dir: String,

    #[arg(long, help = "Path to TCGPlayer pricing CSV file (obtain from TCGPlayer seller account or API)")]
    tcg_csv_path: Option<String>,

    #[arg(long, help = "Skip pricing data processing even if CSV is provided")]
    skip_pricing: bool,

    #[arg(long, help = "Automatically download TCGPlayer CSV using tcgcsv_clean.py (requires valid cookies)")]
    auto_download_tcg: bool,

    #[arg(long, help = "Force download even if files are fresh (default: skip if files are less than 24 hours old)")]
    force_download: bool,

    #[arg(long, default_value = "24", help = "Maximum age in hours before files are considered stale")]
    max_age_hours: u64,

    #[arg(long, default_value = "english", help = "Language filter for TCGPlayer SKUs (english, spanish, etc.)")]
    sku_language: String,

    #[arg(long, default_value = "near mint", help = "Condition filter for TCGPlayer SKUs (near mint, lightly played, etc.)")]
    sku_condition: String,

    #[arg(long, help = "Show data freshness status and exit")]
    status: bool,
}

struct MTGJSONIndexer {
    redis_client: Client,
    data_dir: String,
    sku_pricing: SkuPricingManager,
}

impl MTGJSONIndexer {
    fn new(redis_host: &str, redis_port: u16, data_dir: String) -> Result<Self> {
        let redis_url = format!("redis://{}:{}", redis_host, redis_port);
        let redis_client = Client::open(redis_url)
            .context("Failed to create Redis client")?;

        // high-end hardware config
        let num_threads = std::env::var("MTGJSON_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(24); // Default to 24 threads, user can override with env var
            
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .stack_size(8 * 1024 * 1024)  // 8MB stack for parallel workers
            .thread_name(|index| format!("mtgjson-worker-{}", index))
            .build_global()
            .context("Failed to configure Rayon thread pool")?;

        Ok(Self {
            redis_client: redis_client.clone(),
            data_dir,
            sku_pricing: SkuPricingManager::new(redis_client),
        })
    }

    // High-performance JSON loading with memory mapping for large files
    fn load_json_file<T>(&self, file_path: &Path) -> Result<T> 
    where 
        T: serde::de::DeserializeOwned,
    {
        let file = File::open(file_path)
            .context("Failed to open JSON file")?;
        
        let file_size = file.metadata()
            .context("Failed to get file metadata")?
            .len();

        if file_size > MEMORY_MAP_THRESHOLD {
            // Use memory mapping for large files (>50MB)
            let mmap = unsafe { Mmap::map(&file)? };
            
            // Try SIMD JSON first for extra speed
            let mut json_bytes = mmap.to_vec();
            match simd_json::from_slice(&mut json_bytes) {
                Ok(result) => Ok(result),
                Err(_) => {
                    // Fallback to regular serde_json
                    serde_json::from_slice(&mmap)
                        .context("Failed to parse JSON with memory mapping")
                }
            }
        } else {
            // Use regular buffered reading for smaller files
            let reader = BufReader::new(file);
            serde_json::from_reader(reader)
                .context("Failed to parse JSON with buffered reader")
        }
    }

    // === ADVANCED SEARCH FUNCTIONS (ported from Scryfall indexer) ===

    fn generate_metaphone(&self, text: &str) -> String {
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

    fn generate_ngrams(&self, text: &str, n: usize) -> Vec<String> {
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

    fn tokenize_words(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty() && s.len() >= 2)
            .map(|s| s.to_string())
            .collect()
    }

    // === FILE MANAGEMENT ===

    fn get_timestamp_file_path(&self) -> std::path::PathBuf {
        Path::new(&self.data_dir).join(".mtgjson_download_timestamp")
    }

    fn write_download_timestamp(&self) -> Result<()> {
        let timestamp_file = self.get_timestamp_file_path();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        std::fs::write(&timestamp_file, timestamp.to_string())
            .context("Failed to write download timestamp")?;
        
        println!("✓ Download timestamp saved");
        Ok(())
    }

    fn is_data_fresh(&self, max_age_hours: u64) -> bool {
        let timestamp_file = self.get_timestamp_file_path();
        
        if !timestamp_file.exists() {
            println!("📝 No download timestamp found - will download files");
            return false;
        }

        // Check if required files exist
        let required_files = vec![
            "AllPrintings.json",
            "TcgplayerSkus.json", 
            "AllDeckFiles.tar"
        ];

        for filename in &required_files {
            let file_path = Path::new(&self.data_dir).join(filename);
            if !file_path.exists() {
                println!("📁 Required file missing: {} - will download", filename);
                return false;
            }
        }

        // Read timestamp
        match std::fs::read_to_string(&timestamp_file) {
            Ok(content) => {
                match content.trim().parse::<u64>() {
                    Ok(timestamp) => {
                        let current_time = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        
                        let age_seconds = current_time.saturating_sub(timestamp);
                        let max_age_seconds = max_age_hours * 3600;
                        
                        if age_seconds < max_age_seconds {
                            println!("✅ Data is fresh ({:.1} hours old, max: {} hours)", 
                                   age_seconds as f64 / 3600.0, max_age_hours);
                            true
                        } else {
                            println!("⏰ Data is stale ({:.1} hours old, max: {} hours)", 
                                   age_seconds as f64 / 3600.0, max_age_hours);
                            false
                        }
                    }
                    Err(_) => {
                        println!("⚠️  Invalid timestamp format - will download");
                        false
                    }
                }
            }
            Err(_) => {
                println!("⚠️  Cannot read timestamp file - will download");
                false
            }
        }
    }

    async fn download_file(&self, url: &str, filename: &str, force_download: bool) -> Result<()> {
        let file_path = Path::new(&self.data_dir).join(filename);
        
        if file_path.exists() && force_download {
            println!("♻️  {} exists but force download requested", filename);
        } else if file_path.exists() {
            println!("📂 {} already exists, will download if needed", filename);
        }

        std::fs::create_dir_all(&self.data_dir)
            .context("Failed to create data directory")?;

        println!("Downloading {}...", url);
        
        let response = reqwest::get(url).await
            .context("Failed to download file")?;
        
        let total_size = response.content_length().unwrap_or(0);
        
        let pb = ProgressBar::new(total_size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"));

        let mut stream = response.bytes_stream();
        let mut compressed_data = Vec::new();
        
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Failed to read chunk")?;
            compressed_data.extend_from_slice(&chunk);
            pb.inc(chunk.len() as u64);
        }
        
        pb.finish_with_message("Download complete");

        println!("Decompressing {} ({} bytes)...", filename, compressed_data.len());
        
        // Decompress XZ data
        let mut decoder = XzDecoder::new(&compressed_data[..]);
        let mut decompressed_data = Vec::new();
        std::io::copy(&mut decoder, &mut decompressed_data)
            .context("Failed to decompress XZ data")?;

        // Write decompressed JSON to file
        let json_filename = filename.replace(".xz", "");
        let json_path = Path::new(&self.data_dir).join(&json_filename);
        
        let mut file = BufWriter::new(File::create(&json_path)
            .context("Failed to create output file")?);
        file.write_all(&decompressed_data)
            .context("Failed to write decompressed data")?;
        file.flush()
            .context("Failed to flush file")?;

        println!("✓ Downloaded and saved {} ({} bytes)", json_filename, decompressed_data.len());
        
        Ok(())
    }

    async fn download_data_files(&self, force_download: bool, max_age_hours: u64) -> Result<()> {
        println!("=== Checking MTGJSON Data Files ===");
        
        // Check freshness first (unless force download is requested)
        if !force_download && self.is_data_fresh(max_age_hours) {
            println!("🎯 All required files are fresh - skipping download");
            return Ok(());
        }
        
        if force_download {
            println!("🔄 Force download enabled - will download all files");
        } else {
            println!("📅 Data is stale or missing - downloading fresh files");
        }
        
        let downloads = vec![
            ("https://mtgjson.com/api/v5/AllPrintings.json.xz", "AllPrintings.json.xz"),
            ("https://mtgjson.com/api/v5/TcgplayerSkus.json.xz", "TcgplayerSkus.json.xz"),
            ("https://mtgjson.com/api/v5/AllDeckFiles.tar.xz", "AllDeckFiles.tar.xz"),
        ];

        for (url, filename) in downloads {
            self.download_file(url, filename, force_download).await?;
        }

        // Write timestamp after successful downloads
        self.write_download_timestamp()?;
        println!("✅ All MTGJSON files downloaded and timestamp updated");

        Ok(())
    }

    fn download_tcgplayer_csv(&self) -> Result<String> {
        println!("=== Downloading TCGPlayer Pricing Data ===");
        
        // Check if tcgcsv_clean.py exists
        let script_path = "tcgcsv_clean.py";
        if !Path::new(script_path).exists() {
            return Err(anyhow::anyhow!(
                "tcgcsv_clean.py not found. Please ensure the script is in the current directory."
            ));
        }

        let output_csv = Path::new(&self.data_dir).join("tcg_pricing_clean.csv");
        
        println!("Running Python script to download TCGPlayer data...");
        println!("📝 Note: This requires valid TCGPlayer session cookies in tcgcsv_clean.py");
        
        // Run the Python script
        let output = std::process::Command::new("python")
            .arg(script_path)
            .arg(&output_csv)
            .output()
            .context("Failed to execute tcgcsv_clean.py")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(anyhow::anyhow!(
                "tcgcsv_clean.py failed:\nSTDOUT: {}\nSTDERR: {}", 
                stdout, stderr
            ));
        }

        // Check if the output file was created
        if output_csv.exists() {
            println!("✓ TCGPlayer CSV downloaded to: {:?}", output_csv);
            Ok(output_csv.to_string_lossy().to_string())
        } else {
            Err(anyhow::anyhow!(
                "tcgcsv_clean.py completed but output file not found: {:?}", 
                output_csv
            ))
        }
    }

    fn load_tcgplayer_skus(&self, language_filter: &str, condition_filter: &str) -> Result<HashMap<String, Vec<TcgplayerSku>>> {
        let skus_path = Path::new(&self.data_dir).join("TcgplayerSkus.json");
        
        // Get file size for progress reporting
        let file_size = skus_path.metadata()
            .context("Failed to get file metadata")?
            .len();
        
        // Create loading progress bar for file reading/parsing
        let load_pb = ProgressBar::new_spinner();
        load_pb.set_style(ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")?);
        load_pb.set_message(format!("Loading TCGPlayer SKUs ({:.1} MB) - filtering for {} {}", 
                           file_size as f64 / 1_024.0 / 1_024.0, language_filter, condition_filter));
        
        // Read file with explicit UTF-8 encoding handling
        load_pb.set_message("Reading SKU file...");
        let file_content = std::fs::read_to_string(&skus_path)
            .context("Failed to read TcgplayerSkus.json as UTF-8. The file may have encoding issues.")?;
        
        load_pb.set_message("Parsing JSON...");
        let skus_file: TcgplayerSkusFile = serde_json::from_str(&file_content)
            .context("Failed to parse TcgplayerSkus.json. Check for JSON syntax errors or encoding issues.")?;
        
        load_pb.finish_and_clear();

        // Create progress bar for SKU filtering
        let total_cards = skus_file.data.len();
        let sku_pb = ProgressBar::new(total_cards as u64);
        sku_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} products ({eta}) - {msg}")?
            .progress_chars("#>-"));
        sku_pb.set_message("Filtering SKUs by language/condition");
        
        let mut sku_index = HashMap::new();
        let mut total_skus = 0;
        let mut filtered_skus = 0;
        let mut processed_cards = 0;
        
        for (_uuid, sku_list) in skus_file.data {
            processed_cards += 1;
            sku_pb.set_position(processed_cards as u64);
            
            for sku in sku_list {
                total_skus += 1;
                
                // Filter by specified language
                let is_correct_language = sku.language.as_ref()
                    .map(|lang| lang.eq_ignore_ascii_case(language_filter) || 
                               (language_filter.eq_ignore_ascii_case("english") && lang == "1"))
                    .unwrap_or(false);
                
                // Filter by specified condition
                let is_correct_condition = sku.condition.as_ref()
                    .map(|cond| cond.eq_ignore_ascii_case(condition_filter) || 
                               cond.eq_ignore_ascii_case(&condition_filter.replace(" ", "")) ||
                               (condition_filter.eq_ignore_ascii_case("near mint") && (cond.eq_ignore_ascii_case("nm") || cond == "1")))
                    .unwrap_or(false);
                
                if is_correct_language && is_correct_condition {
                    let product_id = sku.product_id.to_string();
                    sku_index.entry(product_id)
                        .or_insert_with(Vec::new)
                        .push(sku);
                    filtered_skus += 1;
                }
            }
        }
        
        sku_pb.finish_with_message(format!("✓ Filtered {} {} {} SKUs from {} total ({} products)", 
                                          filtered_skus, language_filter, condition_filter, total_skus, sku_index.len()));
        Ok(sku_index)
    }

    fn show_data_status(&self, max_age_hours: u64) -> Result<()> {
        println!("=== MTGJSON Data Status ===");
        
        let timestamp_file = self.get_timestamp_file_path();
        if !timestamp_file.exists() {
            println!("❌ No download timestamp found");
            println!("   Run without --status to download fresh data");
            return Ok(());
        }

        // Check timestamp
        match std::fs::read_to_string(&timestamp_file) {
            Ok(content) => {
                match content.trim().parse::<u64>() {
                    Ok(timestamp) => {
                        let current_time = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        
                        let age_seconds = current_time.saturating_sub(timestamp);
                        let age_hours = age_seconds as f64 / 3600.0;
                        
                        // Convert timestamp to readable date
                        let download_time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);
                        let datetime = chrono::DateTime::<chrono::Utc>::from(download_time);
                        
                        println!("📅 Last download: {} UTC", datetime.format("%Y-%m-%d %H:%M:%S"));
                        println!("⏱️  Data age: {:.1} hours", age_hours);
                        println!("🎯 Max age setting: {} hours", max_age_hours);
                        
                        if age_hours < max_age_hours as f64 {
                            println!("✅ Data status: FRESH");
                        } else {
                            println!("⚠️  Data status: STALE (download recommended)");
                        }
                    }
                    Err(_) => {
                        println!("⚠️  Invalid timestamp format in file");
                    }
                }
            }
            Err(_) => {
                println!("❌ Cannot read timestamp file");
            }
        }

        // Check if files exist
        println!("\n📁 File status:");
        let required_files = vec![
            ("AllPrintings.json", "Card data"),
            ("TcgplayerSkus.json", "TCGPlayer SKU mapping"), 
            ("AllDeckFiles.tar", "Preconstructed deck data")
        ];

        for (filename, description) in required_files {
            let file_path = Path::new(&self.data_dir).join(filename);
            if file_path.exists() {
                if let Ok(metadata) = file_path.metadata() {
                    let size_mb = metadata.len() / 1_024 / 1_024;
                    println!("   ✅ {} ({}) - {} MB", filename, description, size_mb);
                } else {
                    println!("   ⚠️  {} ({}) - exists but can't read size", filename, description);
                }
            } else {
                println!("   ❌ {} ({}) - MISSING", filename, description);
            }
        }

        Ok(())
    }

    fn load_tcgplayer_pricing(&self, csv_path: &str) -> Result<HashMap<String, Vec<TcgPrice>>> {
        println!("Loading TCGPlayer pricing from {}...", csv_path);
        
        let default_csv_path = Path::new(&self.data_dir).join("tcg_pricing_clean.csv");
        let file = File::open(csv_path)
            .or_else(|_| {
                println!("  Primary path failed, trying fallback: {}", default_csv_path.display());
                File::open(&default_csv_path)
            })
            .context("Failed to open TCGPlayer CSV file (tried both provided path and data/tcg_pricing_clean.csv)")?;
        
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        // Read header
        let header = lines.next()
            .ok_or_else(|| anyhow::anyhow!("Empty CSV file"))?
            .context("Failed to read header")?;
        
        println!("CSV Header: {}", header);
        
        // Parse header to find column indexes
        let columns: Vec<&str> = header.split(',').collect();
        let find_col = |name: &str| {
            columns.iter().position(|&col| {
                let trimmed = col.trim_matches('"').trim();
                trimmed == name
            })
        };
        
        let tcgplayer_id_col = find_col("TCGplayer Id").context("TCGplayer Id column not found")?;
        let product_line_col = find_col("Product Line").context("Product Line column not found")?;
        let set_name_col = find_col("Set Name").context("Set Name column not found")?;
        let product_name_col = find_col("Product Name").context("Product Name column not found")?;
        let title_col = find_col("Title").context("Title column not found")?;
        let number_col = find_col("Number").context("Number column not found")?;
        let rarity_col = find_col("Rarity").context("Rarity column not found")?;
        let condition_col = find_col("Condition").context("Condition column not found")?;
        let tcg_market_price_col = find_col("TCG Market Price");
        let tcg_direct_low_col = find_col("TCG Direct Low");
        let tcg_low_price_with_shipping_col = find_col("TCG Low Price With Shipping");
        let tcg_low_price_col = find_col("TCG Low Price");
        let total_quantity_col = find_col("Total Quantity");
        let add_to_quantity_col = find_col("Add to Quantity");
        let tcg_marketplace_price_col = find_col("TCG Marketplace Price");
        
        // Count total lines first for progress bar
        let file_for_counting = File::open(csv_path).context("Failed to open CSV file for counting")?;
        let total_lines = BufReader::new(file_for_counting).lines().count() - 1; // -1 for header
        
        // Re-open file for processing
        let file = File::open(csv_path).context("Failed to open CSV file")?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        // Skip header line
        lines.next();
        
        let pb = ProgressBar::new(total_lines as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} pricing records ({eta})")?
            .progress_chars("#>-"));

        let mut pricing_data: HashMap<String, Vec<TcgPrice>> = HashMap::new();
        let mut line_count = 0;
        
        for line in lines {
            let line = line.context("Failed to read line")?;
            if line.trim().is_empty() {
                continue;
            }
            
            let values: Vec<&str> = line.split(',').collect();
            let required_cols = [tcgplayer_id_col, product_name_col, condition_col, rarity_col];
            let max_required_col = *required_cols.iter().max().unwrap();
                
            if values.len() <= max_required_col {
                continue;
            }
            
            let get_value = |col_idx: usize| -> String {
                values.get(col_idx)
                    .unwrap_or(&"")
                    .trim_matches('"')
                    .trim()
                    .to_string()
            };
            
            let tcgplayer_id = get_value(tcgplayer_id_col);
            let product_line = get_value(product_line_col);
            let set_name = get_value(set_name_col);
            let product_name = get_value(product_name_col);
            let title = get_value(title_col);
            let number = get_value(number_col);
            let rarity = get_value(rarity_col);
            let condition = get_value(condition_col);
            
            let parse_price = |col_idx: Option<usize>| -> Option<f64> {
                col_idx.and_then(|idx| {
                    values.get(idx)
                        .and_then(|val| {
                            let clean_val = val.trim_matches('"').trim();
                            if clean_val.is_empty() { 
                                None 
                            } else { 
                                clean_val.parse::<f64>().ok() 
                            }
                        })
                        .filter(|&price| price > 0.0)
                })
            };
            
            let parse_int = |col_idx: Option<usize>| -> Option<i32> {
                col_idx.and_then(|idx| {
                    values.get(idx)
                        .and_then(|val| {
                            let clean_val = val.trim_matches('"').trim();
                            if clean_val.is_empty() { 
                                None 
                            } else { 
                                clean_val.parse::<i32>().ok() 
                            }
                        })
                })
            };
            
            let price_entry = TcgPrice {
                tcgplayer_id: tcgplayer_id.clone(),
                product_line,
                set_name,
                product_name: product_name.clone(),
                title,
                number,
                rarity,
                condition: condition.clone(),
                tcg_market_price: parse_price(tcg_market_price_col),
                tcg_direct_low: parse_price(tcg_direct_low_col),
                tcg_low_price_with_shipping: parse_price(tcg_low_price_with_shipping_col),
                tcg_low_price: parse_price(tcg_low_price_col),
                total_quantity: parse_int(total_quantity_col),
                add_to_quantity: parse_int(add_to_quantity_col),
                tcg_marketplace_price: parse_price(tcg_marketplace_price_col),
            };
            
            // Index by TCGPlayer product ID for reliable matching with MTGJSON cards
            pricing_data.entry(tcgplayer_id.clone())
                .or_insert_with(Vec::new)
                .push(price_entry);
            
            line_count += 1;
            pb.set_position(line_count as u64);
        }
        
        pb.finish_with_message("Pricing data loaded");
        println!("✓ Loaded pricing for {} product variants ({} total records)", pricing_data.len(), line_count);
        Ok(pricing_data)
    }

    fn load_deck_files(&self) -> Result<HashMap<String, IndexedDeck>> {
        // First check if AllDeckFiles directory exists
        let deck_files_path = Path::new(&self.data_dir).join("AllDeckFiles");
        if !deck_files_path.exists() {
            println!("⚠️  AllDeckFiles directory not found, skipping deck processing");
            return Ok(HashMap::new());
        }

        println!("Loading deck files from {:?}...", deck_files_path);
        
        // First pass: collect all .json files to get total count
        let deck_files: Vec<_> = walkdir::WalkDir::new(&deck_files_path)
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("json") {
                    Some(path.to_owned())
                } else {
                    None
                }
            })
            .collect();

        let pb = ProgressBar::new(deck_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} deck files ({eta})")?
            .progress_chars("#>-"));

        let mut decks = HashMap::with_capacity(deck_files.len()); // Pre-allocate capacity
        let total_processed = Arc::new(AtomicUsize::new(0));
        
        // Process deck files in batches for better memory management and progress reporting
        let deck_batches: Vec<_> = deck_files.chunks(DECK_BATCH_SIZE).collect();
        
        println!("🔄 Processing {} deck files in {} batches of {} files each", 
                deck_files.len(), deck_batches.len(), DECK_BATCH_SIZE);

        for (batch_idx, batch) in deck_batches.iter().enumerate() {
            // Process batch in parallel
            let batch_results: Vec<_> = batch.par_iter()
                .map(|path| {
                    let result = self.process_deck_file(path);
                    let count = total_processed.fetch_add(1, Ordering::Relaxed);
                    pb.set_position(count as u64 + 1);
                    (path, result)
                })
                .collect();

            // Collect successful results from this batch
            let mut batch_successes = 0;
            for (path, result) in batch_results {
                match result {
                    Ok(Some(deck)) => {
                        decks.insert(deck.uuid.clone(), deck);
                        batch_successes += 1;
                    }
                    Ok(None) => {
                        // Deck was skipped (invalid data, etc.)
                    }
                    Err(e) => {
                        // Only show first few errors to avoid spam
                        if batch_idx == 0 && batch_successes < 3 {
                            println!("⚠️  Error processing deck file {}: {}", path.display(), e);
                        }
                    }
                }
            }
            
            // Optional: Run garbage collection every few batches to manage memory
            if batch_idx % 5 == 0 && batch_idx > 0 {
                // Force garbage collection to manage memory on large datasets
                // This is optional but can help with very large deck collections
            }
        }
        
        pb.finish_with_message("Deck files loaded");

        println!("✓ Loaded {} preconstructed decks", decks.len());
        Ok(decks)
    }

    fn process_deck_file(&self, deck_path: &Path) -> Result<Option<IndexedDeck>> {
        // Use high-performance JSON loading
        let deck_file: DeckFile = self.load_json_file(deck_path)
            .context("Failed to parse deck file")?;

        let deck_data = deck_file.data;

        // Generate a UUID for the deck based on its code and name
        let deck_uuid = format!("deck_{}", uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            format!("{}_{}", deck_data.code, deck_data.name).as_bytes()
        ));

        let is_commander = !deck_data.commander.is_empty() || !deck_data.display_commander.is_empty();
        
        // Process all card sections (use display_commander first if available, otherwise commander)
        let commanders_cards = if !deck_data.display_commander.is_empty() {
            &deck_data.display_commander
        } else {
            &deck_data.commander
        };
        
        let commanders = self.process_cardset_as_deck_cards(commanders_cards);
        let main_board = self.process_cardset_as_deck_cards(&deck_data.main_board);
        let side_board = self.process_cardset_as_deck_cards(&deck_data.side_board);

        // Calculate totals
        let total_cards = commanders.iter().map(|c| c.count).sum::<u32>() +
                         main_board.iter().map(|c| c.count).sum::<u32>() +
                         side_board.iter().map(|c| c.count).sum::<u32>();

        let all_cards: Vec<&DeckCardInfo> = commanders.iter()
            .chain(main_board.iter())
            .chain(side_board.iter())
            .collect();
        let unique_cards = all_cards.iter()
            .map(|c| c.uuid.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len() as u32;

        Ok(Some(IndexedDeck {
            uuid: deck_uuid,
            name: deck_data.name,
            code: deck_data.code,
            deck_type: deck_data.deck_type.unwrap_or_else(|| "Unknown".to_string()),
            release_date: deck_data.release_date.unwrap_or_else(|| "Unknown".to_string()),
            is_commander,
            total_cards,
            unique_cards,
            commanders,
            main_board,
            side_board,
            estimated_value: None, // Will be calculated later with pricing data
        }))
    }

    fn process_deck_cards(&self, cards: &[DeckCard]) -> Vec<DeckCardInfo> {
        cards.iter().map(|card| {
            DeckCardInfo {
                uuid: card.uuid.clone(),
                name: card.name.clone(),
                count: card.count,
                is_foil: card.is_foil,
                set_code: card.set_code.clone(),
                tcgplayer_product_id: card.identifiers.tcgplayer_product_id.clone(),
            }
        }).collect()
    }

    fn process_cardset_as_deck_cards(&self, cards: &[CardSet]) -> Vec<DeckCardInfo> {
        cards.iter().map(|card| {
            DeckCardInfo {
                uuid: card.uuid.clone(),
                name: card.name.clone(),
                count: card.count,
                is_foil: card.finishes.contains(&"foil".to_string()),
                set_code: card.set_code.clone(),
                tcgplayer_product_id: card.identifiers.tcgplayer_product_id.clone(),
            }
        }).collect()
    }

    fn calculate_deck_value(
        &self,
        deck: &mut IndexedDeck,
        pricing_data: &HashMap<String, Vec<TcgPrice>>,
        sku_index: &HashMap<String, Vec<TcgplayerSku>>,
    ) {
        let mut market_total = 0.0;
        let mut direct_total = 0.0;
        let mut low_total = 0.0;
        let mut cards_with_pricing = 0;
        let mut cards_without_pricing = 0;

        let all_cards: Vec<&DeckCardInfo> = deck.commanders.iter()
            .chain(deck.main_board.iter())
            .chain(deck.side_board.iter())
            .collect();

        for card in &all_cards {
            let mut card_priced = false;
            
            // Use SKU-based pricing flow via product_id
            if let Some(product_id) = &card.tcgplayer_product_id {
                // Look up pricing data by product_id (not sku_id)
                if let Some(prices) = pricing_data.get(product_id) {
                    // If we have SKU information, try to find the best match
                    if let Some(skus) = sku_index.get(product_id) {
                        // Find the best SKU match (prefer Near Mint, English)
                        let mut best_sku: Option<&TcgplayerSku> = None;
                        
                        for sku in skus {
                            let is_near_mint = sku.condition.as_ref()
                                .map(|c| c.eq_ignore_ascii_case("near mint") || c.eq_ignore_ascii_case("nm") || c == "1")
                                .unwrap_or(false);
                            let is_english = sku.language.as_ref()
                                .map(|l| l.eq_ignore_ascii_case("english") || l == "1")
                                .unwrap_or(false);
                            
                            if is_near_mint && is_english {
                                best_sku = Some(sku);
                                break;
                            } else if best_sku.is_none() {
                                best_sku = Some(sku); // Fallback to any SKU
                            }
                        }
                        
                        // Find pricing record that matches the chosen SKU's condition
                        if let Some(sku) = best_sku {
                            let target_condition = sku.condition.as_deref().unwrap_or("Near Mint");
                            
                            let matching_price = prices.iter()
                                .find(|p| p.condition.eq_ignore_ascii_case(target_condition))
                                .or_else(|| prices.first()); // Fallback to any price
                                
                            if let Some(price) = matching_price {
                                let card_count = card.count as f64;
                                
                                if let Some(market_price) = price.tcg_market_price {
                                    market_total += market_price * card_count;
                                }
                                if let Some(direct_price) = price.tcg_direct_low {
                                    direct_total += direct_price * card_count;
                                }
                                if let Some(low_price) = price.tcg_low_price {
                                    low_total += low_price * card_count;
                                }
                                
                                cards_with_pricing += card.count;
                                card_priced = true;
                            }
                        }
                    } else {
                        // No SKU data available, use any price record
                        if let Some(price) = prices.first() {
                            let card_count = card.count as f64;
                            
                            if let Some(market_price) = price.tcg_market_price {
                                market_total += market_price * card_count;
                            }
                            if let Some(direct_price) = price.tcg_direct_low {
                                direct_total += direct_price * card_count;
                            }
                            if let Some(low_price) = price.tcg_low_price {
                                low_total += low_price * card_count;
                            }
                            
                            cards_with_pricing += card.count;
                            card_priced = true;
                        }
                    }
                }
            }
            
            if !card_priced {
                cards_without_pricing += card.count;
            }
        }

        deck.estimated_value = Some(DeckValue {
            market_total,
            direct_total,
            low_total,
            cards_with_pricing,
            cards_without_pricing,
        });
    }

    fn process_card(
        &self,
        card: &CardSet,
        set_code: &str,
        set_name: &str,
        release_date: &str,
        sku_index: &HashMap<String, Vec<TcgplayerSku>>,
        _pricing_data: &HashMap<String, Vec<TcgPrice>>,
    ) -> IndexedCard {
        let tcgplayer_product_id = card.identifiers.tcgplayer_product_id.clone();
        
        // Get TCGPlayer SKUs if available
        let tcgplayer_skus = if let Some(product_id) = &tcgplayer_product_id {
            sku_index.get(product_id).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };

        IndexedCard {
            uuid: card.uuid.clone(),
            name: card.name.clone(),
            set_code: set_code.to_string(),
            set_name: set_name.to_string(),
            collector_number: card.number.clone(),
            rarity: card.rarity.clone(),
            mana_value: card.mana_value,
            mana_cost: card.mana_cost.clone(),
            colors: card.colors.clone(),
            color_identity: card.color_identity.clone(),
            types: card.types.clone(),
            subtypes: card.subtypes.clone(),
            supertypes: card.supertypes.clone(),
            power: card.power.clone(),
            toughness: card.toughness.clone(),
            loyalty: card.loyalty.clone(),
            defense: card.defense.clone(),
            text: card.text.clone(),
            flavor_text: card.flavor_text.clone(),
            layout: card.layout.clone(),
            availability: card.availability.clone(),
            finishes: card.finishes.clone(),
            has_foil: card.has_foil,
            has_non_foil: card.has_non_foil,
            is_reserved: card.is_reserved.unwrap_or(false),
            is_promo: card.is_promo.unwrap_or(false),
            release_date: release_date.to_string(),
            scryfall_oracle_id: card.identifiers.scryfall_oracle_id.clone(),
            scryfall_id: card.identifiers.scryfall_id.clone(),
            tcgplayer_product_id,
            tcgplayer_skus,
            purchase_urls: card.purchase_urls.clone(),
        }
    }

    fn index_cards(&self, tcg_csv_path: Option<&str>, skip_pricing: bool, auto_download_tcg: bool, sku_language: &str, sku_condition: &str) -> Result<()> {
        println!("=== Starting MTGJSON Card Indexing ===");

        // Connect to Redis
        let mut con = self.redis_client.get_connection()
            .context("Failed to connect to Redis")?;
        
        // Test connection
        let _: String = redis::cmd("PING").query(&mut con)
            .context("Redis connection test failed")?;
        
        println!("✓ Connected to Redis");

        // Load TCGPlayer SKUs
        let sku_index = self.load_tcgplayer_skus(sku_language, sku_condition)?;

        // Load TCGPlayer pricing if provided or auto-download if requested
        let pricing_data = if !skip_pricing {
            if let Some(csv_path) = tcg_csv_path {
                // User provided explicit CSV path
                if Path::new(csv_path).exists() {
                    println!("Loading TCGPlayer pricing data from: {}", csv_path);
                    self.load_tcgplayer_pricing(csv_path)?
                } else {
                    println!("❌ TCGPlayer CSV file not found: {}", csv_path);
                    println!("   To obtain TCGPlayer pricing data:");
                    println!("   • Export from your TCGPlayer seller account");
                    println!("   • Use TCGPlayer API with authentication");
                    println!("   • Use --auto-download-tcg flag with tcgcsv_clean.py");
                    println!("   ℹ️  Continuing without pricing data...");
                    HashMap::new()
                }
            } else if auto_download_tcg {
                // Auto-download using Python script
                match self.download_tcgplayer_csv() {
                    Ok(downloaded_csv_path) => {
                        println!("Loading downloaded TCGPlayer pricing data...");
                        self.load_tcgplayer_pricing(&downloaded_csv_path)?
                    }
                    Err(e) => {
                        println!("❌ Failed to auto-download TCGPlayer data: {}", e);
                        println!("   Make sure tcgcsv_clean.py has valid session cookies");
                        println!("   ℹ️  Continuing without pricing data...");
                        HashMap::new()
                    }
                }
            } else {
                println!("ℹ️  No TCGPlayer CSV provided, continuing without pricing data");
                println!("   Options:");
                println!("   • Use --tcg-csv-path to specify existing CSV file");
                println!("   • Use --auto-download-tcg to download with tcgcsv_clean.py");
                HashMap::new()
            }
        } else {
            println!("⚠️  Skipping TCGPlayer pricing data (--skip-pricing enabled)");
            HashMap::new()
        };

        // Load deck files
        let mut decks = self.load_deck_files()?;

        // Load AllPrintings.json with high-performance memory mapping
        let all_printings_path = Path::new(&self.data_dir).join("AllPrintings.json");
        let file_size = std::fs::metadata(&all_printings_path)?.len();
        
        println!("📖 Loading AllPrintings.json ({:.2} MB) with memory mapping...", 
                file_size as f64 / 1024.0 / 1024.0);
        
        let all_printings: AllPrintingsFile = self.load_json_file(&all_printings_path)
            .context("Failed to parse AllPrintings.json")?;

        let sets_data = all_printings.data;
        println!("✓ Loaded {} sets", sets_data.len());

        // Clear existing data
        self.clear_redis_data(&mut con)?;

        // Process all cards
        let total_cards: usize = sets_data.values()
            .map(|set| set.cards.len())
            .sum();
        
        println!("Processing {} total cards...", total_cards);

        let pb = ProgressBar::new(total_cards as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards ({eta})")?
            .progress_chars("#>-"));

        let mut processed_cards = 0;
        let mut all_indexed_cards = Vec::with_capacity(total_cards); // Collect all cards for search indexing
        
        // Process sets with performance monitoring
        let start_time = std::time::Instant::now();
        let mut sets_processed = 0;
        
        for (set_code, set_data) in sets_data {
            sets_processed += 1;
            // Store set metadata
            let set_info = SetInfo {
                code: set_code.clone(),
                name: set_data.name.clone(),
                release_date: set_data.release_date.clone(),
                set_type: set_data.set_type.clone(),
                total_cards: set_data.cards.len(),
                base_set_size: set_data.base_set_size,
            };

            let set_json = serde_json::to_string(&set_info)?;
            let _: () = con.set(format!("set:{}", set_code), set_json)?;

            // Process cards in batches
            for card_batch in set_data.cards.chunks(BATCH_SIZE) {
                let mut cards = Vec::new();
                
                for card in card_batch {
                    let indexed_card = self.process_card(
                        card,
                        &set_code,
                        &set_data.name,
                        &set_data.release_date,
                        &sku_index,
                        &pricing_data,
                    );
                    all_indexed_cards.push(indexed_card.clone());
                    cards.push(indexed_card);
                }

                self.store_cards_batch(&mut con, cards, &pricing_data, &sku_index)?;
                processed_cards += card_batch.len();
                pb.set_position(processed_cards as u64);
            }
        }

        pb.finish_with_message("Card storage complete");
        
        // Create RediSearch indexes for fast search and autocomplete
        self.create_redisearch_indexes(&mut con)?;

        // Process decks with or without pricing information
        if !decks.is_empty() {
            let pricing_status = if !pricing_data.is_empty() { "with pricing" } else { "without pricing" };
            println!("Processing {} deck files {}...", decks.len(), pricing_status);
            
            let deck_pb = ProgressBar::new(decks.len() as u64);
            deck_pb.set_style(ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} decks ({eta})")?
                .progress_chars("#>-"));

            let mut processed_decks = 0;
            
            // Calculate deck values and store in batches
            for deck_batch in decks.values_mut().collect::<Vec<_>>().chunks_mut(BATCH_SIZE) {
                for deck in deck_batch.iter_mut() {
                    self.calculate_deck_value(deck, &pricing_data, &sku_index);
                }

                let deck_batch_vec: Vec<IndexedDeck> = deck_batch.iter().map(|d| (*d).clone()).collect();
                self.store_decks_batch(&mut con, deck_batch_vec)?;
                
                processed_decks += deck_batch.len();
                deck_pb.set_position(processed_decks as u64);
            }

            deck_pb.finish_with_message("Deck processing complete");
            println!("✓ Processed {} decks", processed_decks);
        }

        // Store metadata
        let index_stats = IndexStats {
            total_sets: 0, // Will be updated
            total_cards,
            processed_cards,
            last_update: Utc::now().to_rfc3339(),
            source: "mtgjson".to_string(),
            version: all_printings.meta.version,
        };

        self.store_index_stats(&mut con, index_stats)?;

        pb.finish_with_message("Indexing complete");
        
        let total_time = start_time.elapsed();
        let cards_per_sec = processed_cards as f64 / total_time.as_secs_f64();
        
        println!("\n=== Indexing Complete ===");
        println!("🚀 Performance Summary:");
        println!("   • Processed {} cards in {:.2}s ({:.0} cards/sec)", 
                processed_cards, total_time.as_secs_f64(), cards_per_sec);
        println!("   • Indexed {} sets across {} threads", sets_processed, rayon::current_num_threads());
        println!("   • Indexed {} preconstructed decks", decks.len());
        if !pricing_data.is_empty() {
            println!("   • Integrated pricing for {} product variants", pricing_data.len());
        }
        println!("   • Batch size: {} cards/batch", BATCH_SIZE);
        println!("   • Memory optimization: {}", if file_size > MEMORY_MAP_THRESHOLD { "Memory-mapped JSON" } else { "Buffered reading" });
        
        Ok(())
    }

    fn clear_redis_data(&self, con: &mut Connection) -> Result<()> {
        println!("Clearing existing Redis data...");
        
        // Drop RediSearch indexes first
        let indexes = vec![
            "mtg:cards:idx",
            "mtg:decks:idx", 
            "mtg:sets:idx"
        ];
        
        for index in indexes {
            let _: Result<String, redis::RedisError> = redis::cmd("FT.DROPINDEX")
                .arg(index)
                .arg("DD") // Delete documents
                .query(con);
            println!("  ✓ Dropped RediSearch index: {}", index);
        }
        
        // Clear suggestion dictionaries
        let _: Result<i64, redis::RedisError> = redis::cmd("FT.SUGDEL")
            .arg("mtg:cards:names")
            .arg("*")
            .query(con);
        
        // Clear remaining key patterns
        let patterns = vec![
            "mtg:*", "card:*", "set:*", "name:*", 
            "uuid:*", "oracle:*", "tcgplayer:*", "sku:*", "price:*",
            "deck:*", "auto:*", "ngram:*", "metaphone:*", "word:*",
            "price_range:*"
        ];

        for pattern in patterns {
            let keys: Vec<String> = con.keys(pattern)
                .context("Failed to get keys")?;
            
            if !keys.is_empty() {
                let _: () = con.del(&keys)
                    .context("Failed to delete keys")?;
                println!("  ✓ Cleared {} keys matching {}", keys.len(), pattern);
            }
        }

        Ok(())
    }

    fn create_redisearch_indexes(&self, con: &mut Connection) -> Result<()> {
        println!("Creating RediSearch indexes...");
        
        // Create main card index
        let card_index_result: Result<String, redis::RedisError> = redis::cmd("FT.CREATE")
            .arg("mtg:cards:idx")
            .arg("ON").arg("JSON")
            .arg("PREFIX").arg(1).arg("mtg:cards:data:")
            .arg("SCHEMA")
            .arg("$.uuid").arg("AS").arg("uuid").arg("TEXT").arg("NOSTEM").arg("SORTABLE")
            .arg("$.name").arg("AS").arg("name").arg("TEXT").arg("PHONETIC").arg("dm:en").arg("SORTABLE")
            .arg("$.set_code").arg("AS").arg("set_code").arg("TAG").arg("SORTABLE")
            .arg("$.set_name").arg("AS").arg("set_name").arg("TEXT").arg("SORTABLE")
            .arg("$.mana_value").arg("AS").arg("mana_value").arg("NUMERIC").arg("SORTABLE")
            .arg("$.types").arg("AS").arg("types").arg("TAG").arg("SEPARATOR").arg(" ")
            .arg("$.colors").arg("AS").arg("colors").arg("TAG").arg("SEPARATOR").arg(",")
            .arg("$.color_identity").arg("AS").arg("color_identity").arg("TAG").arg("SEPARATOR").arg(",")
            .arg("$.rarity").arg("AS").arg("rarity").arg("TAG").arg("SORTABLE")
            .arg("$.tcgplayer_product_id").arg("AS").arg("tcg_product").arg("NUMERIC")
            .arg("$.text").arg("AS").arg("oracle_text").arg("TEXT")
            .arg("$.release_date").arg("AS").arg("release_date").arg("TEXT").arg("SORTABLE")
            .query(con);
            
        match card_index_result {
            Ok(_) => println!("  ✓ Created mtg:cards:idx"),
            Err(e) => println!("  ⚠ Card index creation failed: {}", e),
        }
        
        // Create deck index
        let deck_index_result: Result<String, redis::RedisError> = redis::cmd("FT.CREATE")
            .arg("mtg:decks:idx")
            .arg("ON").arg("JSON")
            .arg("PREFIX").arg(1).arg("mtg:decks:data:")
            .arg("SCHEMA")
            .arg("$.uuid").arg("AS").arg("uuid").arg("TEXT").arg("NOSTEM")
            .arg("$.name").arg("AS").arg("name").arg("TEXT").arg("PHONETIC").arg("dm:en").arg("SORTABLE")
            .arg("$.deck_type").arg("AS").arg("deck_type").arg("TAG").arg("SORTABLE")
            .arg("$.estimated_value.market_total").arg("AS").arg("market_value").arg("NUMERIC").arg("SORTABLE")
            .arg("$.total_cards").arg("AS").arg("total_cards").arg("NUMERIC").arg("SORTABLE")
            .arg("$.is_commander").arg("AS").arg("is_commander").arg("TAG")
            .arg("$.release_date").arg("AS").arg("release_date").arg("TEXT").arg("SORTABLE")
            .query(con);
            
        match deck_index_result {
            Ok(_) => println!("  ✓ Created mtg:decks:idx"),
            Err(e) => println!("  ⚠ Deck index creation failed: {}", e),
        }
        
        // Create set index  
        let set_index_result: Result<String, redis::RedisError> = redis::cmd("FT.CREATE")
            .arg("mtg:sets:idx")
            .arg("ON").arg("JSON")
            .arg("PREFIX").arg(1).arg("mtg:sets:data:")
            .arg("SCHEMA")
            .arg("$.code").arg("AS").arg("code").arg("TAG").arg("SORTABLE")
            .arg("$.name").arg("AS").arg("name").arg("TEXT").arg("SORTABLE")
            .arg("$.set_type").arg("AS").arg("set_type").arg("TAG").arg("SORTABLE")
            .arg("$.release_date").arg("AS").arg("release_date").arg("TEXT").arg("SORTABLE")
            .arg("$.total_cards").arg("AS").arg("total_cards").arg("NUMERIC").arg("SORTABLE")
            .query(con);
            
        match set_index_result {
            Ok(_) => println!("  ✓ Created mtg:sets:idx"),
            Err(e) => println!("  ⚠ Set index creation failed: {}", e),
        }
        
        println!("✅ RediSearch indexes created");
        Ok(())
    }

        fn store_cards_batch(
        &self,
        con: &mut Connection,
        cards: Vec<IndexedCard>,
        pricing_data: &HashMap<String, Vec<TcgPrice>>,
        sku_index: &HashMap<String, Vec<TcgplayerSku>>,
    ) -> Result<()> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        let timestamp = chrono::Utc::now().timestamp();

        for card in &cards {
            let card_json = serde_json::to_string(card)
                .context("Failed to serialize card")?;

            // Store as RediSearch JSON document - this replaces ALL manual indexing
            pipe.cmd("JSON.SET")
                .arg(format!("mtg:cards:data:{}", card.uuid))
                .arg("$")
                .arg(&card_json);

            // TCGPlayer pricing chain - optimized for search performance
            if let Some(product_id) = &card.tcgplayer_product_id {
                // Direct UUID -> ProductID mapping (step 1 of pricing chain)
                pipe.cmd("SET").arg(format!("mtg:tcg:uuid_to_product:{}", card.uuid)).arg(product_id);
                pipe.cmd("SADD").arg(format!("mtg:tcg:product_cards:{}", product_id)).arg(&card.uuid);
                
                // Store SKU-based pricing (steps 2-3 of pricing chain)
                if let Some(skus) = sku_index.get(product_id) {
                    for sku in skus {
                        let sku_id = sku.sku_id.to_string();
                        
                        // ProductID -> SKU mappings (step 2)
                        pipe.cmd("SADD").arg(format!("mtg:tcg:product_skus:{}", product_id)).arg(&sku_id);
                        
                        // SKU metadata for condition/language filtering
                        let sku_meta = serde_json::json!({
                            "condition": sku.condition.clone().unwrap_or_else(|| "Near Mint".to_string()),
                            "language": sku.language.clone().unwrap_or_else(|| "English".to_string()),
                            "foil": sku.printing.as_deref() == Some("Foil"),
                            "product_id": product_id,
                            "card_uuid": card.uuid
                        });
                        
                        pipe.cmd("JSON.SET")
                            .arg(format!("mtg:tcg:sku_meta:{}", sku_id))
                            .arg("$")
                            .arg(sku_meta.to_string());
                        
                        // SKU pricing data (step 3 - final pricing)
                        if let Some(prices) = pricing_data.get(&sku_id) {
                            for price in prices {
                                let price_json = serde_json::json!({
                                    "sku_id": sku_id,
                                    "tcg_market_price": price.tcg_market_price,
                                    "tcg_direct_low": price.tcg_direct_low,
                                    "tcg_low_price": price.tcg_low_price,
                                    "condition": price.condition,
                                    "timestamp": timestamp
                                });
                                
                                // Latest pricing (key for quick lookups)
                                pipe.cmd("JSON.SET")
                                    .arg(format!("mtg:tcg:sku_price:{}", sku_id))
                                    .arg("$")
                                    .arg(price_json.to_string());
                                
                                // Historical pricing for trends
                                if let Some(market_price) = price.tcg_market_price {
                                    pipe.cmd("ZADD")
                                        .arg(format!("mtg:tcg:price_history:{}", sku_id))
                                        .arg(timestamp)
                                        .arg(market_price);
                                }
                            }
                        }
                    }
                }
            }

            // Also handle cards with direct SKUs (no product_id)
            for sku in &card.tcgplayer_skus {
                let sku_id = sku.sku_id.to_string();
                
                if card.tcgplayer_product_id.is_none() {
                    // Direct UUID -> SKU mapping for cards without product_id
                    pipe.cmd("SADD").arg(format!("mtg:tcg:uuid_skus:{}", card.uuid)).arg(&sku_id);
                    
                    let sku_meta = serde_json::json!({
                        "condition": sku.condition.clone().unwrap_or_else(|| "Near Mint".to_string()),
                        "language": sku.language.clone().unwrap_or_else(|| "English".to_string()),
                        "foil": sku.printing.as_deref() == Some("Foil"),
                        "product_id": sku.product_id,
                        "card_uuid": card.uuid
                    });
                    
                    pipe.cmd("JSON.SET")
                        .arg(format!("mtg:tcg:sku_meta:{}", sku_id))
                        .arg("$")
                        .arg(sku_meta.to_string());
                }
            }
        }

        // Execute pipeline
        let _: () = pipe.query(con)
            .context("Failed to execute Redis pipeline")?;

        // Build autocomplete suggestions separately for better performance
        self.build_autocomplete_suggestions(con, &cards)?;

        Ok(())
    }
    
    fn build_autocomplete_suggestions(&self, con: &mut Connection, cards: &[IndexedCard]) -> Result<()> {
        for card in cards {
            // Add card name to suggestions with score based on popularity/rarity
            let score = match card.rarity.as_str() {
                "mythic" => 10.0,
                "rare" => 8.0,
                "uncommon" => 5.0,
                "common" => 3.0,
                _ => 1.0,
            };
            
            // Add to FT.SUGADD for fast autocomplete
            let _: Result<i64, redis::RedisError> = redis::cmd("FT.SUGADD")
                .arg("mtg:autocomplete:names")
                .arg(&card.name)
                .arg(score)
                .arg("PAYLOAD")
                .arg(&card.uuid)
                .query(con);
        }
        
        Ok(())
    }

    fn store_decks_batch(
        &self,
        con: &mut Connection,
        decks: Vec<IndexedDeck>,
    ) -> Result<()> {
        let mut pipe = redis::pipe();
        pipe.atomic();

        for deck in &decks {
            let deck_json = serde_json::to_string(deck)
                .context("Failed to serialize deck")?;

            // Store as RediSearch JSON document
            pipe.cmd("JSON.SET")
                .arg(format!("mtg:decks:data:{}", deck.uuid))
                .arg("$")
                .arg(&deck_json);

            // Store deck composition with card quantities
            let all_cards: Vec<&DeckCardInfo> = deck.commanders.iter()
                .chain(deck.main_board.iter())
                .chain(deck.side_board.iter())
                .collect();
            
            for card in &all_cards {
                // Store card-deck relationships with quantities
                pipe.cmd("ZADD")
                    .arg(format!("mtg:decks:cards:{}", deck.uuid))
                    .arg(card.count)
                    .arg(&card.uuid);
                    
                // Store deck-card relationships (reverse lookup)
                pipe.cmd("SADD")
                    .arg(format!("mtg:cards:decks:{}", card.uuid))
                    .arg(&deck.uuid);
            }

            // Store commanders separately for EDH/Commander format
            for commander in &deck.commanders {
                pipe.cmd("SADD")
                    .arg(format!("mtg:decks:commanders:{}", deck.uuid))
                    .arg(&commander.uuid);
                    
                // Index by commander for finding all decks with specific commanders
                pipe.cmd("SADD")
                    .arg(format!("mtg:commanders:decks:{}", commander.uuid))
                    .arg(&deck.uuid);
            }
        }

        let _: () = pipe.query(con)
            .context("Failed to execute Redis pipeline for decks")?;

        Ok(())
    }

    fn add_deck_search_indexes(&self, pipe: &mut redis::Pipeline, name: &str, deck_uuid: &str) {
        let name_lower = name.to_lowercase();
        
        // Add word-based indexes for deck names
        for word in name_lower.split_whitespace() {
            if word.len() >= 2 {
                pipe.cmd("SADD").arg(format!("deck:word:{}", word)).arg(deck_uuid);
            }
        }
    }

    fn get_value_bucket(value: f64) -> String {
        match value {
            v if v < 25.0 => "under_25".to_string(),
            v if v < 50.0 => "25_to_50".to_string(),
            v if v < 100.0 => "50_to_100".to_string(),
            v if v < 200.0 => "100_to_200".to_string(),
            v if v < 500.0 => "200_to_500".to_string(),
            _ => "over_500".to_string(),
        }
    }

    fn get_price_bucket(price: f64) -> String {
        match price {
            p if p < 1.0 => "under_1".to_string(),
            p if p < 5.0 => "1_to_5".to_string(),
            p if p < 10.0 => "5_to_10".to_string(),
            p if p < 25.0 => "10_to_25".to_string(),
            p if p < 50.0 => "25_to_50".to_string(),
            p if p < 100.0 => "50_to_100".to_string(),
            p if p < 500.0 => "100_to_500".to_string(),
            _ => "over_500".to_string(),
        }
    }

    fn add_enhanced_search_indexes(&self, pipe: &mut redis::Pipeline, name: &str, uuid: &str) {
        let name_lower = name.to_lowercase();
        
        // Add word-based indexes with improved autocomplete
        for word in self.tokenize_words(&name_lower) {
            pipe.cmd("SADD").arg(format!("word:{}", word)).arg(uuid);
            
            // Enhanced autocomplete with word-level and character-level prefixes
            let chars: Vec<char> = word.chars().collect();
            let prefix_limit = chars.len().min(MAX_PREFIX_LENGTH);
            
            for i in 1..=prefix_limit {
                let prefix: String = chars[..i].iter().collect();
                pipe.cmd("SADD").arg(format!("auto:prefix:{}", prefix)).arg(uuid);
            }
        }

        // Add comprehensive autocomplete for full name
        let name_chars: Vec<char> = name_lower.chars().collect();
        let name_prefix_limit = name_chars.len().min(MAX_PREFIX_LENGTH);
        
        for i in 1..=name_prefix_limit {
            let prefix: String = name_chars[..i].iter().collect();
            pipe.cmd("SADD").arg(format!("auto:prefix:{}", prefix)).arg(uuid);
        }

        // Add n-grams for fuzzy matching 
        for ngram in self.generate_ngrams(&name_lower, NGRAM_SIZE) {
            pipe.cmd("SADD").arg(format!("ngram:{}", ngram)).arg(uuid);
        }

        // Add metaphone for phonetic matching
        let metaphone = self.generate_metaphone(&name_lower);
        if !metaphone.is_empty() {
            pipe.cmd("SADD").arg(format!("metaphone:{}", metaphone)).arg(uuid);
        }
    }

    fn build_and_store_search_indexes(&self, con: &mut Connection, indexed_cards: &[IndexedCard]) -> Result<()> {
        println!("Building comprehensive search indexes for {} cards...", indexed_cards.len());
        
        let start_time = std::time::Instant::now();
        let pb = ProgressBar::new(indexed_cards.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} cards processed ({eta})")?
            .progress_chars("#>-"));

        // Process in smaller batches to avoid memory issues with large datasets
        const SEARCH_BATCH_SIZE: usize = 1000;
        let mut search_indexes = SearchIndexes::default();
        
        for card_batch in indexed_cards.chunks(SEARCH_BATCH_SIZE) {
            for card in card_batch {
                let name_lower = card.name.to_lowercase();
                
                // Build n-grams
                for ngram in self.generate_ngrams(&name_lower, NGRAM_SIZE) {
                    search_indexes.ngrams.entry(ngram)
                        .or_insert_with(HashSet::new)
                        .insert(card.uuid.clone());
                }
                
                // Build metaphones
                let metaphone = self.generate_metaphone(&name_lower);
                if !metaphone.is_empty() {
                    search_indexes.metaphones.entry(metaphone)
                        .or_insert_with(HashSet::new)
                        .insert(card.uuid.clone());
                }
                
                // Build word indexes
                for word in self.tokenize_words(&name_lower) {
                    search_indexes.words.entry(word)
                        .or_insert_with(HashSet::new)
                        .insert(card.uuid.clone());
                }
                
                pb.inc(1);
            }
        }

        pb.finish_with_message("Search index building complete");
        
        // Store the search indexes in Redis
        self.store_search_indexes(con, search_indexes)?;
        
        // Store the advanced fuzzy search Lua script
        self.store_fuzzy_search_script(con)?;
        
        let elapsed = start_time.elapsed();
        println!("✅ Advanced search indexes built and stored in {:.2}s", elapsed.as_secs_f32());
        
        Ok(())
    }

    fn store_search_indexes(&self, con: &mut Connection, search_indexes: SearchIndexes) -> Result<()> {
        println!("Storing search indexes in Redis...");
        
        // Store n-grams
        println!("  📝 Storing {} n-gram indexes...", search_indexes.ngrams.len());
        let ngram_pb = ProgressBar::new(search_indexes.ngrams.len() as u64);
        ngram_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} n-grams")?
            .progress_chars("#>-"));

        for (ngram, card_uuids) in search_indexes.ngrams {
            let uuids_vec: Vec<String> = card_uuids.into_iter().collect();
            
            // Store in chunks to avoid Redis memory limits
            const CHUNK_SIZE: usize = 1000;
            for chunk in uuids_vec.chunks(CHUNK_SIZE) {
                let _: () = con.sadd(format!("ngram:{}", ngram), chunk)
                    .context("Failed to store n-gram index")?;
            }
            ngram_pb.inc(1);
        }
        ngram_pb.finish_with_message("N-gram indexes stored");

        // Store metaphones
        println!("  🔊 Storing {} metaphone indexes...", search_indexes.metaphones.len());
        let metaphone_pb = ProgressBar::new(search_indexes.metaphones.len() as u64);
        metaphone_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} metaphones")?
            .progress_chars("#>-"));

        for (metaphone, card_uuids) in search_indexes.metaphones {
            let uuids_vec: Vec<String> = card_uuids.into_iter().collect();
            
            const CHUNK_SIZE: usize = 1000;
            for chunk in uuids_vec.chunks(CHUNK_SIZE) {
                let _: () = con.sadd(format!("metaphone:{}", metaphone), chunk)
                    .context("Failed to store metaphone index")?;
            }
            metaphone_pb.inc(1);
        }
        metaphone_pb.finish_with_message("Metaphone indexes stored");

        // Store words
        println!("  📚 Storing {} word indexes...", search_indexes.words.len());
        let word_pb = ProgressBar::new(search_indexes.words.len() as u64);
        word_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} words")?
            .progress_chars("#>-"));

        for (word, card_uuids) in search_indexes.words {
            let uuids_vec: Vec<String> = card_uuids.into_iter().collect();
            
            const CHUNK_SIZE: usize = 1000;
            for chunk in uuids_vec.chunks(CHUNK_SIZE) {
                let _: () = con.sadd(format!("word:{}", word), chunk)
                    .context("Failed to store word index")?;
            }
            word_pb.inc(1);
        }
        word_pb.finish_with_message("Word indexes stored");

        Ok(())
    }

    fn store_fuzzy_search_script(&self, con: &mut Connection) -> Result<()> {
        println!("📜 Loading enhanced fuzzy search Lua script...");
        
        // Enhanced fuzzy search script optimized for MTGJSON data
        let fuzzy_search_script = r#"
        local query = ARGV[1]
        local max_distance = tonumber(ARGV[2]) or 2
        local max_results = tonumber(ARGV[3]) or 20
        
        local candidates = {}
        local results = {}
        
        -- Function to calculate simple edit distance for small strings
        local function edit_distance(s1, s2)
            if #s1 == 0 then return #s2 end
            if #s2 == 0 then return #s1 end
            
            local matrix = {}
            for i = 0, #s1 do
                matrix[i] = {[0] = i}
            end
            for j = 0, #s2 do
                matrix[0][j] = j
            end
            
            for i = 1, #s1 do
                for j = 1, #s2 do
                    local cost = (s1:sub(i,i) == s2:sub(j,j)) and 0 or 1
                    matrix[i][j] = math.min(
                        matrix[i-1][j] + 1,
                        matrix[i][j-1] + 1,
                        matrix[i-1][j-1] + cost
                    )
                end
            end
            
            return matrix[#s1][#s2]
        end
        
        -- First try exact prefix matches (highest priority)
        local query_lower = query:lower()
        local prefix_key = 'auto:prefix:' .. query_lower
        local prefix_matches = redis.call('SMEMBERS', prefix_key)
        
        for _, uuid in ipairs(prefix_matches) do
            candidates[uuid] = (candidates[uuid] or 0) + 10  -- High score for prefix matches
            if #results < max_results then
                table.insert(results, uuid)
            end
        end
        
        -- If we have enough exact prefix matches, return early
        if #results >= max_results then
            return results
        end
        
        -- Try word-based matching for multi-word queries
        local words = {}
        for word in query_lower:gmatch('%S+') do
            if #word >= 2 then
                table.insert(words, word)
            end
        end
        
        for _, word in ipairs(words) do
            local word_key = 'word:' .. word
            local word_matches = redis.call('SMEMBERS', word_key)
            
            for _, uuid in ipairs(word_matches) do
                candidates[uuid] = (candidates[uuid] or 0) + 5  -- Good score for word matches
            end
        end
        
        -- Try n-gram fuzzy matching for partial matches
        if #query_lower >= 3 then
            local ngram_scores = {}
            
            for i = 1, #query_lower - 2 do
                local ngram = query_lower:sub(i, i + 2)
                local ngram_key = 'ngram:' .. ngram
                local ngram_matches = redis.call('SMEMBERS', ngram_key)
                
                for _, uuid in ipairs(ngram_matches) do
                    ngram_scores[uuid] = (ngram_scores[uuid] or 0) + 1
                end
            end
            
            -- Only add n-gram matches that have sufficient overlap
            local min_ngram_score = math.max(1, math.floor((#query_lower - 2) * 0.3))
            for uuid, score in pairs(ngram_scores) do
                if score >= min_ngram_score then
                    candidates[uuid] = (candidates[uuid] or 0) + score
                end
            end
        end
        
        -- Try metaphone matching for phonetic similarity
        local function simple_metaphone(text)
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
            
            for i = 1, #text do
                local char = text:sub(i, i):lower()
                local code = map[char] or ""
                result = result .. code
            end
            
            return result
        end
        
        local metaphone = simple_metaphone(query_lower)
        if #metaphone > 0 then
            local metaphone_key = 'metaphone:' .. metaphone
            local metaphone_matches = redis.call('SMEMBERS', metaphone_key)
            
            for _, uuid in ipairs(metaphone_matches) do
                candidates[uuid] = (candidates[uuid] or 0) + 3  -- Moderate score for phonetic matches
            end
        end
        
        -- Convert candidates to sorted array
        local candidate_array = {}
        for uuid, score in pairs(candidates) do
            table.insert(candidate_array, {uuid = uuid, score = score})
        end
        
        -- Sort by score (higher is better)
        table.sort(candidate_array, function(a, b) return a.score > b.score end)
        
        -- Build final results list
        local final_results = {}
        for i = 1, math.min(#candidate_array, max_results) do
            table.insert(final_results, candidate_array[i].uuid)
        end
        
        return final_results
        "#;
        
        let script_sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(fuzzy_search_script)
            .query(con)
            .context("Failed to load fuzzy search script")?;
        
        let _: () = con.set("mtgjson:script:fuzzy_search", script_sha)
            .context("Failed to store script SHA")?;
            
        println!("✅ Enhanced fuzzy search script loaded and ready");
        
        Ok(())
    }

    fn store_index_stats(&self, con: &mut Connection, stats: IndexStats) -> Result<()> {
        let stats_json = serde_json::to_string(&stats)
            .context("Failed to serialize index stats")?;
        
        let _: () = con.set("mtgjson:stats", stats_json)
            .context("Failed to store index stats")?;
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let indexer = MTGJSONIndexer::new(&cli.redis_host, cli.redis_port, cli.data_dir)?;

    // Handle status command
    if cli.status {
        indexer.show_data_status(cli.max_age_hours)?;
        return Ok(());
    }

    if !cli.index_only {
        indexer.download_data_files(cli.force_download, cli.max_age_hours).await?;
    }

    if !cli.download_only {
        indexer.index_cards(cli.tcg_csv_path.as_deref(), cli.skip_pricing, cli.auto_download_tcg, &cli.sku_language, &cli.sku_condition)?;
    }

    println!("✓ All operations completed successfully!");
    Ok(())
} 