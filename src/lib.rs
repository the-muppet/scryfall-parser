use pyo3::prelude::*;
use pyo3::types::PyDict;

mod main;
use main::*;

// API modules
pub mod redis_client;

/// Download Scryfall data and build indexes
#[pyfunction]
fn download_and_index(redis_url: Option<String>) -> PyResult<String> {
    let redis_url = redis_url.unwrap_or_else(|| "redis://127.0.0.1:9999".to_string());
    
    match run_indexer(&redis_url) {
        Ok(stats) => Ok(format!(
            "Successfully indexed {} cards with {} sets", 
            stats.card_count, 
            stats.set_count
        )),
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Indexing failed: {}", e
        ))),
    }
}

/// Search for cards using fuzzy matching
#[pyfunction]
fn search_cards(
    query: String,
    max_results: Option<usize>,
    redis_url: Option<String>,
) -> PyResult<Vec<PyObject>> {
    let redis_url = redis_url.unwrap_or_else(|| "redis://127.0.0.1:9999".to_string());
    let max_results = max_results.unwrap_or(20);
    
    Python::with_gil(|py| {
        match search_cards_internal(&query, max_results, &redis_url) {
            Ok(results) => {
                let py_results: PyResult<Vec<PyObject>> = results
                    .into_iter()
                    .map(|card| {
                        let dict = PyDict::new(py);
                        dict.set_item("id", &card.id)?;
                        dict.set_item("oracle_id", &card.oracle_id)?;
                        dict.set_item("name", &card.name)?;
                        dict.set_item("sets", &card.sets)?;
                        dict.set_item("layout", &card.layout)?;
                        dict.set_item("tcgplayer_ids", &card.tcgplayer_ids)?;
                        dict.set_item("main_image", &card.main_image)?;
                        Ok(dict.into())
                    })
                    .collect();
                py_results
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Search failed: {}", e
            ))),
        }
    })
}

/// Get card details by oracle ID
#[pyfunction]
fn get_card_by_oracle_id(
    oracle_id: String,
    redis_url: Option<String>,
) -> PyResult<PyObject> {
    let redis_url = redis_url.unwrap_or_else(|| "redis://127.0.0.1:9999".to_string());
    
    Python::with_gil(|py| {
        match get_card_by_oracle_id_internal(&oracle_id, &redis_url) {
            Ok(Some(card)) => {
                let dict = PyDict::new(py);
                dict.set_item("id", &card.id)?;
                dict.set_item("oracle_id", &card.oracle_id)?;
                dict.set_item("name", &card.name)?;
                dict.set_item("sets", &card.sets)?;
                dict.set_item("layout", &card.layout)?;
                dict.set_item("tcgplayer_ids", &card.tcgplayer_ids)?;
                dict.set_item("main_image", &card.main_image)?;
                let prices_json = serde_json::to_string(&card.prices)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize prices: {}", e)))?;
                dict.set_item("prices", prices_json)?;
                Ok(dict.into())
            }
            Ok(None) => Err(pyo3::exceptions::PyKeyError::new_err(format!(
                "Card with oracle_id '{}' not found", oracle_id
            ))),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to get card: {}", e
            ))),
        }
    })
}

/// Get autocomplete suggestions
#[pyfunction]
fn get_autocomplete(
    prefix: String,
    max_results: Option<usize>,
    redis_url: Option<String>,
) -> PyResult<Vec<String>> {
    let redis_url = redis_url.unwrap_or_else(|| "redis://127.0.0.1:9999".to_string());
    let max_results = max_results.unwrap_or(10);
    
    match get_autocomplete_internal(&prefix, max_results, &redis_url) {
        Ok(suggestions) => Ok(suggestions),
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Autocomplete failed: {}", e
        ))),
    }
}

/// Get statistics about the indexed data
#[pyfunction]
fn get_stats(redis_url: Option<String>) -> PyResult<PyObject> {
    let redis_url = redis_url.unwrap_or_else(|| "redis://127.0.0.1:9999".to_string());
    
    Python::with_gil(|py| {
        match get_stats_internal(&redis_url) {
            Ok(stats) => {
                let dict = PyDict::new(py);
                dict.set_item("card_count", stats.card_count)?;
                dict.set_item("set_count", stats.set_count)?;
                dict.set_item("last_update", &stats.last_update)?;
                Ok(dict.into())
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to get stats: {}", e
            ))),
        }
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn scryfall_indexer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(download_and_index, m)?)?;
    m.add_function(wrap_pyfunction!(search_cards, m)?)?;
    m.add_function(wrap_pyfunction!(get_card_by_oracle_id, m)?)?;
    m.add_function(wrap_pyfunction!(get_autocomplete, m)?)?;
    m.add_function(wrap_pyfunction!(get_stats, m)?)?;
    Ok(())
} 