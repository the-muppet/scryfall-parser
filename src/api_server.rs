use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, error};
use tracing_subscriber;

mod redis_client;
use redis_client::*;

// =============================================================================
// STATE AND ERROR HANDLING
// =============================================================================

type AppState = Arc<Mutex<MTGRedisClient>>;

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    redis: String,
    timestamp: Option<i64>,
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    redis_connection: String,
    total_keys: usize,
    memory_usage: MemoryUsage,
    database_stats: DatabaseStats,
}

// =============================================================================
// QUERY PARAMETERS
// =============================================================================

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
    set_code: Option<String>,
    rarity: Option<String>,
    color: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExpensiveQuery {
    #[serde(default = "default_min_price")]
    min_price: f64,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct AutocompleteQuery {
    prefix: String,
    #[serde(default = "default_autocomplete_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct PriceQuery {
    #[serde(default = "default_condition")]
    condition: String,
}

#[derive(Debug, Deserialize)]
struct PriceHistoryQuery {
    #[serde(default = "default_days")]
    days: u32,
}

#[derive(Debug, Deserialize)]
struct TrendingQuery {
    #[serde(default = "default_direction")]
    direction: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct ArbitrageQuery {
    #[serde(default)]
    card_filter: String,
    #[serde(default = "default_min_diff")]
    min_diff: f64,
}

// Default values
fn default_limit() -> usize { 50 }
fn default_autocomplete_limit() -> usize { 10 }
fn default_min_price() -> f64 { 50.0 }
fn default_condition() -> String { "Near Mint".to_string() }
fn default_days() -> u32 { 30 }
fn default_direction() -> String { "up".to_string() }
fn default_min_diff() -> f64 { 5.0 }

// =============================================================================
// CARD ENDPOINTS
// =============================================================================

async fn get_card(
    Path(uuid): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_card_by_uuid(&uuid).await {
        Ok(Some(card)) => Json(ApiResponse::ok(card)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("Card not found".to_string()))).into_response(),
        Err(e) => {
            error!("Error getting card {}: {}", uuid, e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn search_cards(
    Query(params): Query<SearchQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.search_cards_by_name(&params.q, params.limit).await {
        Ok(cards) => {
            let response = SearchResponse {
                query: params.q,
                count: cards.len(),
                results: cards,
            };
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error searching cards: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn autocomplete_cards(
    Query(params): Query<AutocompleteQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.autocomplete_card_names(&params.prefix, params.limit).await {
        Ok(suggestions) => {
            let response = serde_json::json!({
                "prefix": params.prefix,
                "suggestions": suggestions
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting autocomplete: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// DECK ENDPOINTS
// =============================================================================

async fn get_deck(
    Path(uuid): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_deck_by_uuid(&uuid).await {
        Ok(Some(deck)) => Json(ApiResponse::ok(deck)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("Deck not found".to_string()))).into_response(),
        Err(e) => {
            error!("Error getting deck {}: {}", uuid, e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// PRICING ENDPOINTS
// =============================================================================

async fn get_card_price(
    Path(uuid): Path<String>,
    Query(params): Query<PriceQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_card_price(&uuid, &params.condition).await {
        Ok(Some(price)) => Json(ApiResponse::ok(price)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("Price not found".to_string()))).into_response(),
        Err(e) => {
            error!("Error getting card price: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_sku_price(
    Path(sku_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_sku_price_latest(&sku_id).await {
        Ok(Some(price)) => Json(ApiResponse::ok(price)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("SKU price not found".to_string()))).into_response(),
        Err(e) => {
            error!("Error getting SKU price: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_sku_price_history(
    Path(sku_id): Path<String>,
    Query(params): Query<PriceHistoryQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_sku_price_history(&sku_id, params.days).await {
        Ok(history) => {
            let response = serde_json::json!({
                "sku_id": sku_id,
                "days": params.days,
                "count": history.len(),
                "history": history.into_iter().map(|(price, timestamp)| {
                    serde_json::json!({
                        "price": price,
                        "timestamp": timestamp
                    })
                }).collect::<Vec<_>>()
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting SKU price history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// SET ENDPOINTS
// =============================================================================

async fn get_set(
    Path(set_code): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_set_by_code(&set_code).await {
        Ok(Some(set_data)) => Json(ApiResponse::ok(set_data)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("Set not found".to_string()))).into_response(),
        Err(e) => {
            error!("Error getting set: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_all_sets(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_all_sets().await {
        Ok(sets) => {
            let response = serde_json::json!({
                "count": sets.len(),
                "sets": sets
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting all sets: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// ANALYTICS ENDPOINTS
// =============================================================================

async fn get_database_statistics(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_database_stats().await {
        Ok(stats) => {
            let response = serde_json::json!({
                "statistics": stats
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting database stats: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_memory_usage(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_memory_usage().await {
        Ok(usage) => {
            let response = serde_json::json!({
                "memory": usage
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting memory usage: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// HEALTH & STATUS
// =============================================================================

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    let redis_ok = client.ping().await;
    
    let response = HealthResponse {
        status: if redis_ok { "healthy".to_string() } else { "unhealthy".to_string() },
        redis: if redis_ok { "connected".to_string() } else { "disconnected".to_string() },
        timestamp: if redis_ok { Some(chrono::Utc::now().timestamp()) } else { None },
    };
    
    if redis_ok {
        Json(ApiResponse::ok(response)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ApiResponse::ok(response))).into_response()
    }
}

async fn get_api_stats(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    if !client.ping().await {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(ApiResponse::<()>::error("Database unavailable".to_string()))).into_response();
    }
    
    match (client.get_key_count("*").await, client.get_memory_usage().await, client.get_database_stats().await) {
        (Ok(total_keys), Ok(memory_usage), Ok(database_stats)) => {
            let response = StatsResponse {
                redis_connection: "ok".to_string(),
                total_keys,
                memory_usage,
                database_stats,
            };
            Json(ApiResponse::ok(response)).into_response()
        }
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            error!("Error getting API stats: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

// =============================================================================
// ROUTER SETUP
// =============================================================================

fn create_router(state: AppState) -> Router {
    Router::new()
        // Card endpoints
        .route("/cards/:uuid", get(get_card))
        .route("/cards/search/name", get(search_cards))
        .route("/cards/autocomplete", get(autocomplete_cards))
        
        // Deck endpoints
        .route("/decks/:uuid", get(get_deck))
        
        // Pricing endpoints
        .route("/pricing/card/:uuid", get(get_card_price))
        .route("/pricing/sku/:sku_id", get(get_sku_price))
        .route("/pricing/sku/:sku_id/history", get(get_sku_price_history))
        
        // Set endpoints
        .route("/sets/:set_code", get(get_set))
        .route("/sets", get(get_all_sets))
        
        // Analytics endpoints
        .route("/analytics/database-stats", get(get_database_statistics))
        .route("/analytics/memory-usage", get(get_memory_usage))
        
        // Health & status
        .route("/health", get(health_check))
        .route("/stats", get(get_api_stats))
        
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive())
        )
        .with_state(state)
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("api_server=debug,tower_http=debug")
        .init();

    info!("Starting MTG Database API");

    // Initialize Redis client
    let mtg_client = match create_mtg_client_from_env().await {
        Ok(client) => {
            info!("✓ Connected to Redis database");
            client
        }
        Err(e) => {
            error!("Could not connect to Redis database: {}", e);
            return Err(e.into());
        }
    };

    let state = Arc::new(Mutex::new(mtg_client));
    let app = create_router(state);

    // Start server
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse::<u16>()?;

    let address = format!("{}:{}", host, port);
    info!("Server listening on http://{}", address);

    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, app).await?;

    Ok(())
} 