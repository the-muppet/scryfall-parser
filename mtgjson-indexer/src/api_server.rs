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

use mtgjson_indexer::{redis_client::*, api_types::*};

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
    
    // Build filters HashMap
    let mut filters = std::collections::HashMap::new();
    if let Some(set_code) = params.set_code {
        filters.insert("set".to_string(), set_code);
    }
    if let Some(rarity) = params.rarity {
        filters.insert("rarity".to_string(), rarity);
    }
    if let Some(color) = params.color {
        filters.insert("color".to_string(), color);
    }
    
    match client.search_cards_by_name(&params.q, params.limit, filters).await {
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

async fn get_expensive_cards(
    Query(params): Query<ExpensiveQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_expensive_cards(params.min_price, params.limit).await {
        Ok(cards) => {
            let response = serde_json::json!({
                "min_price": params.min_price,
                "count": cards.len(),
                "cards": cards
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting expensive cards: {}", e);
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

async fn get_deck_composition(
    Path(uuid): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_deck_composition(&uuid).await {
        Ok(composition) => Json(ApiResponse::ok(composition)).into_response(),
        Err(e) => {
            error!("Error getting deck composition: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_commander_decks(State(state): State<AppState>) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_commander_decks().await {
        Ok(decks) => {
            let response = serde_json::json!({
                "count": decks.len(),
                "decks": decks
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting commander decks: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn search_decks(
    Query(params): Query<SearchQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.search_decks_by_name(&params.q).await {
        Ok(decks) => {
            let response = SearchResponse {
                query: params.q,
                count: decks.len(),
                results: decks,
            };
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error searching decks: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn find_decks_with_card(
    Query(params): Query<SearchQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.find_decks_containing_card(&params.q).await {
        Ok(decks) => {
            let response = serde_json::json!({
                "card_name": params.q,
                "count": decks.len(),
                "decks": decks
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error finding decks with card: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_expensive_decks(
    Query(params): Query<ExpensiveQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_expensive_decks(params.min_price).await {
        Ok(decks) => {
            let response = serde_json::json!({
                "min_value": params.min_price,
                "count": decks.len(),
                "decks": decks
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting expensive decks: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn export_deck_csv(
    Path(uuid): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.export_deck_to_tcg_csv(&uuid).await {
        Ok(csv_data) => {
            if csv_data.is_empty() {
                return (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("Deck not found or no exportable data".to_string()))).into_response();
            }
            
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/csv")
                .header("Content-Disposition", format!("attachment; filename=deck_{}.csv", uuid))
                .body(csv_data)
                .unwrap()
                .into_response()
        }
        Err(e) => {
            error!("Error exporting deck CSV: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_trending_cards(
    Query(params): Query<TrendingQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_trending_cards(&params.direction, params.limit).await {
        Ok(cards) => {
            let response = serde_json::json!({
                "direction": params.direction,
                "count": cards.len(),
                "cards": cards
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting trending cards: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error(e.to_string()))).into_response()
        }
    }
}

async fn get_arbitrage_opportunities(
    Query(params): Query<ArbitrageQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.get_price_arbitrage_opportunities(&params.card_filter, params.min_diff).await {
        Ok(opportunities) => {
            let response = serde_json::json!({
                "card_filter": params.card_filter,
                "min_diff": params.min_diff,
                "count": opportunities.len(),
                "opportunities": opportunities
            });
            Json(ApiResponse::ok(response)).into_response()
        }
        Err(e) => {
            error!("Error getting arbitrage opportunities: {}", e);
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
        .route("/cards/expensive", get(get_expensive_cards))
        
        // Deck endpoints
        .route("/decks/:uuid", get(get_deck))
        .route("/decks/:uuid/composition", get(get_deck_composition))
        .route("/decks/commanders", get(get_commander_decks))
        .route("/decks/search/name", get(search_decks))
        .route("/decks/containing-card", get(find_decks_with_card))
        .route("/decks/expensive", get(get_expensive_decks))
        .route("/decks/:uuid/export/tcg-csv", get(export_deck_csv))
        
        // Pricing endpoints
        .route("/pricing/card/:uuid", get(get_card_price))
        .route("/pricing/sku/:sku_id", get(get_sku_price))
        .route("/pricing/sku/:sku_id/history", get(get_sku_price_history))
        .route("/pricing/trending", get(get_trending_cards))
        .route("/pricing/arbitrage", get(get_arbitrage_opportunities))
        
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
    let host = std::env::var("RUST_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("RUST_PORT")
        .unwrap_or_else(|_| "8888".to_string())
        .parse::<u16>()?;

    let address = format!("{}:{}", host, port);
    info!("Server listening on http://{}", address);

    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, app).await?;

    Ok(())
} 