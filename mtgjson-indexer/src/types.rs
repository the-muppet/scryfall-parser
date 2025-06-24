use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AllPrintingsFile {
    pub meta: Meta,
    pub data: HashMap<String, Set>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TcgplayerSkusFile {
    pub meta: Meta,
    pub data: HashMap<String, Vec<TcgplayerSku>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Meta {
    pub date: String,
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Set {
    pub base_set_size: u32,
    pub block: Option<String>,
    pub cards: Vec<CardSet>,
    #[serde(default)]
    pub cardsphere_set_id: Option<u32>,
    pub code: String,
    #[serde(default)]
    pub code_v3: Option<String>,
    #[serde(default)]
    pub is_foreign_only: Option<bool>,
    pub is_foil_only: bool,
    #[serde(default)]
    pub is_non_foil_only: Option<bool>,
    pub is_online_only: bool,
    #[serde(default)]
    pub is_paper_only: Option<bool>,
    #[serde(default)]
    pub is_partial_preview: Option<bool>,
    pub keyrune_code: String,
    #[serde(default)]
    pub languages: Option<Vec<String>>,
    #[serde(default)]
    pub mcm_id: Option<u32>,
    #[serde(default)]
    pub mcm_id_extras: Option<u32>,
    #[serde(default)]
    pub mcm_name: Option<String>,
    #[serde(default)]
    pub mtgo_code: Option<String>,
    pub name: String,
    #[serde(default)]
    pub parent_code: Option<String>,
    pub release_date: String,
    #[serde(default)]
    pub tcgplayer_group_id: Option<u32>,
    pub total_set_size: u32,
    #[serde(default)]
    pub token_set_code: Option<String>,
    pub translations: Translations,
    #[serde(rename = "type")]
    pub set_type: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CardSet {
    #[serde(default)]
    pub artist: Option<String>,
    #[serde(default)]
    pub artist_ids: Option<Vec<String>>,
    #[serde(default)]
    pub ascii_name: Option<String>,
    #[serde(default)]
    pub attraction_lights: Option<Vec<u32>>,
    pub availability: Vec<String>,
    #[serde(default)]
    pub booster_types: Option<Vec<String>>,
    pub border_color: String,
    #[serde(default)]
    pub card_parts: Option<Vec<String>>,
    pub color_identity: Vec<String>,
    #[serde(default)]
    pub color_indicator: Option<Vec<String>>,
    pub colors: Vec<String>,
    pub converted_mana_cost: f32,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(default)]
    pub defense: Option<String>,
    #[serde(default)]
    pub duel_deck: Option<String>,
    #[serde(default)]
    pub edhrec_rank: Option<u32>,
    #[serde(default)]
    pub edhrec_saltiness: Option<f32>,
    #[serde(default)]
    pub etched: Option<bool>,
    #[serde(default)]
    pub face_converted_mana_cost: Option<f32>,
    #[serde(default)]
    pub face_flavor_name: Option<String>,
    #[serde(default)]
    pub face_mana_value: Option<f32>,
    #[serde(default)]
    pub face_name: Option<String>,
    pub finishes: Vec<String>,
    #[serde(default)]
    pub flavor_name: Option<String>,
    #[serde(default)]
    pub flavor_text: Option<String>,
    #[serde(default)]
    pub foreign_data: Option<Vec<ForeignData>>,
    #[serde(default)]
    pub frame_effects: Option<Vec<String>>,
    pub frame_version: String,
    #[serde(default)]
    pub hand: Option<String>,
    #[serde(default)]
    pub has_alternative_deck_limit: Option<bool>,
    #[serde(default)]
    pub has_content_warning: Option<bool>,
    pub has_foil: bool,
    pub has_non_foil: bool,
    pub identifiers: Identifiers,
    #[serde(default)]
    pub is_alternative: Option<bool>,
    #[serde(default)]
    pub is_full_art: Option<bool>,
    #[serde(default)]
    pub is_funny: Option<bool>,
    #[serde(default)]
    pub is_online_only: Option<bool>,
    #[serde(default)]
    pub is_oversized: Option<bool>,
    #[serde(default)]
    pub is_promo: Option<bool>,
    #[serde(default)]
    pub is_rebalanced: Option<bool>,
    #[serde(default)]
    pub is_reprint: Option<bool>,
    #[serde(default)]
    pub is_reserved: Option<bool>,
    #[serde(default)]
    pub is_starter: Option<bool>,
    #[serde(default)]
    pub is_story_spotlight: Option<bool>,
    #[serde(default)]
    pub is_textless: Option<bool>,
    #[serde(default)]
    pub is_timeshifted: Option<bool>,
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
    pub language: String,
    pub layout: String,
    #[serde(default)]
    pub leadership_skills: Option<LeadershipSkills>,
    pub legalities: Legalities,
    #[serde(default)]
    pub life: Option<String>,
    #[serde(default)]
    pub loyalty: Option<String>,
    #[serde(default)]
    pub mana_cost: Option<String>,
    pub mana_value: f32,
    pub name: String,
    pub number: String,
    #[serde(default)]
    pub original_printings: Option<Vec<String>>,
    #[serde(default)]
    pub original_release_date: Option<String>,
    #[serde(default)]
    pub original_text: Option<String>,
    #[serde(default)]
    pub original_type: Option<String>,
    #[serde(default)]
    pub other_face_ids: Option<Vec<String>>,
    #[serde(default)]
    pub power: Option<String>,
    #[serde(default)]
    pub printings: Option<Vec<String>>,
    #[serde(default)]
    pub promo_types: Option<Vec<String>>,
    pub purchase_urls: PurchaseUrls,
    pub rarity: String,
    #[serde(default)]
    pub related_cards: Option<RelatedCards>,
    #[serde(default)]
    pub rebalanced_printings: Option<Vec<String>>,
    #[serde(default)]
    pub rulings: Option<Vec<Ruling>>,
    #[serde(default)]
    pub security_stamp: Option<String>,
    pub set_code: String,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub source_products: Option<SourceProducts>,
    #[serde(default)]
    pub subsets: Option<Vec<String>>,
    pub subtypes: Vec<String>,
    pub supertypes: Vec<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub toughness: Option<String>,
    #[serde(rename = "type")]
    pub card_type: String,
    pub types: Vec<String>,
    pub uuid: String,
    #[serde(default)]
    pub variations: Option<Vec<String>>,
    #[serde(default)]
    pub watermark: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Identifiers {
    #[serde(default)]
    pub abu_id: Option<String>,
    #[serde(default)]
    pub card_kingdom_etched_id: Option<String>,
    #[serde(default)]
    pub card_kingdom_foil_id: Option<String>,
    #[serde(default)]
    pub card_kingdom_id: Option<String>,
    #[serde(default)]
    pub cardsphere_id: Option<String>,
    #[serde(default)]
    pub cardsphere_foil_id: Option<String>,
    #[serde(default)]
    pub cardtrader_id: Option<String>,
    #[serde(default)]
    pub csi_id: Option<String>,
    #[serde(default)]
    pub mcm_id: Option<String>,
    #[serde(default)]
    pub mcm_meta_id: Option<String>,
    #[serde(default)]
    pub miniaturemarket_id: Option<String>,
    #[serde(default)]
    pub mtg_arena_id: Option<String>,
    #[serde(default)]
    pub mtgjson_foil_version_id: Option<String>,
    #[serde(default)]
    pub mtgjson_non_foil_version_id: Option<String>,
    #[serde(default)]
    pub mtgjson_v4_id: Option<String>,
    #[serde(default)]
    pub mtgo_foil_id: Option<String>,
    #[serde(default)]
    pub mtgo_id: Option<String>,
    #[serde(default)]
    pub multiverse_id: Option<String>,
    #[serde(default)]
    pub scg_id: Option<String>,
    #[serde(default)]
    pub scryfall_id: Option<String>,
    #[serde(default)]
    pub scryfall_card_back_id: Option<String>,
    #[serde(default)]
    pub scryfall_oracle_id: Option<String>,
    #[serde(default)]
    pub scryfall_illustration_id: Option<String>,
    #[serde(default)]
    pub tcgplayer_product_id: Option<String>,
    #[serde(default)]
    pub tcgplayer_etched_product_id: Option<String>,
    #[serde(default)]
    pub tnt_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TcgplayerSku {
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub printing: Option<String>,
    pub product_id: u64,
    pub sku_id: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ForeignData {
    #[serde(default)]
    pub face_name: Option<String>,
    #[serde(default)]
    pub flavor_text: Option<String>,
    pub identifiers: Identifiers,
    pub language: String,
    pub name: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    #[serde(rename = "type")]
    pub card_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LeadershipSkills {
    pub brawl: bool,
    pub commander: bool,
    pub oathbreaker: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Legalities {
    #[serde(default)]
    pub alchemy: Option<String>,
    #[serde(default)]
    pub brawl: Option<String>,
    #[serde(default)]
    pub commander: Option<String>,
    #[serde(default)]
    pub duel: Option<String>,
    #[serde(default)]
    pub explorer: Option<String>,
    #[serde(default)]
    pub future: Option<String>,
    #[serde(default)]
    pub gladiator: Option<String>,
    #[serde(default)]
    pub historic: Option<String>,
    #[serde(default)]
    pub historicbrawl: Option<String>,
    #[serde(default)]
    pub legacy: Option<String>,
    #[serde(default)]
    pub modern: Option<String>,
    #[serde(default)]
    pub oathbreaker: Option<String>,
    #[serde(default)]
    pub oldschool: Option<String>,
    #[serde(default)]
    pub pauper: Option<String>,
    #[serde(default)]
    pub paupercommander: Option<String>,
    #[serde(default)]
    pub penny: Option<String>,
    #[serde(default)]
    pub pioneer: Option<String>,
    #[serde(default)]
    pub predh: Option<String>,
    #[serde(default)]
    pub premodern: Option<String>,
    #[serde(default)]
    pub standard: Option<String>,
    #[serde(default)]
    pub standardbrawl: Option<String>,
    #[serde(default)]
    pub timeless: Option<String>,
    #[serde(default)]
    pub vintage: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PurchaseUrls {
    #[serde(default)]
    pub card_kingdom: Option<String>,
    #[serde(default)]
    pub card_kingdom_etched: Option<String>,
    #[serde(default)]
    pub card_kingdom_foil: Option<String>,
    #[serde(default)]
    pub cardmarket: Option<String>,
    #[serde(default)]
    pub tcgplayer: Option<String>,
    #[serde(default)]
    pub tcgplayer_etched: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RelatedCards {
    #[serde(default)]
    pub reverse_related: Option<Vec<String>>,
    #[serde(default)]
    pub spellbook: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ruling {
    pub date: String,
    pub text: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceProducts {
    #[serde(default)]
    pub etched: Vec<String>,
    #[serde(default)]
    pub foil: Vec<String>,
    #[serde(default)]
    pub nonfoil: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Translations {
    #[serde(default)]
    #[serde(rename = "Ancient Greek")]
    pub ancient_greek: Option<String>,
    #[serde(default)]
    #[serde(rename = "Arabic")]
    pub arabic: Option<String>,
    #[serde(default)]
    #[serde(rename = "Chinese Simplified")]
    pub chinese_simplified: Option<String>,
    #[serde(default)]
    #[serde(rename = "Chinese Traditional")]
    pub chinese_traditional: Option<String>,
    #[serde(default)]
    #[serde(rename = "French")]
    pub french: Option<String>,
    #[serde(default)]
    #[serde(rename = "German")]
    pub german: Option<String>,
    #[serde(default)]
    #[serde(rename = "Hebrew")]
    pub hebrew: Option<String>,
    #[serde(default)]
    #[serde(rename = "Italian")]
    pub italian: Option<String>,
    #[serde(default)]
    #[serde(rename = "Japanese")]
    pub japanese: Option<String>,
    #[serde(default)]
    #[serde(rename = "Korean")]
    pub korean: Option<String>,
    #[serde(default)]
    #[serde(rename = "Latin")]
    pub latin: Option<String>,
    #[serde(default)]
    #[serde(rename = "Phyrexian")]
    pub phyrexian: Option<String>,
    #[serde(default)]
    #[serde(rename = "Portuguese (Brazil)")]
    pub portuguese_brazil: Option<String>,
    #[serde(default)]
    #[serde(rename = "Russian")]
    pub russian: Option<String>,
    #[serde(default)]
    #[serde(rename = "Sanskrit")]
    pub sanskrit: Option<String>,
    #[serde(default)]
    #[serde(rename = "Spanish")]
    pub spanish: Option<String>,
}

// Simplified card structure optimized for Redis storage and fast querying
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IndexedCard {
    pub uuid: String,
    pub name: String,
    pub set_code: String,
    pub set_name: String,
    pub collector_number: String,
    pub rarity: String,
    pub mana_value: f32,
    pub mana_cost: Option<String>,
    pub colors: Vec<String>,
    pub color_identity: Vec<String>,
    pub types: Vec<String>,
    pub subtypes: Vec<String>,
    pub supertypes: Vec<String>,
    pub power: Option<String>,
    pub toughness: Option<String>,
    pub loyalty: Option<String>,
    pub defense: Option<String>,
    pub text: Option<String>,
    pub flavor_text: Option<String>,
    pub layout: String,
    pub availability: Vec<String>,
    pub finishes: Vec<String>,
    pub has_foil: bool,
    pub has_non_foil: bool,
    pub is_reserved: bool,
    pub is_promo: bool,
    pub release_date: String,
    pub scryfall_oracle_id: Option<String>,
    pub scryfall_id: Option<String>,
    pub tcgplayer_product_id: Option<String>,
    pub tcgplayer_skus: Vec<TcgplayerSku>,
    pub purchase_urls: PurchaseUrls,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IndexStats {
    pub total_sets: usize,
    pub total_cards: usize,
    pub processed_cards: usize,
    pub last_update: String,
    pub source: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct DeckFile {
    pub meta: Meta,
    pub data: DeckData,
}

#[derive(Debug, Deserialize)]
pub struct DeckData {
    pub name: String,
    pub code: String,
    #[serde(default)]
    #[serde(rename = "type")]
    pub deck_type: Option<String>,
    #[serde(default)]
    #[serde(rename = "releaseDate")]
    pub release_date: Option<String>,
    #[serde(default)]
    pub commander: Vec<CardSet>,
    #[serde(default)]
    #[serde(rename = "displayCommander")]
    pub display_commander: Vec<CardSet>,
    #[serde(default, rename = "mainBoard")]
    pub main_board: Vec<CardSet>,
    #[serde(default, rename = "sideBoard")]
    pub side_board: Vec<CardSet>,
    #[serde(default)]
    pub planes: Vec<CardSet>,
}

#[derive(Debug, Deserialize)]
pub struct DeckCard {
    pub uuid: String,
    pub name: String,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(default)]
    pub is_foil: bool,
    #[serde(rename = "setCode")]
    pub set_code: String,
    #[serde(default)]
    pub identifiers: Identifiers,
}

fn default_count() -> u32 {
    1
}

#[derive(Debug, Serialize, Clone)]
pub struct IndexedDeck {
    pub uuid: String,
    pub name: String,
    pub code: String,
    pub deck_type: String,
    pub release_date: String,
    pub is_commander: bool,
    pub total_cards: u32,
    pub unique_cards: u32,
    pub commanders: Vec<DeckCardInfo>,
    pub main_board: Vec<DeckCardInfo>,
    pub side_board: Vec<DeckCardInfo>,
    pub estimated_value: Option<DeckValue>,
}

#[derive(Debug, Serialize, Clone)]
pub struct DeckCardInfo {
    pub uuid: String,
    pub name: String,
    pub count: u32,
    pub is_foil: bool,
    pub set_code: String,
    pub tcgplayer_product_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct DeckValue {
    pub market_total: f64,
    pub direct_total: f64,
    pub low_total: f64,
    pub cards_with_pricing: u32,
    pub cards_without_pricing: u32,
}

#[derive(Debug, Serialize)]
pub struct SetInfo {
    pub code: String,
    pub name: String,
    pub release_date: String,
    pub set_type: String,
    pub total_cards: usize,
    pub base_set_size: u32,
}

#[derive(Debug, Clone)]
pub struct TcgPrice {
    pub tcgplayer_id: String,
    pub product_line: String,
    pub set_name: String,
    pub product_name: String,
    pub title: String,
    pub number: String,
    pub rarity: String,
    pub condition: String,
    pub tcg_market_price: Option<f64>,
    pub tcg_direct_low: Option<f64>,
    pub tcg_low_price_with_shipping: Option<f64>,
    pub tcg_low_price: Option<f64>,
    pub total_quantity: Option<i32>,
    pub add_to_quantity: Option<i32>,
    pub tcg_marketplace_price: Option<f64>,
}