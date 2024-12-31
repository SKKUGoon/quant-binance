use serde::Deserialize;

use super::websocket::BitgetStreamArg;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BitgetDepthMessage {
    pub action: String, // "snapshot" or "update"
    pub arg: BitgetStreamArg,
    pub data: Vec<DepthData>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct DepthData {
    pub asks: Vec<(String, String)>, // List of asks (price, quantity)
    pub bids: Vec<(String, String)>, // List of bids (price, quantity)
    pub checksum: i64,               // Checksum for validation
    pub ts: String,                  // Timestamp as a string
}
