use futures::{SinkExt, StreamExt};
use log::{error, info};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::cex::combined_order_book::CombinedOrderBook;

use super::websocket::BinanceData;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWebsocketDiffBook {
    pub stream: String,
    pub data: DepthEvent,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct DepthEvent {
    pub e: String,                // Event type
    pub E: u64,                   // Event time
    pub T: u64,                   // Transaction time
    pub s: String,                // Symbol
    pub U: u64,                   // First update ID in event
    pub u: u64,                   // Final update ID in event
    pub pu: u64,                  // Final update ID from previous event
    pub b: Vec<(String, String)>, // Bids to update
    pub a: Vec<(String, String)>, // Asks to update
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct DepthSnapShot {
    pub lastUpdateId: u64,
    pub E: u64, // Message output time
    pub T: u64, // Transaction time
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<DepthSnapShot, reqwest::Error> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=1000",
        symbol
    );

    let client = reqwest::Client::new();

    let response = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json::<DepthSnapShot>()
        .await?;

    Ok(response)
}

pub async fn handle_order_book<R, S>(
    mut read: R,
    mut write: S,
    order_book: &mut CombinedOrderBook,
    snapshot: DepthSnapShot,
    tx: mpsc::Sender<BinanceData>,
) where
    R: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut last_update_id = snapshot.lastUpdateId;

    // Initialize the order book with the snapshot data
    for (price, quantity) in snapshot.bids.iter().chain(snapshot.asks.iter()) {
        if price.parse::<f64>().is_err() || quantity.parse::<f64>().is_err() {
            error!(
                "Invalid snapshot data: price={}, quantity={}",
                price, quantity
            );
            continue;
        }
    }

    order_book.bids.extend(snapshot.bids.into_iter());
    order_book.asks.extend(snapshot.asks.into_iter());

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<BinanceWebsocketDiffBook>(&text) {
                    Ok(event) => {
                        if event.data.u <= last_update_id || event.data.U <= last_update_id {
                            error!("Event out of order: Reinitializing");
                            continue;
                        }
                        order_book.update_binance(&event.data);

                        if tx
                            .send(BinanceData::OrderBook(order_book.clone()))
                            .await
                            .is_err()
                        {
                            error!("Failed to send order book update");
                        }
                        last_update_id = event.data.u;
                    }
                    Err(e) => error!("Failed to parse event: {} - Error: {}", text, e),
                }
            }
            Ok(Message::Ping(payload)) => {
                if let Err(e) = write.send(Message::Pong(payload)).await {
                    error!("Failed to send Pong: {}", e);
                }
            }
            Ok(Message::Pong(_)) => info!("Received Pong"),
            Ok(Message::Close(reason)) => {
                info!("WebSocket closed: {:?}", reason);
                break;
            }
            Err(e) => error!("Error reading message: {}", e),
            _ => (),
        }
    }
}
