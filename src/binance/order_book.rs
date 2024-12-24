// Order book management

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

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

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: HashMap<String, String>, // Price -> Quantity
    pub asks: HashMap<String, String>, // Price -> Quantity
    pub time: u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: HashMap::new(),
            asks: HashMap::new(),
            time: 0u64,
        }
    }

    pub fn update(&mut self, event: &DepthEvent) {
        self.time = event.E;

        for (price, quantity) in event.b.iter() {
            if quantity == "0" {
                self.bids.remove(price);
            } else {
                self.bids.insert(price.clone(), quantity.clone());
            }
        }

        for (price, quantity) in event.a.iter() {
            if quantity == "0" {
                self.asks.remove(price);
            } else {
                self.asks.insert(price.clone(), quantity.clone());
            }
        }
    }

    fn group_prices(&self, prices: &HashMap<String, String>) -> Vec<(String, f64)> {
        let mut grouped: HashMap<String, f64> = HashMap::new();

        for (price, quantity) in prices.iter() {
            let price_f64 = match price.parse::<f64>() {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("Failed to parse price: {}", price);
                    continue;
                }
            };
            let quantity_f64 = match quantity.parse::<f64>() {
                Ok(q) => q,
                Err(_) => {
                    eprintln!("Failed to parse quantity: {}", quantity);
                    continue;
                }
            };

            let magnitude = (10f64.powf(price_f64.log10().floor() - 2.0)).min(100.0);
            let range = format!("{:.2}", (price_f64 / magnitude).floor() * magnitude);

            *grouped.entry(range).or_insert(0.0) += quantity_f64;
        }

        let mut grouped_vec: Vec<(String, f64)> = grouped.into_iter().collect();
        grouped_vec.sort_by(|(price1, _), (price2, _)| {
            price2
                .parse::<f64>()
                .unwrap_or(0.0)
                .partial_cmp(&price1.parse::<f64>().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        grouped_vec
    }

    #[allow(dead_code)]
    pub fn display(&self) {
        println!("\n=== Grouped Order Book ===");

        let grouped_asks = self.group_prices(&self.asks);
        for (range, total_quantity) in grouped_asks.iter() {
            println!(
                "ASK  │ Price: {:>10} │ Quantity: {:>10.3}",
                range, total_quantity
            );
        }

        println!("─────┼──────────────────┼────────────────");

        let grouped_bids = self.group_prices(&self.bids);
        for (range, total_quantity) in grouped_bids.iter() {
            println!(
                "BID  │ Price: {:>10} │ Quantity: {:>10.3}",
                range, total_quantity
            );
        }

        println!("=== End of Order Book ===\n");
    }
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
    order_book: &mut OrderBook,
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
            eprintln!(
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
                            eprintln!("Event out of order: Reinitializing");
                            continue;
                        }
                        order_book.update(&event.data);

                        if tx
                            .send(BinanceData::OrderBook(order_book.clone()))
                            .await
                            .is_err()
                        {
                            eprintln!("Failed to send order book update");
                        }
                        last_update_id = event.data.u;
                    }
                    Err(e) => eprintln!("Failed to parse event: {} - Error: {}", text, e),
                }
            }
            Ok(Message::Ping(payload)) => {
                if let Err(e) = write.send(Message::Pong(payload)).await {
                    eprintln!("Failed to send Pong: {}", e);
                }
            }
            Ok(Message::Pong(_)) => println!("Received Pong"),
            Ok(Message::Close(reason)) => {
                println!("WebSocket closed: {:?}", reason);
                break;
            }
            Err(e) => eprintln!("Error reading message: {}", e),
            _ => (),
        }
    }
}
