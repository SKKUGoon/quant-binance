use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::websocket::BinanceData;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWebsocketLiquidation {
    pub stream: String,
    pub data: LiquidationEvent,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Clone)]
pub struct LiquidationEvent {
    pub e: String, // Event type
    pub E: u64,    // Event time
    pub o: LiquidationOrder,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Clone)]
pub struct LiquidationOrder {
    pub s: String,  // Symbol
    pub S: String,  // Side
    pub o: String,  // Order Type
    pub f: String,  // Time in force
    pub q: String,  // Quantity
    pub p: String,  // Price
    pub ap: String, // Average Price
    pub X: String,  // Order Status
    pub l: String,  // Last Filled Quantity
    pub z: String,  // Filled Accumulated Quantity
    pub T: u64,     // Transaction Time
}

pub async fn handle_liquidation_order<R, S>(
    mut read: R,
    mut write: S,
    tx: mpsc::Sender<BinanceData>,
) where
    R: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<BinanceWebsocketLiquidation>(&text) {
                    Ok(event) => {
                        println!("Liquidation event: {:?}", event.data);

                        if tx
                            .send(BinanceData::Liquidation(event.data.clone()))
                            .await
                            .is_err()
                        {
                            eprintln!("Failed to send liquidation event");
                        }
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
