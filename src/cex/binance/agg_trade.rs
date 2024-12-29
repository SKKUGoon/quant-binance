use futures::{SinkExt, StreamExt};
use log::{error, info};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::websocket::BinanceData;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWebsocketAggTrade {
    pub stream: String,
    pub data: AggregateTradeEvent,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Clone)]
pub struct AggregateTradeEvent {
    pub e: String, // Event type
    pub E: u64,    // Event time
    pub s: String, // Symbol
    pub a: u64,    // Aggregate trade ID
    pub p: String, // Price
    pub q: String, // Quantity
    pub f: u64,    // First trade ID
    pub l: u64,    // Last trade ID
    pub T: u64,    // Trade time
    pub m: bool,   // Is the buyer the market maker?
}

pub async fn handle_agg_trade<R, S>(mut read: R, mut write: S, tx: mpsc::Sender<BinanceData>)
where
    R: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<BinanceWebsocketAggTrade>(&text) {
                    Ok(event) => {
                        if tx
                            .send(BinanceData::AggTrade(event.data.clone()))
                            .await
                            .is_err()
                        {
                            error!("Failed to send agg trade event");
                        }
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
