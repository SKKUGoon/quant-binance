use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWebsocketAggTrade {
    pub stream: String,
    pub data: AggregateTradeEvent,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
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

pub async fn handle_agg_trade<R, S>(mut read: R, mut write: S)
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
                        println!("Agg trade event: {:?}", event.data);
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
