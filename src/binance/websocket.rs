use crate::binance::agg_trade::handle_agg_trade;
use crate::binance::liquidation::handle_liquidation_order;
use crate::binance::order_book::{fetch_depth_snapshot, handle_order_book, OrderBook};

use futures::StreamExt;

/// WebSocket handling for Binance

pub async fn connect_to_binance(symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
    let streams = [
        format!("{}@depth", symbol.to_lowercase()),
        format!("{}@forceOrder", symbol.to_lowercase()),
        format!("{}@aggTrade", symbol.to_lowercase()),
    ];

    for stream in &streams {
        let ws_url = format!("wss://fstream.binance.com/stream?streams={}", stream);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
        let (write, read) = ws_stream.split();

        match stream.as_str() {
            s if s.ends_with("@depth") => {
                let symbol = symbol.to_string();
                tokio::spawn(async move {
                    let snapshot = fetch_depth_snapshot(&symbol).await.unwrap();
                    let mut order_book = OrderBook::new();
                    handle_order_book(read, write, &mut order_book, snapshot).await;
                });
            }
            s if s.ends_with("@forceOrder") => {
                tokio::spawn(async move {
                    handle_liquidation_order(read, write).await;
                });
            }
            s if s.ends_with("@aggTrade") => {
                tokio::spawn(async move {
                    handle_agg_trade(read, write).await;
                });
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
