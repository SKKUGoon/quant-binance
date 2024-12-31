use futures::StreamExt;
use log::info;
use tokio::sync::mpsc;

use crate::cex::binance::{
    agg_trade::{handle_agg_trade, AggregateTradeEvent},
    liquidation::{handle_liquidation_order, LiquidationEvent},
    order_book::{fetch_depth_snapshot, handle_order_book},
};
use crate::cex::combined_order_book::CombinedOrderBook;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum BinanceData {
    OrderBook(CombinedOrderBook),
    Liquidation(LiquidationEvent),
    AggTrade(AggregateTradeEvent),
}

#[derive(Debug, Clone)]
pub struct BinanceStreamBuilder {
    symbol: String,
    streams: Vec<String>,
}

#[allow(dead_code)]
impl BinanceStreamBuilder {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_lowercase().to_string(),
            streams: Vec::new(),
        }
    }

    pub fn with_depth(mut self) -> Self {
        self.streams.push(format!("{}@depth", self.symbol));
        self
    }

    pub fn with_liquidation(mut self) -> Self {
        self.streams.push(format!("{}@forceOrder", self.symbol));
        self
    }

    pub fn with_agg_trade(mut self) -> Self {
        self.streams.push(format!("{}@aggTrade", self.symbol));
        self
    }

    pub async fn build(
        self,
        tx: mpsc::Sender<BinanceData>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "Starting {} Binance stream for {}",
            self.streams.len(),
            self.symbol
        );

        for stream in &self.streams {
            let ws_url = format!("wss://fstream.binance.com/stream?streams={}", stream);
            let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
            let (write, read) = ws_stream.split();

            match stream.as_str() {
                s if s.ends_with("@depth") => {
                    let symbol = self.symbol.clone();
                    let tx_clone = tx.clone();
                    tokio::spawn(async move {
                        let snapshot = fetch_depth_snapshot(&symbol).await.unwrap();
                        let mut order_book = CombinedOrderBook::new();
                        handle_order_book(read, write, &mut order_book, snapshot, tx_clone).await;
                    });
                }
                s if s.ends_with("@forceOrder") => {
                    let tx_clone = tx.clone();
                    tokio::spawn(async move {
                        handle_liquidation_order(read, write, tx_clone).await;
                    });
                }
                s if s.ends_with("@aggTrade") => {
                    let tx_clone = tx.clone();
                    tokio::spawn(async move {
                        handle_agg_trade(read, write, tx_clone).await;
                    });
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}
