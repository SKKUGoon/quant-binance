use futures::{SinkExt, StreamExt};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub enum BitgetData {}

#[derive(Debug, Clone)]
pub struct BitgetStreamBuilder {
    symbol: String,
    streams: Vec<BitgetStreamArg>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct BitgetStreamArg {
    pub instType: String, // e.g., "USDT-FUTURES"
    pub channel: String,  // e.g., "books"
    pub instId: String,   // e.g., "BTCUSDT"
}

#[allow(dead_code)]
impl BitgetStreamBuilder {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_uppercase().to_string(),
            streams: Vec::new(),
        }
    }

    pub fn with_depth(mut self) -> Self {
        let arg = BitgetStreamArg {
            instType: "USDT-FUTURES".to_string(),
            channel: "books".to_string(),
            instId: self.symbol.clone(),
        };
        self.streams.push(arg);

        self
    }

    #[allow(unused_variables)]
    pub async fn build(
        self,
        tx: mpsc::Sender<BitgetData>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "Starting {} Bitget streams for {}",
            self.streams.len(),
            self.symbol
        );

        for stream in &self.streams {
            let ws_url = "wss://ws.bitget.com/v2/ws/public";
            let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await?;
            let (mut write, read) = ws_stream.split();

            // Send the subscription message (stream)
            let subscription_message = serde_json::json!({
                "op": "subscribe",
                "args": [stream]
            })
            .to_string();

            write
                .send(Message::Text(subscription_message.into()))
                .await?;

            // match stream.channel.as_str() {
            //     "books" => {
            //         let symbol = self.symbol.clone();
            //         let tx_clone = tx.clone();
            //         tokio::spawn(async move {
            //             handle_order_book(read, write, &mut)
            //         })
            //     }
            //     _ => unreachable!(),
            // }
        }
        Ok(())
    }
}
