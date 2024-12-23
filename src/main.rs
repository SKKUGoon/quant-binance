mod binance;
mod database;

use binance::websocket::{connect_to_binance, BinanceData};

use database::postgres::timescale_writer;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let symbol = "btcusdt";
    let (tx, rx) = mpsc::channel::<BinanceData>(100);

    tokio::spawn(async move {
        if let Err(e) = timescale_writer(rx).await {
            eprintln!("Failed to start timescale writer: {}", e);
        }
    });

    if let Err(e) = connect_to_binance(symbol, tx).await {
        eprintln!("Error: {}", e);
    }

    // Keep the main task running
    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
