mod binance;
mod database;

use binance::websocket::connect_to_binance;

#[tokio::main]
async fn main() {
    let symbol = "btcusdt";

    if let Err(e) = connect_to_binance(symbol).await {
        eprintln!("Error: {}", e);
    }

    // Keep the main task running
    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
