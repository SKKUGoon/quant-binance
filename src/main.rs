mod binance;
mod database;

use binance::websocket::{connect_to_binance, BinanceData};
use clap::{Arg, Command};
use database::postgres::{feature_writer, timescale_batch_writer};
use log::{error, info};
use std::io::{stdout, Write};
use tokio::signal;
use tokio::{sync::mpsc, time::Instant};
struct Config {
    symbol: String,
}

fn parse_args() -> Config {
    let matches = Command::new("Binance Data Collector")
        .version("1.0")
        .about("Collects data from Binance WebSocket")
        .arg(
            Arg::new("symbol")
                .short('S')
                .long("symbol")
                .value_name("SYMBOL")
                .help("The trading pair symbol (e.g., btcusdt)")
                .value_parser(clap::value_parser!(String))
                .required(true),
        )
        .get_matches();
    let symbol = matches.get_one::<String>("symbol").unwrap().to_string();

    Config { symbol }
}

fn format_elapsed_time(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    format!("{}d {:02}h {:02}m {:02}s", days, hours, minutes, secs)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let config = parse_args();
    let symbol_data = config.symbol;
    let symbol_feature = symbol_data.clone();

    // Timescale DB writer
    let (tx_data, rx_data) = mpsc::channel::<BinanceData>(9999);
    let (tx_feature, rx_feature) = mpsc::channel::<BinanceData>(9999);

    tokio::spawn(async move {
        if let Err(e) = timescale_batch_writer(rx_data).await {
            error!("Failed to start timescale writer: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = feature_writer(rx_feature).await {
            error!("Failed to start feature writer: {}", e);
        }
    });

    tokio::spawn({
        let start_time = Instant::now();
        let tx_monitor = tx_data.clone(); // Clone `tx` specifically for monitoring
        async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                let elapsed = start_time.elapsed().as_secs();
                let formatted_time = format_elapsed_time(elapsed);

                info!(
                    "Time passed: {}s | Buffer usage: {} / 9999 ",
                    formatted_time,
                    tx_monitor.capacity()
                );
                stdout().flush().unwrap();
            }
        }
    });

    tokio::spawn(async move {
        if let Err(e) = connect_to_binance(&symbol_feature, tx_feature).await {
            error!("Failed to connect to Binance: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = connect_to_binance(&symbol_data, tx_data).await {
            error!("Error: {}", e);
        }
    });

    tokio::select! {
        // Keep alive
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }
}
