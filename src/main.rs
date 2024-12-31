mod cex;
mod database;

use cex::binance::websocket::{BinanceData, BinanceStreamBuilder};
use clap::{Arg, Command};
use database::postgres::{feature_writer, timescale_batch_writer};
use log::{error, info};
use std::io::{stdout, Write};
use tokio::{signal, sync::mpsc, time::Instant};

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

    // Monitor buffer usage
    tokio::spawn({
        let start_time = Instant::now();
        let tx_mon_data = tx_data.clone(); // Clone `tx` specifically for monitoring
        let tx_mon_feat = tx_feature.clone(); // Clone `tx` specifically for monitoring
        async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                let elapsed = start_time.elapsed().as_secs();
                let formatted_time = format_elapsed_time(elapsed);

                info!(
                    "Time passed: {}s | Buffer Usage: (Data) {} / 9999 (Feature) {} / 9999 ",
                    formatted_time,
                    tx_mon_data.capacity(),
                    tx_mon_feat.capacity()
                );
                stdout().flush().unwrap();
            }
        }
    });

    // Timescale DB writer
    tokio::spawn(async move {
        if let Err(e) = timescale_batch_writer(rx_data).await {
            error!("Failed to start timescale writer: {}", e);
        }
    });

    // Feature writer
    tokio::spawn(async move {
        if let Err(e) = feature_writer(rx_feature).await {
            error!("Failed to start feature writer: {}", e);
        }
    });

    // Binance data stream
    tokio::spawn(async move {
        if let Err(e) = BinanceStreamBuilder::new(&symbol_data)
            // .with_depth()
            .with_agg_trade()
            .build(tx_data)
            .await
        {
            error!("Failed to connect to Binance Data stream: {}", e);
        }
    });

    // Binance feature stream
    tokio::spawn(async move {
        if let Err(e) = BinanceStreamBuilder::new(&symbol_feature)
            .with_depth()
            .with_agg_trade()
            .build(tx_feature)
            .await
        {
            error!("Failed to connect to Binance feature stream: {}", e);
        }
    });

    tokio::select! {
        // Keep alive
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }
}
