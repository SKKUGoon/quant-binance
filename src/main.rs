mod binance;
mod database;

use binance::websocket::{connect_to_binance, BinanceData};
use clap::{Arg, Command};
use database::postgres::timescale_writer;
use std::io::{stdout, Write};
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

    format!("{}d {}h {}m {}s", days, hours, minutes, secs)
}

#[tokio::main]
async fn main() {
    let config = parse_args();
    let symbol = config.symbol;

    // Timescale DB writer
    let (tx, rx) = mpsc::channel::<BinanceData>(100);
    tokio::spawn(async move {
        if let Err(e) = timescale_writer(rx).await {
            eprintln!("Failed to start timescale writer: {}", e);
        }
    });

    // Time counter
    let start_time = Instant::now();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let elapsed = start_time.elapsed().as_secs();
            let formatted_time = format_elapsed_time(elapsed);
            print!("\rTime passed: {}s", formatted_time);
            stdout().flush().unwrap();
        }
    });

    if let Err(e) = connect_to_binance(symbol.as_str(), tx).await {
        eprintln!("Error: {}", e);
    }

    // Keep the main task running
    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
