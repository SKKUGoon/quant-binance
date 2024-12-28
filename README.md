# Data Gathering with Binance Websocket Client

![Badge](https://img.shields.io/badge/Rust-000000.svg?&logo=Rust&logoColor=fff)
![Badge](https://img.shields.io/badge/Binance-F0B90B.svg?&logo=Binance&logoColor=fff)
![Badge](https://img.shields.io/badge/Timescale-FDB515.svg?&logo=Timescale&logoColor=fff)


## Function

- Collects data from Binance Websocket for single coin
- Writes data to Timescale DB

## Setup

Prepare TimeScale postgres database.

Change the `connection_str` in `src/database/postgres.rs`'s `connect_to_timescaledb` function

## Run

```bash
# Symbol is required (in lowercase)
cargo run -- --symbol btcusdt
```

```bash
# Set log level
RUST_LOG=info cargo run -- --symbol btcusdt
```
