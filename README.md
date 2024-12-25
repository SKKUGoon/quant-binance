# Data Gathering with Binance Websocket Client

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