use crate::{
    binance::websocket::BinanceData,
    database::{
        agg_trade::batch_insert_agg_trade, agg_trade::insert_agg_trade,
        liquidation::batch_insert_liquidation, liquidation::insert_liquidation,
        order_book::insert_order_book,
    },
};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

use super::order_book::batch_insert_order_book;

#[allow(dead_code)]
pub async fn connect_to_timescaledb() -> Result<Client, Box<dyn std::error::Error>> {
    let connection_str =
        "host=localhost port=10501 user=postgres password=postgres dbname=postgres";

    let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

#[allow(dead_code)]
pub async fn timescale_writer(
    mut rx: mpsc::Receiver<BinanceData>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect_to_timescaledb().await?;
    while let Some(event) = rx.recv().await {
        match event {
            BinanceData::OrderBook(order_book_update) => {
                let time = chrono::Utc::now();
                if let Err(e) = insert_order_book(
                    &client,
                    time,
                    order_book_update.bids,
                    order_book_update.asks,
                )
                .await
                {
                    eprintln!("Failed to insert order book update: {}", e);
                }
            }
            BinanceData::Liquidation(liquidation_event) => {
                if let Err(e) = insert_liquidation(&client, liquidation_event).await {
                    eprintln!("Failed to insert liquidation event: {}", e);
                }
            }
            BinanceData::AggTrade(agg_trade_event) => {
                if let Err(e) = insert_agg_trade(&client, agg_trade_event).await {
                    eprintln!("Failed to insert agg trade event: {}", e);
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn timescale_batch_writer(
    mut rx: mpsc::Receiver<BinanceData>,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut order_book_bids = Vec::new();
    let mut order_book_asks = Vec::new();
    let mut liquidations = Vec::new();
    let mut agg_trades = Vec::new();

    let client = connect_to_timescaledb().await?;
    while let Some(event) = rx.recv().await {
        match event {
            BinanceData::OrderBook(order_book_update) => {
                let time = order_book_update.time as f64;
                order_book_bids.push(order_book_update.bids.clone());
                order_book_asks.push(order_book_update.asks.clone());
                if order_book_bids.len() >= batch_size || order_book_asks.len() >= batch_size {
                    if let Err(e) = batch_insert_order_book(
                        &client,
                        time,
                        std::mem::take(&mut order_book_bids),
                        std::mem::take(&mut order_book_asks),
                    )
                    .await
                    {
                        eprintln!("Failed to insert order book update: {}", e);
                    }
                }
            }
            BinanceData::Liquidation(liquidation_event) => {
                liquidations.push(liquidation_event);
                if liquidations.len() >= batch_size {
                    if let Err(e) =
                        batch_insert_liquidation(&client, std::mem::take(&mut liquidations)).await
                    {
                        eprintln!("Failed to insert liquidation events: {}", e);
                    }
                }
            }
            BinanceData::AggTrade(agg_trade_event) => {
                agg_trades.push(agg_trade_event);
                if agg_trades.len() >= batch_size {
                    if let Err(e) =
                        batch_insert_agg_trade(&client, std::mem::take(&mut agg_trades)).await
                    {
                        eprintln!("Failed to insert agg trade events: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}
