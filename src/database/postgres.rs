use crate::database::{
    features::orderbook_imbalance::{
        calculate_feature_one,
        insert_feature_one, // insert_feature_orderbook_imbalance,
    },
    order_book::batch_insert_order_book,
};
use crate::{
    cex::binance::websocket::BinanceData,
    database::{
        agg_trade::{batch_insert_agg_trade, insert_agg_trade},
        liquidation::{batch_insert_liquidation, insert_liquidation},
        order_book::insert_order_book,
    },
};
use log::error;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

#[allow(dead_code)]
pub async fn connect_to_timescaledb() -> Result<Client, Box<dyn std::error::Error>> {
    let connection_str =
        "host=localhost port=10501 user=postgres password=postgres dbname=postgres";

    let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
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
                    error!("Failed to insert order book update: {}", e);
                }
            }
            BinanceData::Liquidation(liquidation_event) => {
                if let Err(e) = insert_liquidation(&client, liquidation_event).await {
                    error!("Failed to insert liquidation event: {}", e);
                }
            }
            BinanceData::AggTrade(agg_trade_event) => {
                if let Err(e) = insert_agg_trade(&client, agg_trade_event).await {
                    error!("Failed to insert agg trade event: {}", e);
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn feature_writer(
    mut rx: mpsc::Receiver<BinanceData>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect_to_timescaledb().await?;
    let mut current_price = String::from("0.0");

    while let Some(event) = rx.recv().await {
        match event {
            BinanceData::OrderBook(order_book_update) => {
                let time = order_book_update.time as f64;
                let feature_one_05 = calculate_feature_one(
                    order_book_update.bids.clone(),
                    order_book_update.asks.clone(),
                    current_price.clone(),
                    0.05,
                );
                let feature_one_10 = calculate_feature_one(
                    order_book_update.bids.clone(),
                    order_book_update.asks.clone(),
                    current_price.clone(),
                    0.1,
                );

                if let Err(e) = insert_feature_one(&client, time, feature_one_05, 0.05).await {
                    error!("Failed to insert order book imbalance: {}", e);
                }

                if let Err(e) = insert_feature_one(&client, time, feature_one_10, 0.1).await {
                    error!("Failed to insert order book imbalance: {}", e);
                }
            }
            BinanceData::AggTrade(agg_trade_event) => {
                current_price = agg_trade_event.p.clone();
            }
            _ => {}
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn timescale_batch_writer(
    mut rx: mpsc::Receiver<BinanceData>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut order_book_bids = Vec::new();
    let mut order_book_asks = Vec::new();
    let mut liquidations = Vec::new();
    let mut agg_trades = Vec::new();

    let client = connect_to_timescaledb().await?;
    while let Some(event) = rx.recv().await {
        // Dynamic batch size adjustment based on the buffer usage
        let batch_size = if 9999 - rx.capacity() > 1000 {
            500
        } else {
            100
        };

        match event {
            BinanceData::OrderBook(order_book_update) => {
                let time = order_book_update.time as f64;
                order_book_bids.push((order_book_update.bids.clone(), time));
                order_book_asks.push((order_book_update.asks.clone(), time));

                if order_book_bids.len() >= batch_size || order_book_asks.len() >= batch_size {
                    if let Err(e) = batch_insert_order_book(
                        &client,
                        std::mem::take(&mut order_book_bids),
                        std::mem::take(&mut order_book_asks),
                    )
                    .await
                    {
                        error!("Failed to insert order book update: {}", e);
                    }
                }
            }
            BinanceData::Liquidation(liquidation_event) => {
                liquidations.push(liquidation_event);
                // Liquidations are less frequent, so we can batch them less frequently
                if liquidations.len() >= batch_size / 10 {
                    if let Err(e) =
                        batch_insert_liquidation(&client, std::mem::take(&mut liquidations)).await
                    {
                        error!("Failed to insert liquidation events: {}", e);
                    }
                }
            }
            BinanceData::AggTrade(agg_trade_event) => {
                agg_trades.push(agg_trade_event);
                if agg_trades.len() >= batch_size {
                    if let Err(e) =
                        batch_insert_agg_trade(&client, std::mem::take(&mut agg_trades)).await
                    {
                        error!("Failed to insert agg trade events: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}
