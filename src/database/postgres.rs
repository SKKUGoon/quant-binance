use crate::{
    binance::websocket::BinanceData,
    database::{
        agg_trade::insert_agg_trade, liquidation::insert_liquidation, order_book::insert_order_book,
    },
};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

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
