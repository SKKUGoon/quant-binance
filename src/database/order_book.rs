use std::collections::HashMap;
use std::time::SystemTime;
use tokio_postgres::Client;

pub async fn insert_order_book(
    client: &Client,
    time: chrono::DateTime<chrono::Utc>,
    bids: HashMap<String, String>,
    asks: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let time_sys: SystemTime = time.into();
    for (price, quantity) in bids.iter() {
        client
            .execute(
                "INSERT INTO binance.order_books (time, price_level, quantity, side) VALUES ($1, $2, $3, 'bid')",
                &[&time_sys, &price, &quantity.parse::<f32>().unwrap()],
            )
            .await?;
    }

    for (price, quantity) in asks.iter() {
        client
            .execute(
                "INSERT INTO binance.order_books (time, price_level, quantity, side) VALUES ($1, $2, $3, 'ask')",
                &[&time_sys, &price, &quantity.parse::<f32>().unwrap()],
            )
            .await?;
    }

    Ok(())
}
