use std::collections::HashMap;
use std::time::SystemTime;
use tokio_postgres::Client;

#[allow(dead_code)]
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

#[allow(dead_code)]
pub async fn batch_insert_order_book(
    client: &Client,
    time: f64,
    bids: Vec<HashMap<String, String>>,
    asks: Vec<HashMap<String, String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if bids.is_empty() && asks.is_empty() {
        return Ok(());
    }

    let base_query =
        String::from("INSERT INTO binance.order_books (time, price_level, quantity, side) VALUES ");

    let mut placeholders = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

    let mut param_index = 1;
    for bid_map in bids.iter() {
        for (price, quantity) in bid_map.iter() {
            if quantity.parse::<f32>()? == 0.0 {
                continue;
            }

            placeholders.push(format!(
                "(to_timestamp(${}::FLOAT8), ${}, ${}, ${})",
                param_index,
                param_index + 1,
                param_index + 2,
                param_index + 3
            ));
            params.push(Box::new(time / 1000.0));
            params.push(Box::new(price));
            params.push(Box::new(quantity.parse::<f32>()?));
            params.push(Box::new("bid"));
            param_index += 4;
        }
    }

    for ask_map in asks.iter() {
        for (price, quantity) in ask_map.iter() {
            if quantity.parse::<f32>()? == 0.0 {
                continue;
            }

            placeholders.push(format!(
                "(to_timestamp(${}::FLOAT8), ${}, ${}, ${})",
                param_index,
                param_index + 1,
                param_index + 2,
                param_index + 3
            ));
            params.push(Box::new(time / 1000.0));
            params.push(Box::new(price));
            params.push(Box::new(quantity.parse::<f32>()?));
            params.push(Box::new("ask"));
            param_index += 4;
        }
    }

    let query = format!("{}{}", base_query, placeholders.join(","));

    eprintln!("Generated Query: {}", query);
    eprintln!("Placeholder Count: {:?}", placeholders.len());
    eprintln!("Params Count: {:?}", params.len());
    client
        .execute(
            &query,
            &params
                .iter()
                .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect::<Vec<_>>(),
        )
        .await?;
    Ok(())
}
