use tokio_postgres::Client;

use crate::cex::binance::agg_trade::AggregateTradeEvent;

#[allow(dead_code)]
pub async fn insert_agg_trade(
    client: &Client,
    agg_trade_event: AggregateTradeEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(
            "INSERT INTO binance.agg_trades (
                event_time, symbol, aggregate_trade_id, price, quantity, first_trade_id, 
                last_trade_id, trade_time, buyer_is_market_maker
            ) VALUES (
                to_timestamp($1::FLOAT8), $2, $3, $4, $5, $6, $7, to_timestamp($8::FLOAT8), $9
            )",
            &[
                &(agg_trade_event.E as f64 / 1000.0), // Event time in milliseconds
                &agg_trade_event.s,                   // Symbol
                &(agg_trade_event.a as i64),          // Aggregate trade ID
                &agg_trade_event.p.parse::<f32>()?,
                &agg_trade_event.q.parse::<f32>()?,
                &(agg_trade_event.f as i64),          // First trade ID
                &(agg_trade_event.l as i64),          // Last trade ID
                &(agg_trade_event.T as f64 / 1000.0), // Trade time in milliseconds
                &agg_trade_event.m,                   // Buyer is market maker
            ],
        )
        .await?;

    Ok(())
}

#[allow(dead_code)]
pub async fn batch_insert_agg_trade(
    client: &Client,
    agg_trades: Vec<AggregateTradeEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    if agg_trades.is_empty() {
        return Ok(());
    }

    // Base query for the batch insert
    let base_query = String::from(
        "INSERT INTO binance.agg_trades (
        event_time, symbol, aggregate_trade_id, price, quantity, first_trade_id,
        last_trade_id, trade_time, buyer_is_market_maker
    ) VALUES ",
    );

    let mut placeholders = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

    for (i, agg_trade) in agg_trades.iter().enumerate() {
        // Each record requires 9 parameters
        let offset = i * 9;
        placeholders.push(format!(
        "(to_timestamp(${}::FLOAT8), ${}, ${}, ${}, ${}, ${}, ${}, to_timestamp(${}::FLOAT8), ${})",
        offset + 1, offset + 2, offset + 3, offset + 4, offset + 5, offset + 6, offset + 7, offset + 8, offset + 9,
    ));

        // Add parameters
        params.push(Box::new(agg_trade.E as f64 / 1000.0));
        params.push(Box::new(agg_trade.s.clone()));
        params.push(Box::new(agg_trade.a as i64));
        params.push(Box::new(agg_trade.p.parse::<f32>()?));
        params.push(Box::new(agg_trade.q.parse::<f32>()?));
        params.push(Box::new(agg_trade.f as i64));
        params.push(Box::new(agg_trade.l as i64));
        params.push(Box::new(agg_trade.T as f64 / 1000.0));
        params.push(Box::new(agg_trade.m));
    }

    // Join all placeholders into final query
    let query = format!("{}{}", base_query, placeholders.join(","));
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
