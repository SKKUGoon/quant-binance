use chrono;
use std::time::SystemTime;
use tokio_postgres::Client;

use crate::binance::agg_trade::AggregateTradeEvent;

pub async fn insert_agg_trade(
    client: &Client,
    agg_trade_event: AggregateTradeEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_time: SystemTime = chrono::DateTime::<chrono::Utc>::from(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(agg_trade_event.E),
    )
    .into();

    let trade_time: SystemTime = chrono::DateTime::<chrono::Utc>::from(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(agg_trade_event.T),
    )
    .into();

    client
        .execute(
            "INSERT INTO binance.agg_trades (
                 event_time, symbol, aggregate_trade_id, price, quantity, first_trade_id, 
                 last_trade_id, trade_time, buyer_is_market_maker
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9
             )",
            &[
                &event_time,
                &agg_trade_event.s,
                &(agg_trade_event.a as i64),
                &agg_trade_event.p.parse::<f32>()?,
                &agg_trade_event.q.parse::<f32>()?,
                &(agg_trade_event.f as i64),
                &(agg_trade_event.l as i64),
                &trade_time,
                &agg_trade_event.m,
            ],
        )
        .await?;

    Ok(())
}
