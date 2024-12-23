use chrono;
use std::time::SystemTime;
use tokio_postgres::Client;

use crate::binance::liquidation::LiquidationEvent;

pub async fn insert_liquidation(
    client: &Client,
    liquidation_event: LiquidationEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_time: SystemTime = chrono::DateTime::<chrono::Utc>::from(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(liquidation_event.E),
    )
    .into();

    client
        .execute(
            "INSERT INTO binance.liquidations (
                event_time, symbol, side, order_type, time_in_force, quantity, price, avg_price, order_status, last_filled_quantity, total_filled_quantity, trade_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            &[
                &event_time,
                &liquidation_event.o.s,
                &liquidation_event.o.S,
                &liquidation_event.o.o,
                &liquidation_event.o.f,
                &liquidation_event.o.q.parse::<f32>().unwrap(),
                &liquidation_event.o.p.parse::<f32>().unwrap(),
                &liquidation_event.o.ap.parse::<f32>().unwrap(),
                &liquidation_event.o.X,
                &liquidation_event.o.l.parse::<f32>().unwrap(),
                &liquidation_event.o.z.parse::<f32>().unwrap(),
                &(liquidation_event.o.T as i64),
            ],
        )
        .await?;
    Ok(())
}
