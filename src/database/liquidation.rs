use crate::binance::liquidation::LiquidationEvent;
use tokio_postgres::Client;

pub async fn insert_liquidation(
    client: &Client,
    liquidation_event: LiquidationEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(
            "INSERT INTO binance.liquidations (
                event_time, symbol, side, order_type, time_in_force, quantity, price, avg_price, order_status, last_filled_quantity, total_filled_quantity, trade_time
            ) VALUES (
                to_timestamp($1::FLOAT8), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, to_timestamp($12::FLOAT8)
            )",
            &[
                &(liquidation_event.E as f64 / 1000.0),                               // Event time in seconds
                &liquidation_event.o.s,                   // Symbol
                &liquidation_event.o.S,                   // Side
                &liquidation_event.o.o,                   // Order type
                &liquidation_event.o.f,                   // Time in force
                &liquidation_event.o.q.parse::<f32>()?,   // Quantity
                &liquidation_event.o.p.parse::<f32>()?,   // Price
                &liquidation_event.o.ap.parse::<f32>()?,  // Average price
                &liquidation_event.o.X,                   // Order status
                &liquidation_event.o.l.parse::<f32>()?,   // Last filled quantity
                &liquidation_event.o.z.parse::<f32>()?,   // Total filled quantity
                &(liquidation_event.o.T as f64 / 1000.0),                              // Trade time in seconds
            ],
        )
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn batch_insert_liquidation(
    client: &Client,
    liquidations: Vec<LiquidationEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    if liquidations.is_empty() {
        return Ok(());
    }

    let base_query = String::from(
        "INSERT INTO binance.liquidations (
            event_time, symbol, side, order_type, time_in_force, quantity, price, avg_price, order_status, last_filled_quantity, total_filled_quantity, trade_time
        ) VALUES ",
    );

    let mut placeholders = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    for (i, liquidation) in liquidations.iter().enumerate() {
        let offset = i * 11;
        placeholders.push(format!(
            "(to_timestamp(${}::FLOAT8), ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, to_timestamp(${}::FLOAT8))",
            offset + 1, offset + 2, offset + 3, offset + 4, offset + 5, offset + 6, offset + 7, offset + 8, offset + 9, offset + 10, offset + 11, offset + 12
        ));

        params.push(Box::new(liquidation.E as f64 / 1000.0));
        params.push(Box::new(liquidation.o.s.clone()));
        params.push(Box::new(liquidation.o.S.clone()));
        params.push(Box::new(liquidation.o.o.clone()));
        params.push(Box::new(liquidation.o.f.clone()));
        params.push(Box::new(liquidation.o.q.parse::<f32>()?));
        params.push(Box::new(liquidation.o.p.parse::<f32>()?));
        params.push(Box::new(liquidation.o.ap.parse::<f32>()?));
        params.push(Box::new(liquidation.o.X.clone()));
        params.push(Box::new(liquidation.o.l.parse::<f32>()?));
        params.push(Box::new(liquidation.o.z.parse::<f32>()?));
        params.push(Box::new(liquidation.o.T as f64 / 1000.0));
    }

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
