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

    let mut combined_data = Vec::new();
    for liquid in liquidations {
        combined_data.push((
            liquid.E as f64 / 1000.0,
            liquid.o.s.clone(),
            liquid.o.S.clone(),
            liquid.o.o.clone(),
            liquid.o.f.clone(),
            liquid.o.q.parse::<f32>()?,
            liquid.o.p.parse::<f32>()?,
            liquid.o.ap.parse::<f32>()?,
            liquid.o.X.clone(),
            liquid.o.l.parse::<f32>()?,
            liquid.o.z.parse::<f32>()?,
            liquid.o.T as f64 / 1000.0,
        ));
    }

    for chunks in combined_data.chunks(10) {
        let mut placeholders = Vec::new();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut param_index = 1;

        for (
            event_time,
            symbol,
            side,
            order_type,
            time_in_force,
            quantity,
            price,
            avg_price,
            order_status,
            last_filled_quantity,
            total_filled_quantity,
            trade_time,
        ) in chunks
        {
            placeholders.push(format!(
                "(to_timestamp(${}::FLOAT8), ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, to_timestamp(${}::FLOAT8))",
                param_index, param_index + 1, param_index + 2, param_index + 3, param_index + 4, param_index + 5, param_index + 6, param_index + 7, param_index + 8, param_index + 9, param_index + 10, param_index + 11
            ));
            params.push(Box::new(event_time));
            params.push(Box::new(symbol));
            params.push(Box::new(side));
            params.push(Box::new(order_type));
            params.push(Box::new(time_in_force));
            params.push(Box::new(quantity));
            params.push(Box::new(price));
            params.push(Box::new(avg_price));
            params.push(Box::new(order_status));
            params.push(Box::new(last_filled_quantity));
            params.push(Box::new(total_filled_quantity));
            params.push(Box::new(trade_time));
            param_index += 11;
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
    }

    Ok(())
}
