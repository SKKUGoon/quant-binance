CREATE SCHEMA IF NOT EXISTS binance;

CREATE TABLE binance.order_books (
    time TIMESTAMPTZ NOT NULL,
    price_level TEXT NOT NULL,       -- Price level as a string (since it's a hashmap key)
    quantity FLOAT4 NOT NULL,        -- Quantity as a float
    side TEXT NOT NULL               -- "bid" or "ask"
);

-- Convert this table into a hypertable
SELECT create_hypertable('binance.order_books', 'time');

CREATE TABLE binance.liquidations (
    event_time TIMESTAMPTZ NOT NULL,  -- Directly store the event time as a timestamp
    symbol TEXT NOT NULL,             -- Symbol
    side TEXT NOT NULL,               -- "BUY" or "SELL"
    order_type TEXT NOT NULL,         -- Order Type
    time_in_force TEXT NOT NULL,      -- Time in force
    quantity FLOAT4 NOT NULL,         -- Quantity
    price FLOAT4 NOT NULL,            -- Price
    avg_price FLOAT4 NOT NULL,        -- Average Price
    order_status TEXT NOT NULL,       -- Order Status
    last_filled_quantity FLOAT4 NOT NULL,  -- Last Filled Quantity
    total_filled_quantity FLOAT4 NOT NULL, -- Filled Accumulated Quantity
    trade_time TIMESTAMPTZ NOT NULL   -- Trade time as a timestamp
);

-- Convert this table into a hypertable using event_time as the partition key
SELECT create_hypertable('binance.liquidations', 'event_time');

CREATE TABLE binance.agg_trades (
    event_time TIMESTAMPTZ NOT NULL,  -- Directly store the event time as a timestamp
    symbol TEXT NOT NULL,             -- Symbol
    aggregate_trade_id BIGINT NOT NULL, -- Aggregate trade ID
    price FLOAT4 NOT NULL,           -- Price
    quantity FLOAT4 NOT NULL,        -- Quantity
    first_trade_id BIGINT NOT NULL,  -- First trade ID
    last_trade_id BIGINT NOT NULL,   -- Last trade ID
    trade_time TIMESTAMPTZ NOT NULL, -- Trade time as a timestamp
    buyer_is_market_maker BOOLEAN NOT NULL -- Is the buyer the market maker?
);

-- Convert this table into a hypertable
SELECT create_hypertable('binance.agg_trades', 'event_time');

SELECT add_retention_policy('binance.order_books', INTERVAL '3 days');
SELECT add_retention_policy('binance.liquidations', INTERVAL '3 days');
SELECT add_retention_policy('binance.agg_trades', INTERVAL '3 days');