use std::collections::HashMap;

use crate::cex::binance::order_book::DepthEvent as BinanceDepthEvent;

#[derive(Debug, Clone)]
pub struct CombinedOrderBook {
    pub bids: HashMap<String, String>, // Price -> Quantity
    pub asks: HashMap<String, String>, // Price -> Quantity
    pub time: u64,
}

#[allow(dead_code)]
impl CombinedOrderBook {
    pub fn new() -> Self {
        Self {
            bids: HashMap::new(),
            asks: HashMap::new(),
            time: 0u64,
        }
    }

    pub fn update_binance(&mut self, event: &BinanceDepthEvent) {
        self.time = event.E;

        for (price, quantity) in event.b.iter() {
            if quantity == "0" {
                self.bids.remove(price);
            } else {
                self.bids.insert(price.clone(), quantity.clone());
            }
        }

        for (price, quantity) in event.a.iter() {
            if quantity == "0" {
                self.asks.remove(price);
            } else {
                self.asks.insert(price.clone(), quantity.clone());
            }
        }
    }

    pub fn update_bitget(&mut self) {
        // TODO: Implement Bitget order book update
    }

    pub fn update_okx(&mut self) {
        // TODO: Implement Okx order book update
    }
}
