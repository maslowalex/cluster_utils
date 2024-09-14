use serde::{de, Deserialize, Serialize};
use chrono::NaiveDateTime;

#[derive(Debug, Serialize, Deserialize)]
enum Side {
    Buy,
    Sell
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Trade {
    symbol: String,
    price: f64,
    volume: f64,
    timestamp: f64,
    side: Side
}

impl Trade {
    pub fn new(symbol: String, price: f64, volume: f64, timestamp: f64, side: Side) -> Trade {
        Trade {
            symbol,
            price,
            volume,
            timestamp,
            side
        }
    }
}
