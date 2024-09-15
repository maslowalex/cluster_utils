use serde::{Deserialize, Serialize};
use trade_aggregation::{ModularCandle, TakerTrade};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub enum Side {
    Buy,
    Sell,
    #[default]
    None,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Trade {
    symbol: String,
    price: f64,
    volume: f64,
    timestamp: f64,
    side: Side,
}

impl Trade {
    pub fn new(symbol: String, price: f64, volume: f64, timestamp: f64, side: Side) -> Trade {
        Trade {
            symbol,
            price,
            volume,
            timestamp,
            side,
        }
    }
}

impl TakerTrade for Trade {
    #[inline(always)]
    fn timestamp(&self) -> i64 {
        self.timestamp as i64
    }

    #[inline(always)]
    fn price(&self) -> f64 {
        self.price
    }

    #[inline(always)]
    fn size(&self) -> f64 {
        self.volume
    }
}

impl ModularCandle<Trade> for Cluster {
    fn update(&mut self, trade: &Trade) {
        self.update_levels(trade);
        self.ts = trade.timestamp as i64;
    }

    fn reset(&mut self) {
        Cluster::default();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Level {
    price: f64,
    volume: f64,
    volume_delta: f64,
    buy_trades: u32,
    sell_trades: u32,
    trades_delta: i32,
}

impl Level {
    fn new(trade: &Trade) -> Level {
        match trade.side {
            Side::None => panic!("Invalid side passed to Level::new"),
            Side::Buy => Level {
                price: trade.price,
                volume: trade.volume,
                volume_delta: trade.volume,
                buy_trades: 1,
                sell_trades: 0,
                trades_delta: 1,
            },
            Side::Sell => Level {
                price: trade.price,
                volume: trade.volume,
                volume_delta: -trade.volume,
                buy_trades: 0,
                sell_trades: 1,
                trades_delta: -1,
            },
        }
    }

    fn update(&mut self, trade: &Trade) {
        self.volume += trade.volume;

        match trade.side {
            Side::Buy => {
                self.volume_delta += trade.volume;
                self.buy_trades += 1;
                self.trades_delta += 1;
            }

            Side::Sell => {
                self.volume_delta -= trade.volume;
                self.sell_trades += 1;
                self.trades_delta -= 1;
            }
            Side::None => panic!("Invalid side passed to Level::update"),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Cluster {
    pub levels: Vec<Level>,
    pub ts: i64,
}

impl Cluster {
    fn default() -> Cluster {
        Cluster {
            levels: Vec::new(),
            ts: 0,
        }
    }

    fn update_levels(&mut self, trade: &Trade) {
        let mut found = false;
        for level in self.levels.iter_mut() {
            if level.price == trade.price {
                level.update(&trade);
                found = true;
                break;
            }
        }

        if !found {
            self.levels.push(Level::new(trade));
        }
    }
}
