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

#[derive(Debug, Serialize, Clone)]
enum Pressure {
    Top,
    Middle,
    Bottom,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Cluster {
    #[serde(skip_serializing)]
    pub levels: Vec<Level>,
    pub ts: i64,
    poc: Option<Level>,
    pressure: Option<Pressure>,
    height: Option<usize>,
    volume: Option<f64>,
    volume_delta: Option<f64>,
    trades_delta: Option<i32>,
}

impl Cluster {
    fn default() -> Cluster {
        Cluster {
            levels: Vec::new(),
            ts: 0,
            poc: None,
            pressure: None,
            height: None,
            volume: None,
            volume_delta: None,
            trades_delta: None,
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

    pub fn finalize(&mut self) {
        self.sort_levels();

        let (poc_index, point_of_control) = self
            .levels
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.volume.partial_cmp(&b.1.volume).unwrap())
            .unwrap();

        self.poc = Some(point_of_control.clone());

        let cluster_length = self.levels.len();
        let poc_fraction = poc_index as f64 / cluster_length as f64;

        if poc_fraction < 0.33 {
            self.pressure = Some(Pressure::Top);
        } else if poc_fraction > 0.66 {
            self.pressure = Some(Pressure::Bottom);
        } else {
            self.pressure = Some(Pressure::Middle);
        }

        self.height = Some(cluster_length);

        self.levels.iter().for_each(|l| {
            self.volume = Some(self.volume.unwrap_or(0.0) + l.volume);
            self.volume_delta = Some(self.volume_delta.unwrap_or(0.0) + l.volume_delta);
            self.trades_delta = Some(self.trades_delta.unwrap_or(0) + l.trades_delta);
        });
    }

    fn sort_levels(&mut self) {
        self.levels
            .sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
    }
}
