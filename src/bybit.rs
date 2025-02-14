use crate::common::{Side, Trade};
use chrono::{Duration, NaiveTime, Utc};
use csv::ReaderBuilder;
use csv::StringRecord;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::iter::Enumerate;
use std::path::Path;

const BASE_URL: &str = "https://public.bybit.com/trading";
static HEADERS: Lazy<StringRecord> = Lazy::new(|| {
    StringRecord::from(vec![
        "timestamp",
        "symbol",
        "side",
        "_",
        "price",
        "_",
        "_",
        "_",
        "_",
        "volume",
    ])
});

#[derive(Debug, Deserialize)]
struct BybitTrade {
    timestamp: f64,
    symbol: String,
    side: String,
    price: f64,
    volume: f64,
}

impl BybitTrade {
    fn to_trade(&self) -> Trade {
        let side = match self.side.as_str() {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            _ => panic!("Invalid side"),
        };
        let timestamp: f64 = self.timestamp * 1000.0;

        Trade::new(
            self.symbol.clone(),
            maybe_round_price(self.price),
            self.volume,
            timestamp as f64,
            side,
        )
    }
}

pub fn deserialize_row(record: &StringRecord) -> Trade {
    let byit_trade: BybitTrade = record.deserialize::<BybitTrade>(Some(&HEADERS)).unwrap();
    byit_trade.to_trade()
}

pub fn get_trades_csvs(symbol: &str, days_ago: i32) -> Vec<String> {
    let current_date = Utc::now().date_naive();

    let dates: Vec<String> = (1..=days_ago)
        .map(|i| {
            (current_date - Duration::days(i as i64))
                .format("%Y-%m-%d")
                .to_string()
        })
        .collect();

    get_available_files(symbol, dates)
}

fn get_available_files(symbol: &str, dates: Vec<String>) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();

    for date in dates {
        let symbol_date = format!("{}{}", symbol, date);
        let url = format!("{}/{}/{}.csv.gz", BASE_URL, symbol, symbol_date);
        let csv_file_path: String = format!("tmp/{}.csv", symbol_date);

        if Path::new(&csv_file_path).exists() {
            files.push(csv_file_path);
            continue;
        }

        // Download, and save the file
        let response = ureq::get(&url).call();
        if response.is_ok() {
            let mut file = std::fs::File::create(format!("/tmp/{}.csv.gz", symbol_date)).unwrap();
            std::io::copy(&mut response.unwrap().into_reader(), &mut file).unwrap();

            // Then unzip the file
            let gz_file = std::fs::File::open(format!("/tmp/{}.csv.gz", symbol_date)).unwrap();
            let mut gz_decoder = flate2::read::GzDecoder::new(gz_file);
            let mut out_file = std::fs::File::create(&csv_file_path).unwrap();
            std::io::copy(&mut gz_decoder, &mut out_file).unwrap();

            files.push(csv_file_path);
        } else {
            println!("{:?}", response.unwrap_err());
        }
    }

    files
}

pub fn get_trades_lazy<'a>(file: &str) -> impl Iterator<Item = Trade> {
    let file = std::fs::File::open(file).unwrap();
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    let mut first_trade = true;

    let trades_iterator = rdr.into_records().map(move |result| {
        let record = result.expect("Error reading CSV record");
        let mut trade = deserialize_row(&record);

        if first_trade {
            first_trade = false;
            let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let date = chrono::DateTime::from_timestamp_millis(trade.timestamp as i64).unwrap();
            let corrected_ts = date.with_time(midnight).unwrap().timestamp_millis() as f64;
            trade = Trade::new(
                trade.symbol.clone(),
                trade.price,
                trade.volume,
                corrected_ts,
                trade.side,
            );
        }
        trade
    });

    trades_iterator
}

fn maybe_round_price(price: f64) -> f64 {
    match price {
        price if price > 10_000.0 =>
        // Round to 10s
        {
            (price / 10.0).round() * 10.0
        }
        price if price > 1000.0 =>
        // Round to 5s
        {
            (price / 5.0).round() * 5.0
        }
        _ => price,
    }
}
