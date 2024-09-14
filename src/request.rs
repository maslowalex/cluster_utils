use crate::common::Trade;
use chrono::{Duration, Utc};
use csv::StringRecord;
use std::path::Path;

const BASE_URL: &str = "https://public.bybit.com/trading";
const HEADERS: [&str; 10] = ["timestamp", "symbol", "side", "_", "price", "tick_direction", "trade_id", "_", "_", "volume"];

pub fn get_trades(symbol: &str, days_ago: i32) -> Vec<Trade> {
    let mut trades: Vec<Trade> = Vec::new();

    let current_date = Utc::now().date_naive();

    let dates: Vec<String> = (1..=days_ago)
    .map(|i| (current_date - Duration::days(i as i64)).format("%Y-%m-%d").to_string())
    .collect();

    let available_files = get_available_files(symbol, dates);

    for file in available_files {
        let file = std::fs::File::open(file).unwrap();
        let buf_reader = std::io::BufReader::new(file);
        let mut rdr = csv::Reader::from_reader(buf_reader);

        let headers = StringRecord::from(HEADERS.to_vec());

        for result in rdr.records() {
            let record = result.unwrap();

            let trade: Trade = record.deserialize::<Trade>(Some(&headers)).unwrap();

            trades.push(trade);
        }

    }

    dbg!(&trades);

    trades
}

fn get_available_files(symbol: &str, dates: Vec<String>) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();

    for date in dates {
        let symbol_date = format!("{}{}", symbol, date);
        let url = format!("{}/{}/{}.csv.gz", BASE_URL, symbol, symbol_date);
        let csv_file_path: String = format!("/tmp/{}.csv", symbol_date);

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
