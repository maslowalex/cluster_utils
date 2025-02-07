use clap::Parser;
use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::task;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[clap(short, long)]
    period_sec: i64,

    #[clap(short, long)]
    symbol: String,

    #[clap(short, long, default_value = "1")]
    days_ago: i32,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    let trades = bybit::get_trades(&args.symbol, args.days_ago);
    println!("Total number of trades in batch: {}", trades.len());

    let timeframes = vec![300, 600, 1800, 3600, 14400];

    let mut tasks = Vec::new();

    for (_i, item) in timeframes.iter().enumerate() {
        let trades = trades.clone();
        let symbol = args.symbol.clone(); // Full copy of the data

        tasks.push(task::spawn(aggregate_trades(
            trades,
            *item,
            symbol,
            args.days_ago,
        )));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

async fn aggregate_trades(trades: Vec<Trade>, timeframe: i64, symbol: String, days_ago: i32) -> () {
    // specify the aggregation rule to be time based and the resolution each trade timestamp has
    let time_rule = TimeRule::new(timeframe, TimestampResolution::Millisecond);
    // Notice how the aggregator is generic over the output candle type,
    // the aggregation rule as well as the input trade data
    let mut aggregator = GenericAggregator::<Cluster, TimeRule, Trade>::new(time_rule, true);

    let mut clusters: Vec<Cluster> = Vec::new();

    let start = std::time::Instant::now();
    for t in trades.iter() {
        if let Some(mut cluster) = aggregator.update(&t) {
            cluster.finalize();

            clusters.push(cluster);
        }
    }

    let elapsed = start.elapsed();
    println!("Final: {:?}", elapsed);

    println!("Clusters: {}", clusters.len());
    println!("Generating JSON");
    let json = serde_json::to_string(&clusters).unwrap();
    println!("Writing to file");

    // write to file
    let filename = format!(
        "data/{}-{}-{}dago-clusters.json",
        symbol, timeframe, days_ago
    );

    let file = File::create(&filename)
        .await
        .expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    writer
        .write_all(json.as_bytes())
        .await
        .expect("Failed to write to file");
    writer.flush().await.expect("Failed to flush buffer");
}
