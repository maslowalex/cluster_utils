use clap::Parser;
use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

use std::sync::Arc;
use std::thread;

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

fn main() {
    let args = Arguments::parse();

    let trades = bybit::get_trades(&args.symbol, args.days_ago);
    println!("Total number of trades in batch: {}", trades.len());

    let trades = Arc::new(trades);

    let timeframes = vec![300, 600, 1800, 3600, 14400];
    let timeframes = Arc::new(timeframes);

    let handles: Vec<_> = (0..timeframes.len())
        .map(|timeframe: usize| {
            let data_clone = Arc::clone(&trades);
            let timeframes = Arc::clone(&timeframes);
            let symbol = args.symbol.clone();
            thread::spawn(move || {
                aggregate_trades(data_clone, timeframes[timeframe], &symbol, args.days_ago);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

fn aggregate_trades(trades: Arc<Vec<Trade>>, timeframe: i64, symbol: &str, days_ago: i32) -> () {
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
    let file_name = format!(
        "data/{}-{}-{}dago-clusters.json",
        symbol, timeframe, days_ago
    );
    std::fs::write(file_name, json).unwrap();
}
