use clap::Parser;
use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

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

    // specify the aggregation rule to be time based and the resolution each trade timestamp has
    let time_rule = TimeRule::new(args.period_sec, TimestampResolution::Millisecond);
    // Notice how the aggregator is generic over the output candle type,
    // the aggregation rule as well as the input trade data
    let mut aggregator = GenericAggregator::<Cluster, TimeRule, Trade>::new(time_rule, true);

    let mut clusters: Vec<Cluster> = Vec::new();

    for t in trades {
        if let Some(cluster) = aggregator.update(&t) {
            clusters.push(cluster);

            dbg!(&clusters.len());
        }
    }

    clusters.iter_mut().for_each(|c| c.finalize());

    let ten: Vec<Cluster> = clusters.into_iter().collect();

    let json = serde_json::to_string(&ten).unwrap();

    // write to file
    let file_name = format!(
        "data/{}-{}-{}dago-clusters.json",
        args.symbol, args.period_sec, args.days_ago
    );
    std::fs::write(file_name, json).unwrap();
}
