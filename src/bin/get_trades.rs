use cluster_utils::common::{Cluster, Trade};
use cluster_utils::request;
use trade_aggregation::*;

fn main() {
    let trades = request::get_trades("BTCUSD", 1);

    // specify the aggregation rule to be time based and the resolution each trade timestamp has
    let time_rule = TimeRule::new(M5, TimestampResolution::Second);
    // Notice how the aggregator is generic over the output candle type,
    // the aggregation rule as well as the input trade data
    let mut aggregator = GenericAggregator::<Cluster, TimeRule, Trade>::new(time_rule, false);

    // let candles = aggregate_all_trades(&trades, &mut aggregator);

    // for c in candles.iter() {
    //     dbg!(c);
    // }
    let mut clusters: Vec<Cluster> = Vec::new();

    for t in trades {
        if let Some(cluster) = aggregator.update(&t) {
            clusters.push(cluster);
        }
    }

    let ten: Vec<Cluster> = clusters.into_iter().take(10).collect();

    let json = serde_json::to_string(&ten).unwrap();

    // write to file
    let file_name = format!("data/{}-clusters.json", "5m");
    std::fs::write(file_name, json).unwrap();
}
