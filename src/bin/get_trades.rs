use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

fn main() {
    let trades = bybit::get_trades("BTCUSD", 1);

    // specify the aggregation rule to be time based and the resolution each trade timestamp has
    let time_rule = TimeRule::new(M15, TimestampResolution::Second);
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

    clusters.iter_mut().for_each(|c| c.finalize());

    let ten: Vec<Cluster> = clusters.into_iter().collect();

    let json = serde_json::to_string(&ten).unwrap();

    // write to file
    let file_name = format!("data/{}-clusters.json", "15m");
    std::fs::write(file_name, json).unwrap();
}
