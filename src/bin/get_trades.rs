use clap::Parser;
use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

use std::time::Instant;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{broadcast, mpsc};
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
    let (tx_trades, _) = broadcast::channel::<Trade>(1_000_000);
    let (tx_clusters, _) = broadcast::channel::<Cluster>(10_000_000);

    for t in &timeframes {
        let rx_worker = tx_trades.subscribe();
        let tx_worker = tx_clusters.clone();
        task::spawn(aggregate_trades(rx_worker, tx_worker, *t));
    }

    // Spawn the writer task
    let writer_tasks: Vec<_> = timeframes.clone().iter().map(|t| {
        let worker_rx = tx_clusters.subscribe();
        task::spawn(write_clusters(
            worker_rx,
            args.symbol.clone(),
            *t,
            args.days_ago,
        ))
    }).collect();

    // Stream trades to workers via the broadcast channel
    for trade in trades {
        tx_trades.send(trade).expect("Failed to send trade");
    }

    // Drop the sender so workers can finish
    drop(tx_trades);
    drop(tx_clusters); // Close cluster sender so writer exits

    for writer_task in writer_tasks.into_iter() {
        writer_task.await.expect("Failed to join writer task");
    }
}
async fn aggregate_trades(
    mut rx: broadcast::Receiver<Trade>,
    tx_clusters: broadcast::Sender<Cluster>,
    timeframe: i64,
) -> () {
    // specify the aggregation rule to be time based and the resolution each trade timestamp has
    let time_rule = TimeRule::new(timeframe, TimestampResolution::Millisecond);
    // Notice how the aggregator is generic over the output candle type,
    // the aggregation rule as well as the input trade data
    let mut aggregator = GenericAggregator::<Cluster, TimeRule, Trade>::new(time_rule, true);

    while let Ok(trade) = rx.recv().await {
        if let Some(mut cluster) = aggregator.update(&trade) {
            cluster.finalize();
            tx_clusters
                .send(cluster)
                .expect("Failed to send cluster");
        }
    }
}

/// Writer task: collects clusters from workers and writes them in bulk
async fn write_clusters(
    mut rx_clusters: broadcast::Receiver<Cluster>,
    symbol: String,
    timeframe: i64,
    days_ago: i32,
) {
    let mut clusters = Vec::new();
    let filename = format!(
        "data/{}-{}-{}dago-clusters.json",
        symbol, timeframe, days_ago
    );
    let file = File::create(&filename)
        .await
        .expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    while let Ok(cluster) = rx_clusters.recv().await {
        clusters.push(cluster);
    }

    let json = serde_json::to_string(&clusters).unwrap();
    writer
        .write_all(json.as_bytes())
        .await
        .expect("Failed to write to file");
    writer.flush().await.expect("Failed to flush buffer");
}
