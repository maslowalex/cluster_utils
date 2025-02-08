use clap::Parser;
use cluster_utils::bybit;
use cluster_utils::common::{Cluster, Trade};
use trade_aggregation::*;

use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tokio::time::{sleep, Duration};

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
    let (tx_trades, _) = broadcast::channel::<Trade>(trades.len());
    let mut tx_clusters_map = HashMap::new();
    let mut worker_handles = Vec::new();

    // Create a dedicated `mpsc::channel` for each timeframe
    for &timeframe in &timeframes {
        let (tx_clusters, rx_clusters) = mpsc::channel::<Cluster>(5000);
        tx_clusters_map.insert(timeframe, tx_clusters);

        task::spawn(write_clusters(
            rx_clusters,
            args.symbol.clone(),
            timeframe,
            args.days_ago,
        ));
    }

    // Spawn worker tasks
    for tf in &timeframes {
        let rx_worker = tx_trades.subscribe(); // Subscribe before sending
        let cluster_woker = tx_clusters_map.get(tf).unwrap().clone();

        let task = task::spawn(aggregate_trades(rx_worker, cluster_woker, *tf));
        worker_handles.push(task);
    }

    // Send trades AFTER workers are ready
    for trade in trades {
        tx_trades.send(trade.clone()).expect("Failed to send trade");
    }

    drop(tx_trades); // Ensure workers finish
    drop(tx_clusters_map); // Ensure writers finish

    for handle in worker_handles {
        handle.await.expect("Worker task panicked");
    }
}

async fn aggregate_trades(
    mut rx: broadcast::Receiver<Trade>,
    tx_clusters: mpsc::Sender<Cluster>,
    timeframe: i64,
) -> () {
    let time_rule = TimeRule::new(timeframe, TimestampResolution::Millisecond);
    let mut aggregator = GenericAggregator::<Cluster, TimeRule, Trade>::new(time_rule, true);

    while let Ok(trade) = rx.recv().await {
        if let Some(mut cluster) = aggregator.update(&trade) {
            cluster.finalize();
            tx_clusters
                .send(cluster)
                .await
                .expect("Failed to send cluster");
        }
    }
}

/// Writer task: collects clusters from workers and writes them in bulk
async fn write_clusters(
    mut rx_clusters: mpsc::Receiver<Cluster>,
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

    println!("Spawned writer for {}", timeframe);

    while let Some(cluster) = rx_clusters.recv().await {
        println!(
            "Pushing cluster for {} with {} levels",
            timeframe,
            &cluster.levels.len()
        );

        clusters.push(cluster);
    }

    let json = serde_json::to_string(&clusters).unwrap();

    writer
        .write_all(json.as_bytes())
        .await
        .expect("Failed to write to file");
    writer.flush().await.expect("Failed to flush buffer");
}
