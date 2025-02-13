This library is the *hobby project* and not a *production-grade code*.

The purpose of this library is to explore the concept of clusters in trading and model some familiar problem domain with Rust.

Clusters is the "candlesticks under the magnifying glass", unlike the candles it gives more insights
into the candle structure itself, for example:

- You can identify the variety of "cluster patterns" based on the POC location in candle (Bottom, Middle or Top).
- You can gather insights about the delta between the sell and buy market orders.

The naive `bin` solution is implemented to demonstrate what clusters is,
it  and does the calculations across the different timeframes.

Try it out:

```bash
cargo run --bin get_trades -- --period-sec 300 --symbol BTCUSDT --days-ago 1
```

It will do following:
- downloads historical trades for `symbol` from the Bybit exchange public endpoint
- Perform the calculations across 4 different timeframes (5m, 10m, 30m, 1h)
- Save the result to data folder
