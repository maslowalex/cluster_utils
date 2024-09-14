use cluster_utils::request;

fn main() {
    // print current date in format: yyyy-mm-dd
    let date = chrono::Local::now().format("%Y-%m-%d").to_string();

    request::get_trades("BTCUSD", 2);
}