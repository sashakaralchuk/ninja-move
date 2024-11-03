use exchanges_arbitrage::{domain, RedpandaPort};

const _: &str = r#"
-- redpanda
rpk topic create tickers-spot-futures-arbitrage
-- clickhouse
CREATE TABLE default.trade_spot_feature_arbitrage_v1
(
    read_topic String,
    read_error String,
    read_raw_message String,
    exchange String,
    symbol String,
    kind String,
    timestamp DateTime,
    price Float64
)
ENGINE = ReplacingMergeTree
-- TODO: find out toYYYYMMDDhh, p.s. toYYYYMMDDhhmmss is exists
-- XXX: squash duplicates based on ticker_id
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, kind, timestamp, price);
CREATE TABLE default.trade_spot_feature_arbitrage_v1_queue
(data String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda-1:9093',
         kafka_topic_list = 'tickers-spot-futures-arbitrage',
         kafka_group_name = 'clickhouse-consumer',
         kafka_format = 'JSONAsString',
         kafka_thread_per_consumer = 0,
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream',
         kafka_max_block_size = 100000;
CREATE MATERIALIZED VIEW default.trade_spot_feature_arbitrage_v1_mv
TO default.trade_spot_feature_arbitrage_v1 AS
SELECT
    _topic read_topic,
    _error read_error,
    _raw_message read_raw_message,
    JSONExtractString(data, 'ex') exchange,
    JSONExtractString(data, 's') symbol,
    JSONExtractString(data, 'k') kind,
    toDateTime(JSONExtractUInt(data, 'ts') / 1000) timestamp,
    JSONExtractFloat(data, 'p') price
FROM default.trade_spot_feature_arbitrage_v1_queue;
"#;

fn main() {
    env_logger::init();
    let (tx, rx) = std::sync::mpsc::channel();
    let t1_tx = tx.clone();
    let t2_tx = tx.clone();
    let _ = std::thread::spawn(move || {
        bybit_listen_derivative_tickers_stream(&t1_tx);
    });
    let _ = std::thread::spawn(move || {
        bybit_listen_spot_tickers_stream(&t2_tx);
    });
    let mut queue_tickers = vec![];
    loop {
        match rx.recv() {
            Ok(v) => queue_tickers.push(v),
            Err(e) => {
                log::error!("e={:?}", e);
                continue;
            }
        }
        if queue_tickers.len() == 100 {
            log::info!("produce len={}", queue_tickers.len());
            let queue_tickers_to_produce = queue_tickers
                .iter()
                .map(|x| serde_json::to_string(&x).unwrap())
                .collect::<Vec<_>>();
            let _ = RedpandaPort::connect_produce_messages_sync(
                "tickers-spot-futures-arbitrage",
                &queue_tickers_to_produce,
            );
            queue_tickers.clear();
        }
    }
}

fn bybit_listen_derivative_tickers_stream(tx: &std::sync::mpsc::Sender<QueueTicker>) {
    let url_obj = url::Url::parse(domain::bybit::URL_WS_V5_PUBLIC_LINEAR).unwrap();
    let (mut socket, _response) = tungstenite::connect(url_obj).unwrap();
    let subscribe_text = format!(
        "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
        "ETHUSDT",
    );
    socket
        .write_message(tungstenite::Message::Text(subscribe_text))
        .unwrap();
    loop {
        let msg = socket.read_message().unwrap();
        let msg_str = msg.to_text().unwrap();
        let val = serde_json::from_str::<serde_json::Value>(msg_str).unwrap();
        let topic = match val.get("type") {
            Some(v) => v.as_str().unwrap(),
            _ => "",
        };
        if topic != "delta" {
            continue;
        }
        let ts = val.get("ts").unwrap().as_i64().unwrap();
        let data = val.get("data").unwrap().as_object().unwrap();
        let data_symbol = data.get("symbol").unwrap().as_str().unwrap();
        let data_bid1_price = match data.get("bid1Price") {
            Some(v) => v.as_str().unwrap(),
            _ => continue,
        };
        let ticker = QueueTicker::new("bybit", data_symbol, "derivatives", ts, data_bid1_price);
        log::debug!("ticker={ticker:?}");
        tx.send(ticker).unwrap();
    }
}

fn bybit_listen_spot_tickers_stream(tx: &std::sync::mpsc::Sender<QueueTicker>) {
    let url_obj = url::Url::parse(domain::bybit::URL_WS_V5_PUBLIC_SPOT).unwrap();
    let (mut socket, _response) = tungstenite::connect(url_obj).unwrap();
    let subscribe_text = format!(
        "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
        "ETHUSDT",
    );
    socket
        .write_message(tungstenite::Message::Text(subscribe_text))
        .unwrap();
    loop {
        let msg = socket.read_message().unwrap();
        let msg_str = msg.to_text().unwrap();
        let val = serde_json::from_str::<serde_json::Value>(msg_str).unwrap();
        let ts = match val.get("ts") {
            Some(v) => v.as_i64().unwrap(),
            _ => continue,
        };
        let data = val.get("data").unwrap().as_object().unwrap();
        let data_symbol = data.get("symbol").unwrap().as_str().unwrap();
        let data_last_price = data.get("lastPrice").unwrap().as_str().unwrap();
        let ticker = QueueTicker::new("bybit", data_symbol, "spot", ts, data_last_price);
        log::debug!("ticker={ticker:?}");
        tx.send(ticker).unwrap();
    }
}

#[derive(Debug, serde::Serialize)]
struct QueueTicker {
    ex: String,
    s: String,
    k: String,
    ts: i64,
    p: f64,
}

impl QueueTicker {
    fn new(ex: &str, s: &str, k: &str, ts: i64, p: &str) -> Self {
        let p_f64 = p.parse::<f64>().unwrap();
        Self {
            ex: ex.into(),
            s: s.into(),
            k: k.into(),
            ts,
            p: p_f64,
        }
    }
}
