use clap::Parser;
use exchanges_arbitrage::{pool, RedpandaPort, TelegramBotPort};
use std::collections::{HashMap, HashSet};

use trade_contango::trade_contango_client::TradeContangoClient;
use trade_contango::{FireTradeReqV2, QTickerReq, QTickerReqV2, SpreadsReqV2};

pub mod trade_contango {
    tonic::include_proto!("trade_contango");
}

const _: &str = r#"
-- redpanda
rpk topic create \
    -c retention.ms=900000 \
    -c segment.ms=900000 \
    -c segment.bytes=67108864 \
    -c retention.bytes=67108864 \
    tickers-contango-arbitrage
-- clickhouse
CREATE TABLE default.trade_contango_arbitrage_v1
(
    read_topic String,
    read_error String,
    read_raw_message String,
    exchange String,
    symbol String,
    kind String,
    timestamp DateTime,
    price Float64,
    volume Float64,
    status String
)
ENGINE = ReplacingMergeTree
PARTITION BY (toYYYYMMDD(timestamp), toHour(timestamp))
ORDER BY (exchange, symbol, kind, timestamp, price);
CREATE TABLE default.trade_contango_arbitrage_v1_queue
(data String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda-1:9093',
         kafka_topic_list = 'tickers-contango-arbitrage',
         kafka_group_name = 'clickhouse-consumer',
         kafka_format = 'JSONAsString',
         kafka_thread_per_consumer = 0,
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream',
         kafka_max_block_size = 100000;
CREATE MATERIALIZED VIEW default.trade_contango_arbitrage_v1_mv
TO default.trade_contango_arbitrage_v1 AS
SELECT
    _topic read_topic,
    _error read_error,
    _raw_message read_raw_message,
    JSONExtractString(data, 'ex') exchange,
    JSONExtractString(data, 's') symbol,
    JSONExtractString(data, 'k') kind,
    toDateTime(JSONExtractUInt(data, 'ts') / 1000) timestamp,
    JSONExtractFloat(data, 'p') price,
    JSONExtractFloat(data, 'v') volume,
    JSONExtractString(data, 'st') status
FROM default.trade_contango_arbitrage_v1_queue;
CREATE TABLE default.trade_contango_arbitrage_v1_diff_tracks
(
    ts DateTime,
    symbol_int_1 String,
    fut_price Float64,
    spot_price Float64,
    der_ex String,
    spot_ex String,
    diff_abs Float64,
    diff_rel Float64
)
ENGINE = Log;
-- select last prices
SELECT *
FROM (
    SELECT
        exchange,
        UPPER(replaceRegexpAll(symbol, '[10*_-]?', '')) symbol_int_1,
        kind,
        timestamp,
        price,
        ROW_NUMBER() OVER(
            PARTITION BY exchange, symbol_int_1, kind
            ORDER BY timestamp DESC
        ) _rownum
    FROM default.trade_contango_arbitrage_v1
    WHERE timestamp >= (now() - toIntervalSecond(60))
        AND status = 'TRADING'
)
WHERE _rownum = 1
    AND symbol_int_1 = 'METISUSDT'
    AND exchange = 'htx'
-- select last trades-diffs
WITH max_ts AS (
  SELECT max(ts) as ts
  FROM default.trade_contango_arbitrage_v1_diff_tracks
)
SELECT *
FROM default.trade_contango_arbitrage_v1_diff_tracks
WHERE ts IN max_ts
ORDER BY ts DESC
LIMIT 10
"#;

fn main() {
    env_logger::init();
    let run_args = RunArgs::parse();
    match run_args.command.as_str() {
        "fetch-process-tickers" => run_fetch_process_tickers(),
        "track-diff" => run_track_diff(),
        _ => log::error!("unknown command"),
    }
}

fn run_fetch_process_tickers() {
    binance_int::fetch_write_config();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut fns = vec![
        bybit_int::fetch_derivatives_tickers_from_api,
        bybit_int::fetch_spot_tickers_from_api,
        mexc_int::fetch_spot_tickers_from_api,
        gateio_int::fetch_derivatives_tickers_from_api,
        gateio_int::fetch_spot_tickers_from_api,
    ];
    if std::env::var("TRADE_CONTANGO_ALL_EXCHANGES").unwrap_or("0".into()) == "1" {
        fns.append(&mut vec![
            mexc_int::fetch_derivatives_tickers_from_api,
            htx_int::fetch_derivatives_tickers_from_api,
            htx_int::fetch_spot_tickers_from_api,
            binance_int::fetch_derivatives_tickers_from_api,
            binance_int::fetch_spot_tickers_from_api,
            kucoin_int::fetch_derivatives_tickers_from_api,
            kucoin_int::fetch_spot_tickers_from_api,
            bingx_int::fetch_derivatives_tickers_from_api,
            bingx_int::fetch_spot_tickers_from_api,
        ]);
    }
    let mut threads = vec![std::thread::spawn(move || {
        let k = "TRADE_CONTANGO_FETCH_PROCESS_ACTION";
        match std::env::var(k).unwrap().as_str() {
            "write-to-queue" => write_to_queue(rx),
            "write-to-worker" => tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(write_to_worker(rx)),
            _ => panic!("unknown k={k}"),
        }
    })];
    for f in fns {
        let tx = tx.clone();
        let t = std::thread::spawn(move || loop {
            let start_millis = now_millis();
            let tickers = backoff_call(f);
            let end_millis = now_millis();
            let t0 = &tickers[0];
            if end_millis - start_millis > 1000 {
                log::warn!("dur_millis={} > 1000", end_millis - start_millis);
            }
            let delay_millis = (1000 - (end_millis - start_millis) % 1000) as u64;
            log::info!(
                "done {:?}_tickers l={} ex={:?} dur_millis={} delay_millis={}",
                t0.k,
                tickers.len(),
                t0.ex,
                end_millis - start_millis,
                delay_millis
            );
            tx.send(tickers).unwrap();
            // XXX: remove delay
            std::thread::sleep(std::time::Duration::from_millis(delay_millis));
        });
        threads.push(t);
    }
    pool(&threads);
    TelegramBotPort::new_from_envs().notify_pretty(file!().into(), "pool-fail".into());
}

fn run_track_diff() {
    let query = "
        -- calc for every ticker max/min prices on direvatives and spot in last 60s
        INSERT INTO default.trade_contango_arbitrage_v1_diff_tracks
        WITH t AS (
            SELECT
                exchange,
                kind,
                UPPER(replaceRegexpAll(symbol, '[10*_-]??', '')) symbol_int_1,
                price / COALESCE(toFloat64OrNull(regexpExtract(symbol, '10*', 0)), 1) price_int_1
            FROM default.trade_contango_arbitrage_v1
            FINAL
            WHERE timestamp >= (now() - toIntervalSecond(60))
                AND length(replaceRegexpOne(symbol, '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) = length(symbol)
                AND volume > 0
                AND status = 'TRADING'
                AND symbol_int_1 != 'DEFIUSDT'
        )
        SELECT
            now() ts,
            t1.symbol_int_1,
            t1.price_int_1 AS fut_price,
            t2.price_int_1 AS spot_price,
            t1.exchange AS der_ex,
            t2.exchange AS spot_ex,
            round(fut_price - spot_price, 4) AS diff_abs,
            round((fut_price - spot_price) / spot_price * 100, 2) AS diff_rel
        FROM (
            SELECT *
            FROM (
                SELECT
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'futures'
            )
            WHERE _rownum = 1
        ) t1
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'spot'
            )
            WHERE _rownum = 1
        ) t2
            ON t1.symbol_int_1 = t2.symbol_int_1
        WHERE diff_rel > ?
        ORDER BY diff_rel DESC
    ";
    let threshold_rel = 3.0;
    loop {
        let mut written_rows = 0;
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let clickhouse_client =
                    clickhouse::Client::default().with_url("http://127.0.0.1:18123");
                let query_id = uuid::Uuid::new_v4().to_string();
                let _ = clickhouse_client
                    .query(query)
                    .bind(&threshold_rel)
                    .with_option("query_id", &query_id)
                    .execute()
                    .await
                    .unwrap();
                loop {
                    let finished = clickhouse_client
                        .query("SELECT count() FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'")
                        .bind(&query_id)
                        .fetch::<u64>()
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .unwrap();
                    if finished != 0 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                written_rows = clickhouse_client
                    .query("SELECT sum(written_rows) FROM system.query_log WHERE query_id = ?")
                    .bind(&query_id)
                    .fetch::<u64>()
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap();
            });
        let m = format!("tick >{threshold_rel}% written_rows={written_rows}");
        log::info!("{}", m);
        if written_rows > 0 {
            TelegramBotPort::new_from_envs().notify_pretty(file!().into(), m);
        }
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct RunArgs {
    #[arg(short, long, help = "Command to run")]
    command: String,
}

fn write_to_queue(rx: std::sync::mpsc::Receiver<Vec<QTicker>>) {
    let mut queue_tickers = vec![];
    fn produce(vec: &Vec<QTicker>) {
        log::info!("produce len={}", vec.len());
        let vec_ = vec
            .iter()
            .map(|x| serde_json::to_string(&x).unwrap())
            .collect::<Vec<_>>();
        let _ = RedpandaPort::connect_produce_messages_sync("tickers-contango-arbitrage", &vec_);
    }
    loop {
        match rx.recv_timeout(std::time::Duration::from_millis(1000)) {
            Ok(vec) => {
                for v in vec {
                    queue_tickers.push(v)
                }
            }
            Err(e) => {
                match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        if queue_tickers.len() > 0 {
                            produce(&queue_tickers);
                            queue_tickers.clear();
                        }
                    }
                    _ => log::error!("e={:?}", e.to_string()),
                }
                continue;
            }
        }
        if queue_tickers.len() == 10_000 {
            produce(&queue_tickers);
            queue_tickers.clear();
        }
    }
}

async fn write_to_worker(rx: std::sync::mpsc::Receiver<Vec<QTicker>>) {
    let mut map = SpreadsMap::new();
    let mut client = TradeContangoClient::connect("http://[::1]:50051")
        .await
        .unwrap();
    loop {
        let mut batch_vec = vec![];
        while let Ok(t) = rx.try_recv() {
            batch_vec.push(t);
        }
        if batch_vec.len() > 0 {
            if batch_vec.len() > 3 {
                panic!("write_to_worker rx batch.len > 3");
            }
            log::info!("write_to_worker batch.len={}", batch_vec.len());
            for b in batch_vec {
                let insert_start = now_millis();
                for t in b {
                    map.insert(&t);
                }
                log::info!("write_to_worker insert_dur={}", now_millis() - insert_start);
                let find_start = now_millis();
                if let Some(vec_to_send) = map.find_spreads_all() {
                    log::info!(
                        "vec_to_send.len={} find_dur={}",
                        vec_to_send.len(),
                        now_millis() - find_start
                    );
                    let _ = client
                        .fire_trade_v2(tonic::Request::new(FireTradeReqV2 { list: vec_to_send }))
                        .await
                        .unwrap();
                }
            }
        }
    }
}

fn backoff_call(
    f: impl Fn() -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>>,
) -> Vec<QTicker> {
    let n = 5;
    for i in 0..n {
        match f() {
            Ok(v) => return v,
            Err(e) => {
                log::warn!("backoff f call err={:?}", e.to_string());
                if i != n - 1 {
                    std::thread::sleep(std::time::Duration::from_secs(15));
                }
            }
        }
    }
    panic!("backoff_call fail");
}

///
/// On 2024-11-03 in websocket stream some symbols are absent => i had to do http requests in busy loop.
///
mod bybit_int {
    use crate::{QEx, QKind, QSt, QTicker};
    use exchanges_arbitrage::domain;

    #[allow(dead_code)]
    fn listen_derivatives_tickers_stream(tx: &std::sync::mpsc::Sender<QTicker>) {
        let url_obj = url::Url::parse(domain::bybit::URL_WS_V5_PUBLIC_LINEAR).unwrap();
        let (mut socket, _response) = tungstenite::connect(url_obj).unwrap();
        let subscribe_text = format!(
            "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
            "ETHUSDT",
        );
        log::info!("subscribe_text={subscribe_text}");
        socket
            .write_message(tungstenite::Message::Text(subscribe_text))
            .unwrap();
        loop {
            let msg = socket.read_message().unwrap();
            let msg_str = msg.to_text().unwrap();
            let val = serde_json::from_str::<serde_json::Value>(msg_str).unwrap();
            let topic = match val.get("type") {
                Some(v) => v.as_str().unwrap(),
                _ => {
                    log::debug!("msg_str={msg_str}");
                    ""
                }
            };
            if topic != "delta" {
                continue;
            }
            let ts = val.get("ts").unwrap().as_i64().unwrap();
            let data = val.get("data").unwrap().as_object().unwrap();
            let data_symbol = data.get("symbol").unwrap().as_str().unwrap();
            let data_bid1 = match data.get("bid1Price") {
                Some(v) => v.as_str().unwrap(),
                _ => continue,
            };
            let ticker = QTicker::new(
                QEx::Bybit,
                data_symbol,
                QSt::Trading,
                QKind::Futures,
                ts,
                data_bid1,
                "0.0",
            );
            log::debug!("derivatives-ticker={ticker:?}");
            tx.send(ticker).unwrap();
        }
    }

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.bybit.com/v5/market/tickers?category=linear";
        let res = serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?;
        let ts = res.get("time").unwrap().as_i64().unwrap();
        Ok(res
            .get("result")
            .unwrap()
            .as_object()
            .unwrap()
            .get("list")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                let s = x.get("symbol").unwrap().as_str().unwrap();
                let p_bid = x.get("bid1Price").unwrap().as_str().unwrap();
                let p_ask = x.get("ask1Price").unwrap().as_str().unwrap();
                let v = x.get("volume24h").unwrap().as_str().unwrap();
                QTicker::new_from_bid_ask(
                    QEx::Bybit,
                    s,
                    QSt::Trading,
                    QKind::Futures,
                    ts,
                    p_bid,
                    p_ask,
                    v,
                )
            })
            .collect::<Vec<_>>())
    }

    #[allow(dead_code)]
    fn listen_spot_tickers_stream(tx: &std::sync::mpsc::Sender<QTicker>) {
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
            let ticker = QTicker::new(
                QEx::Bybit,
                data_symbol,
                QSt::Trading,
                QKind::Spot,
                ts,
                data_last_price,
                "0",
            );
            log::debug!("spot-ticker={ticker:?}");
            tx.send(ticker).unwrap();
        }
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.bybit.com/v5/market/tickers?category=spot";
        let res = serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?;
        let ts = res.get("time").unwrap().as_i64().unwrap();
        Ok(res
            .get("result")
            .unwrap()
            .as_object()
            .unwrap()
            .get("list")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                let s = x.get("symbol").unwrap().as_str().unwrap();
                let p_bid = x.get("bid1Price").unwrap().as_str().unwrap();
                let p_ask = x.get("ask1Price").unwrap().as_str().unwrap();
                let v = x.get("volume24h").unwrap().as_str().unwrap();
                QTicker::new_from_bid_ask(
                    QEx::Bybit,
                    s,
                    QSt::Trading,
                    QKind::Spot,
                    ts,
                    p_bid,
                    p_ask,
                    v,
                )
            })
            .collect::<Vec<_>>())
    }

    #[allow(dead_code)]
    fn bybit_print_all_spot_symbols() {
        let url_str = "https://api.bybit.com/v5/market/instruments-info?category=spot";
        let res = reqwest::blocking::get(url_str).unwrap();
        let val = serde_json::from_str::<serde_json::Value>(&res.text().unwrap()).unwrap();
        let symbols = val
            .get("result")
            .unwrap()
            .as_object()
            .unwrap()
            .get("list")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                let symbol = x.get("symbol").unwrap().as_str().unwrap().to_string();
                let base_coin = x.get("baseCoin").unwrap().as_str().unwrap();
                let quote_coin = x.get("quoteCoin").unwrap().as_str().unwrap();
                (symbol, base_coin, quote_coin)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|x| (*x).2 == "USDT")
            .collect::<Vec<_>>();
        log::info!("symbols={:?}", symbols);
    }
}

mod binance_int {
    use crate::{now_millis, QEx, QKind, QSt, QTicker};

    static PATH_EXCHANGE_INFO: &str = "/tmp/binance_fapi_v1_exchange_info.json";

    pub fn fetch_write_config() {
        let path = std::path::Path::new(PATH_EXCHANGE_INFO);
        fn fetch_write(p: &std::path::Path) {
            let url = "https://fapi.binance.com/fapi/v1/exchangeInfo";
            let res = reqwest::blocking::get(url).unwrap();
            std::fs::write(p, res.text().unwrap()).unwrap();
        }
        if path.exists() {
            let metadata = std::fs::metadata(path).unwrap();
            let ts = metadata.modified().unwrap();
            let updated_dur_secs = std::time::SystemTime::now()
                .duration_since(ts)
                .unwrap()
                .as_secs();
            if updated_dur_secs > (60 * 60) {
                log::info!("file expired -> download and write");
                fetch_write(&path);
            }
        } else {
            log::info!("binance-config file not exists -> download and write");
            fetch_write(&path);
        }
    }

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let symbol_status = read_symbol_status_config();
        let url = "https://fapi.binance.com/fapi/v1/ticker/24hr";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let st = match symbol_status.get(s) {
                        Some(v) => v.clone(),
                        _ => QSt::NotFound,
                    };
                    let p = x.get("lastPrice").unwrap().as_str().unwrap();
                    let v = x.get("volume").unwrap().as_str().unwrap();
                    QTicker::new(QEx::Binance, s, st, QKind::Futures, ts, p, v)
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let symbol_status = read_symbol_status_config();
        let url = "https://api.binance.com/api/v3/ticker/24hr";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let st = match symbol_status.get(s) {
                        Some(v) => v.clone(),
                        _ => QSt::NotFound,
                    };
                    let p = x.get("lastPrice").unwrap().as_str().unwrap();
                    let v = x.get("volume").unwrap().as_str().unwrap();
                    QTicker::new(QEx::Binance, s, st, QKind::Spot, ts, p, v)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn read_symbol_status_config() -> std::collections::HashMap<String, QSt> {
        serde_json::from_str::<serde_json::Value>(
            &std::fs::read_to_string(std::path::Path::new(PATH_EXCHANGE_INFO)).unwrap(),
        )
        .unwrap()
        .get("symbols")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let status_raw = x.get("status").unwrap().as_str().unwrap();
            let status = match status_raw {
                "SETTLING" => QSt::Settling,
                "PENDING_TRADING" => QSt::PendingTrading,
                "TRADING" => QSt::Trading,
                _ => panic!("unknown status_raw={}", status_raw),
            };
            (symbol.into(), status.into())
        })
        .collect::<std::collections::HashMap<_, _>>()
    }
}

mod mexc_int {
    use crate::{now_millis, QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://contract.mexc.com/api/v1/contract/ticker";
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let p = match x.get("bid1") {
                        Some(v) => v.as_f64().unwrap().to_string(),
                        _ => "-1.0".into(),
                    };
                    let ts = x.get("timestamp").unwrap().as_i64().unwrap();
                    let v = x.get("volume24").unwrap().as_f64().unwrap().to_string();
                    QTicker::new(QEx::Mexc, s, QSt::Trading, QKind::Futures, ts, &p, &v)
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.mexc.com/api/v3/ticker/24hr";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let p_bid = x.get("bidPrice").unwrap().as_str().unwrap();
                    let p_ask = x.get("askPrice").unwrap().as_str().unwrap();
                    let v = x.get("volume").unwrap().as_str().unwrap();
                    QTicker::new_from_bid_ask(
                        QEx::Mexc,
                        s,
                        QSt::Trading,
                        QKind::Spot,
                        ts,
                        p_bid,
                        p_ask,
                        v,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }
}

mod kucoin_int {
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api-futures.kucoin.com/api/v1/allTickers";
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let ts = x.get("ts").unwrap().as_u64().unwrap();
                    let ts_str = (ts / 1_000_000) as i64;
                    let p = x.get("price").unwrap().as_str().unwrap();
                    // NOTE: that's not a volume
                    let v = x.get("size").unwrap().as_i64().unwrap().to_string();
                    QTicker::new(QEx::Kucoin, s, QSt::Trading, QKind::Futures, ts_str, p, &v)
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.kucoin.com/api/v1/market/allTickers";
        let res = serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?;
        let res_data = res.get("data").unwrap();
        let ts = res_data.get("time").unwrap().as_i64().unwrap();
        Ok(res_data
            .get("ticker")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                let s = x.get("symbol").unwrap().as_str().unwrap();
                let p = x.get("high").unwrap().as_str().unwrap();
                let v = x.get("volValue").unwrap().as_str().unwrap();
                QTicker::new(QEx::Kucoin, s, QSt::Trading, QKind::Spot, ts, p, v)
            })
            .collect::<Vec<_>>())
    }
}

mod gateio_int {
    use crate::{now_millis, QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://fx-api.gateio.ws/api/v4/futures/usdt/tickers";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("contract").unwrap().as_str().unwrap();
                    let p_bid = x.get("highest_bid").unwrap().as_str().unwrap();
                    let p_ask = x.get("lowest_ask").unwrap().as_str().unwrap();
                    let v = x.get("volume_24h").unwrap().as_str().unwrap();
                    QTicker::new_from_bid_ask(
                        QEx::Gateio,
                        s,
                        QSt::Trading,
                        QKind::Futures,
                        ts,
                        p_bid,
                        p_ask,
                        v,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.gateio.ws/api/v4/spot/tickers";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("currency_pair").unwrap().as_str().unwrap();
                    let p_bid_raw = x.get("highest_bid").unwrap().as_str().unwrap();
                    let p_bid = if p_bid_raw == "" { "-1.0" } else { p_bid_raw };
                    let p_ask_raw = x.get("lowest_ask").unwrap().as_str().unwrap();
                    let p_ask = if p_ask_raw == "" { "-1.0" } else { p_ask_raw };
                    // XXX: collect base_volume and quote_volume
                    let v = x.get("base_volume").unwrap().as_str().unwrap();
                    QTicker::new_from_bid_ask(
                        QEx::Gateio,
                        s,
                        QSt::Trading,
                        QKind::Spot,
                        ts,
                        p_bid,
                        p_ask,
                        v,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }
}

mod bingx_int {
    use crate::{now_millis, QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let p = x.get("bidPrice").unwrap().as_str().unwrap();
                    let v = x.get("volume").unwrap().as_str().unwrap();
                    QTicker::new(QEx::Bingx, s, QSt::Trading, QKind::Futures, ts, p, v)
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://open-api.bingx.com/openApi/spot/v1/ticker/price";
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let trade = x
                        .get("trades")
                        .unwrap()
                        .as_array()
                        .unwrap()
                        .first()
                        .unwrap();
                    let ts = trade.get("timestamp").unwrap().as_i64().unwrap();
                    let p = trade.get("price").unwrap().as_str().unwrap();
                    let v = trade.get("volume").unwrap().as_str().unwrap();
                    QTicker::new(QEx::Bingx, s, QSt::Trading, QKind::Spot, ts, p, v)
                })
                .collect::<Vec<_>>(),
        )
    }
}

mod htx_int {
    use crate::{now_millis, QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.hbdm.com/v2/linear-swap-ex/market/detail/batch_merged";
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("ticks")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("contract_code").unwrap().as_str().unwrap();
                    let ts = x.get("ts").unwrap().as_i64().unwrap();
                    let bid_raw = x.get("bid").unwrap();
                    let p = if bid_raw.is_null() {
                        x.get("close").unwrap().as_str().unwrap().to_string()
                    } else {
                        bid_raw
                            .as_array()
                            .unwrap()
                            .first()
                            .unwrap()
                            .as_f64()
                            .unwrap()
                            .to_string()
                    };
                    let v = x.get("vol").unwrap().as_str().unwrap();
                    QTicker::new(QEx::Htx, s, QSt::Trading, QKind::Futures, ts, &p, v)
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn fetch_spot_tickers_from_api(
    ) -> std::result::Result<Vec<QTicker>, Box<dyn std::error::Error>> {
        let url = "https://api.huobi.pro/market/tickers";
        let ts = now_millis();
        Ok(
            serde_json::from_str::<serde_json::Value>(&reqwest::blocking::get(url)?.text()?)?
                .get("data")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| {
                    let s = x.get("symbol").unwrap().as_str().unwrap();
                    let p = x.get("bid").unwrap().as_f64().unwrap().to_string();
                    let v = x.get("vol").unwrap().as_f64().unwrap().to_string();
                    QTicker::new(QEx::Htx, s, QSt::Trading, QKind::Spot, ts, &p, &v)
                })
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, serde::Serialize, Clone)]
struct QTicker {
    ex: QEx,
    s: String,
    st: QSt,
    k: QKind,
    ts: i64,
    p: f64,
    p_bid: f64,
    p_ask: f64,
    v: f64,
}

impl QTicker {
    fn new(_ex: QEx, _s: &str, _st: QSt, _k: QKind, _ts: i64, _p: &str, _v: &str) -> Self {
        unimplemented!("use new_from_bid_ask")
    }

    fn new_from_bid_ask(
        ex: QEx,
        s: &str,
        st: QSt,
        k: QKind,
        ts: i64,
        p_bid: &str,
        p_ask: &str,
        v: &str,
    ) -> Self {
        if !Self::is_millis(ts) {
            panic!("ts={} is not in millis", ts);
        }
        Self {
            ex: ex.into(),
            s: s.into(),
            st,
            k: k.into(),
            ts,
            p: -1.0,
            p_bid: Self::parse_p(p_bid),
            p_ask: Self::parse_p(p_ask),
            v: v.parse::<f64>().unwrap(),
        }
    }

    fn is_millis(ts: i64) -> bool {
        let threshold = 365 * 24 * 60 * 60 * 1000;
        ts >= threshold
    }

    #[allow(dead_code)]
    fn conv_to_ticker_req(&self) -> QTickerReq {
        unimplemented!("use conv_to_ticker_req");
    }

    fn conv_to_ticker_req_v2(&self) -> QTickerReqV2 {
        QTickerReqV2 {
            ex: self.ex.to_string().to_lowercase(),
            s: self.s.clone(),
            st: self.st.to_string().to_lowercase(),
            k: self.k.to_string().to_lowercase(),
            ts: self.ts,
            p_bid: self.p_bid,
            p_ask: self.p_ask,
            v: self.v,
        }
    }

    fn parse_p(p: &str) -> f64 {
        match p.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                log::warn!("p=\"{}\" e={}", p, e);
                0.0
            }
        }
    }
}

#[derive(Debug, serde::Serialize, Hash, Eq, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
enum QKind {
    Spot,
    Futures,
}

impl std::fmt::Display for QKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, serde::Serialize, Hash, Eq, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
enum QEx {
    Bybit,
    Binance,
    Mexc,
    Kucoin,
    Gateio,
    Bingx,
    Htx,
}

impl std::fmt::Display for QEx {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Clone, serde::Serialize, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum QSt {
    Settling,
    PendingTrading,
    Trading,
    NotFound,
}

impl std::fmt::Display for QSt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, serde::Serialize)]
struct SpreadsMap<'a> {
    symbols_to_ignore: HashSet<(&'a str, &'a QEx, &'a QKind)>,
    symbols_to_re_map: HashMap<(&'a str, &'a QEx, &'a QKind), &'a str>,
    symbols: HashMap<String, HashMap<QEx, HashMap<QKind, QTicker>>>,
    diff_ref_bottom: f64,
    diff_ref_top: f64,
}

impl<'a> SpreadsMap<'a> {
    fn new() -> Self {
        let mut symbols_to_ignore = HashSet::new();
        // XXX: make symbols map (symbol x names on exchange 1 like y1, on exchange 2 like y2, etc) and remove filter diff_rel < 15
        symbols_to_ignore.insert(("ZECUSDT", &QEx::Bybit, &QKind::Spot));
        symbols_to_ignore.insert(("FBUSDT", &QEx::Bybit, &QKind::Spot));
        symbols_to_ignore.insert(("MEUSDT", &QEx::Bybit, &QKind::Spot));
        symbols_to_ignore.insert(("DEFIUSDT", &QEx::Binance, &QKind::Futures));
        symbols_to_ignore.insert(("OMNI_USDT", &QEx::Bingx, &QKind::Spot));
        symbols_to_ignore.insert(("GFT_USDT", &QEx::Bingx, &QKind::Spot));
        symbols_to_ignore.insert(("QIUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("ALTUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("GASUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("OAXUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("MAGAUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("CATEUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("MDTUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("ASTUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("GPTUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("SOLSUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("WOLFUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("REEFUSDT", &QEx::Mexc, &QKind::Spot));
        symbols_to_ignore.insert(("ME_USDT", &QEx::Gateio, &QKind::Spot));
        let mut symbols_to_re_map = HashMap::new();
        symbols_to_re_map.insert(("OMNINETWORK-USDT", &QEx::Bingx, &QKind::Spot), "OMNI-USDT");
        symbols_to_re_map.insert(("OMNINETWORK_USDT", &QEx::Bingx, &QKind::Spot), "OMNI_USDT");
        Self {
            symbols_to_ignore,
            symbols_to_re_map,
            symbols: HashMap::new(),
            diff_ref_bottom: std::env::var("DIFF_REL_BOTTOM_THRESHOLD")
                .unwrap_or("0.0".into())
                .parse::<f64>()
                .unwrap(),
            diff_ref_top: std::env::var("DIFF_REL_TOP_THRESHOLD")
                .unwrap_or("100.0".into())
                .parse::<f64>()
                .unwrap(),
        }
    }

    fn insert(&mut self, t: &QTicker) {
        // XXX: workout USDC
        if t.st != QSt::Trading
            || t.p_bid == 0.0
            || t.p_ask == 0.0
            || t.v == 0.0
            || !t.s.ends_with("USDT")
        {
            return;
        }
        let key_to_ignore = (t.s.as_str(), &t.ex, &t.k);
        if self.symbols_to_ignore.contains(&key_to_ignore) {
            log::debug!("ignoring {:?}", key_to_ignore);
            return;
        }
        let (s_int_1, _) = Self::conv_to_symbol_int_1_v2(
            *self
                .symbols_to_re_map
                .get(&key_to_ignore)
                .unwrap_or(&t.s.as_str()),
            0.0,
        );
        let ex = {
            if !self.symbols.contains_key(&s_int_1) {
                self.symbols.insert(s_int_1.clone(), HashMap::new());
            }
            self.symbols.get_mut(&s_int_1).unwrap()
        };
        let kind = {
            if !ex.contains_key(&t.ex) {
                ex.insert(t.ex, HashMap::new());
            }
            ex.get_mut(&t.ex).unwrap()
        };
        kind.insert(t.k, t.clone());
    }

    fn find_spreads(self: &Self, s: &str) -> Option<(QTicker, QTicker)> {
        if !self.symbols.contains_key(s) {
            return None;
        }
        let mut min_spot: Option<QTicker> = None;
        let mut max_fut: Option<QTicker> = None;
        let m1_millis = now_millis() - 60 * 1000;
        for (_, kind_map) in self.symbols.get(s).unwrap() {
            for (kind, t) in kind_map {
                if t.ts < m1_millis {
                    continue;
                }
                match kind {
                    QKind::Futures => match &max_fut {
                        Some(v) => {
                            if v.p_bid < t.p_bid {
                                max_fut = Some(t.clone());
                            }
                        }
                        None => max_fut = Some(t.clone()),
                    },
                    QKind::Spot => match &min_spot {
                        Some(v) => {
                            if v.p_ask > t.p_ask {
                                min_spot = Some(t.clone());
                            }
                        }
                        None => min_spot = Some(t.clone()),
                    },
                }
            }
        }
        if min_spot.is_none() || max_fut.is_none() {
            return None;
        }
        Some((min_spot.unwrap(), max_fut.unwrap()))
    }

    fn find_spreads_all(self: &Self) -> Option<Vec<SpreadsReqV2>> {
        let mut spreads = vec![];
        for (k, _) in self.symbols.iter() {
            match self.find_spreads(k) {
                Some(s) => {
                    let (_, p_ask_spot) = SpreadsMap::conv_to_symbol_int_1_v2(&s.0.s, s.0.p_ask);
                    let (_, p_bid_fut) = SpreadsMap::conv_to_symbol_int_1_v2(&s.1.s, s.1.p_bid);
                    let diff_rel = (p_bid_fut - p_ask_spot) / p_ask_spot * 100.0;
                    if diff_rel >= self.diff_ref_bottom && diff_rel <= self.diff_ref_top {
                        log::debug!("k={} diff_rel={}", k, diff_rel);
                        spreads.push(SpreadsReqV2 {
                            p_ask_spot,
                            p_bid_fut,
                            diff_rel,
                            t_spot: Some(s.0.conv_to_ticker_req_v2()),
                            t_fut: Some(s.1.conv_to_ticker_req_v2()),
                        });
                    } else {
                        log::debug!("k={} tiny spread", k);
                    }
                }
                None => {}
            }
        }
        if spreads.is_empty() {
            None
        } else {
            Some(spreads)
        }
    }

    fn conv_to_symbol_int_1_v2(s: &str, p: f64) -> (String, f64) {
        let s_int = s.replace('-', "").replace('_', "");
        let chars = s_int.chars().collect::<Vec<_>>();
        if chars[0] != '1' {
            return (s_int, p);
        }
        let mut zeros_amount = 0;
        for i in 1..chars.len() {
            if chars[i] == '0' {
                zeros_amount += 1;
            } else {
                break;
            }
        }
        if zeros_amount == 0 {
            return (s_int, p);
        }
        return (
            s_int.split_at(zeros_amount + 1).1.to_string(),
            p / 10.0_f64.powi(zeros_amount as i32),
        );
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod test {
    use crate::{now_millis, QEx, QKind, QSt, QTicker, SpreadsMap};

    #[test]
    fn test_validate_millis() {
        use crate::QTicker;
        let t = 365 * 24 * 60 * 60 * 1000;
        assert_eq!(QTicker::is_millis(t - 1), false);
        assert_eq!(QTicker::is_millis(t), true);
        assert_eq!(QTicker::is_millis(t + 1), true);
    }

    #[test]
    fn test_enums_serialization() {
        use crate::{QEx, QKind};
        assert_eq!(serde_json::to_string(&QEx::Bybit).unwrap(), "\"bybit\"");
        assert_eq!(serde_json::to_string(&QKind::Spot).unwrap(), "\"spot\"");
        assert_eq!(
            serde_json::to_string(&QKind::Futures).unwrap(),
            "\"futures\""
        );
        assert_eq!(QEx::Bybit.to_string(), "Bybit");
    }

    #[test]
    fn test_f64_parse() {
        assert_eq!("0.0".parse::<f64>().unwrap(), 0.0);
        assert_eq!("0".parse::<f64>().unwrap(), 0.0);
    }

    fn gen_tickers_for_price_spread(ts_millis: i64) -> Vec<QTicker> {
        vec![
            QTicker::new_from_bid_ask(
                QEx::Mexc,
                "BTCUSDT",
                QSt::Trading,
                QKind::Spot,
                ts_millis,
                "60000.0",
                "60000.0",
                "1.0",
            ),
            QTicker::new_from_bid_ask(
                QEx::Mexc,
                "BTCUSDT",
                QSt::Trading,
                QKind::Spot,
                ts_millis,
                "61000.0",
                "61000.0",
                "1.0",
            ),
            QTicker::new_from_bid_ask(
                QEx::Mexc,
                "BTCUSDT",
                QSt::Trading,
                QKind::Futures,
                ts_millis,
                "62000.0",
                "62000.0",
                "1.0",
            ),
            QTicker::new_from_bid_ask(
                QEx::Mexc,
                "BTCUSDT",
                QSt::Trading,
                QKind::Futures,
                ts_millis,
                "63000.0",
                "63000.0",
                "1.0",
            ),
        ]
    }

    #[test]
    fn test_trigger_price_spread() {
        {
            let mut map = crate::SpreadsMap::new();
            map.insert(&QTicker::new_from_bid_ask(
                QEx::Mexc,
                "BTCUSDT",
                QSt::Trading,
                QKind::Spot,
                now_millis(),
                "65000.0",
                "65000.0",
                "1.0",
            ));
            for t in gen_tickers_for_price_spread(now_millis()) {
                map.insert(&t);
            }
            let out = map.find_spreads("BTCUSDT").unwrap();
            assert_eq!(out.0.p_ask, 61000.0);
            assert_eq!(out.1.p_bid, 63000.0);
        }
        {
            let mut map = crate::SpreadsMap::new();
            for t in gen_tickers_for_price_spread(now_millis() - 2 * 60 * 1000) {
                map.insert(&t);
            }
            let out = map.find_spreads("BTCUSDT");
            assert!(out.is_none());
        }
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("1000000PEPEUSDT", 100.0),
            ("PEPEUSDT".into(), 0.00010)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("10PEPEUSDT", 100.0),
            ("PEPEUSDT".into(), 10.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("1PEPEUSDT", 100.0),
            ("1PEPEUSDT".into(), 100.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("PEPEUSDT", 100.0),
            ("PEPEUSDT".into(), 100.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("PEPE-USDT", 1.0),
            ("PEPEUSDT".into(), 1.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("PEPE_USDT", 1.0),
            ("PEPEUSDT".into(), 1.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("PEPE1-USDT", 1.0),
            ("PEPE1USDT".into(), 1.0)
        );
        assert_eq!(
            SpreadsMap::conv_to_symbol_int_1_v2("0DOGUSDT", 1.0),
            ("0DOGUSDT".into(), 1.0)
        );
    }

    #[test]
    fn test_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(("BTCUSDT", "bybit"));
        set.insert(("BTCUSDT", "binance"));
        assert!(set.contains(&("BTCUSDT", "bybit")));
    }
}
