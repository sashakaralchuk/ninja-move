use exchanges_arbitrage::{pool, RedpandaPort};

const _: &str = r#"
-- redpanda
rpk topic create -c retention.ms=900000 -c segment.ms=900000 -c segment.bytes=67108864 -c retention.bytes=67108864 tickers-contango-arbitrage
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
-- TODO: find out toYYYYMMDDhh, p.s. toYYYYMMDDhhmmss is exists
-- XXX: squash duplicates based on ticker_id
PARTITION BY toYYYYMMDD(timestamp)
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
-- calc for every ticker max/min prices on direvatives and spot in last 60s
WITH t AS (
    SELECT
        *,
        UPPER(replaceRegexpAll(symbol, '[_-]?', '')) symbol_int_1
    FROM default.trade_contango_arbitrage_v1
    FINAL
    WHERE timestamp >= (now() - toIntervalSecond(60))
        AND length(replaceRegexpOne(symbol, '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) = length(symbol)
        AND length(replaceRegexpOne(symbol, '10*', '')) = length(symbol)
        AND volume > 0
        AND status = 'TRADING'
)
SELECT
    t1.symbol_int_1,
    t1.price AS fut_price,
    t2.price AS spot_price,
    t1.exchange AS der_ex,
    t2.exchange AS spot_ex,
    round(fut_price - spot_price, 4) AS diff_abs,
    round((fut_price - spot_price) / spot_price * 100, 2) AS diff_rel
FROM (
    SELECT *
    FROM (
        SELECT
            symbol_int_1,
            price,
            exchange,
            ROW_NUMBER() OVER(
                PARTITION BY symbol_int_1, kind
                ORDER BY price DESC
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
            price,
            exchange,
            ROW_NUMBER() OVER(
                PARTITION BY symbol_int_1, kind
                ORDER BY price DESC
            ) _rownum
        FROM t
        WHERE kind = 'spot'
    )
    WHERE _rownum = 1
) t2
    ON t1.symbol_int_1 = t2.symbol_int_1
ORDER BY diff_rel DESC
LIMIT 10
"#;

fn main() {
    // -- TODO: for 10* figure out price for 1 token
    // -- TODO: debug price which i see here and price in UI
    // -- TODO: implement error print on error
    // -- TODO: implement re-try in requests
    env_logger::init();
    binance_int::fetch_write_config();
    let (tx, rx) = std::sync::mpsc::channel();
    let fns = vec![
        bybit_int::fetch_derivatives_tickers_from_api,
        bybit_int::fetch_spot_tickers_from_api,
        binance_int::fetch_derivatives_tickers_from_api,
        binance_int::fetch_spot_tickers_from_api,
        mexc_int::fetch_derivatives_tickers_from_api,
        mexc_int::fetch_spot_tickers_from_api,
        kucoin_int::fetch_derivatives_tickers_from_api,
        kucoin_int::fetch_spot_tickers_from_api,
        gateio_int::fetch_derivatives_tickers_from_api,
        gateio_int::fetch_spot_tickers_from_api,
        bingx_int::fetch_derivatives_tickers_from_api,
        bingx_int::fetch_spot_tickers_from_api,
        htx_int::fetch_derivatives_tickers_from_api,
        htx_int::fetch_spot_tickers_from_api,
    ];
    let mut threads = vec![std::thread::spawn(move || {
        write_to_queue(rx);
    })];
    for f in fns {
        let tx = tx.clone();
        let t = std::thread::spawn(move || loop {
            let tickers = f();
            let t0 = &tickers[0];
            log::info!("done {:?}_tickers l={} ex={:?}", t0.k, tickers.len(), t0.ex);
            for ticker in tickers {
                tx.send(ticker).unwrap();
            }
            std::thread::sleep(std::time::Duration::from_secs(15));
        });
        threads.push(t);
    }
    pool(&threads);
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

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.bybit.com/v5/market/tickers?category=linear";
        let res = serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap();
        let ts = res.get("time").unwrap().as_i64().unwrap();
        res.get("result")
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
                let p = x.get("bid1Price").unwrap().as_str().unwrap();
                let v = x.get("volume24h").unwrap().as_str().unwrap();
                QTicker::new(QEx::Bybit, s, QSt::Trading, QKind::Futures, ts, p, v)
            })
            .collect::<Vec<_>>()
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

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.bybit.com/v5/market/tickers?category=spot";
        let res = serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap();
        let ts = res.get("time").unwrap().as_i64().unwrap();
        res.get("result")
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
                let p = x.get("ask1Price").unwrap().as_str().unwrap();
                let v = x.get("volume24h").unwrap().as_str().unwrap();
                QTicker::new(QEx::Bybit, s, QSt::Trading, QKind::Spot, ts, p, v)
            })
            .collect::<Vec<_>>()
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
    use crate::{QEx, QKind, QSt, QTicker};

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

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let symbol_status = read_symbol_status_confing();
        let url = "https://fapi.binance.com/fapi/v1/ticker/24hr";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("symbol").unwrap().as_str().unwrap();
            let st = symbol_status.get(s).unwrap().clone();
            let p = x.get("lastPrice").unwrap().as_str().unwrap();
            let v = x.get("volume").unwrap().as_str().unwrap();
            QTicker::new(QEx::Binance, s, st, QKind::Futures, ts, p, v)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let symbol_status = read_symbol_status_confing();
        let url = "https://api.binance.com/api/v3/ticker/24hr";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
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
        .collect::<Vec<_>>()
    }

    fn read_symbol_status_confing() -> std::collections::HashMap<String, QSt> {
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
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://contract.mexc.com/api/v1/contract/ticker";
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .get("data")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("symbol").unwrap().as_str().unwrap();
            let p = match x.get("ask1") {
                Some(v) => v.as_f64().unwrap().to_string(),
                _ => "-1.0".into(),
            };
            let ts = x.get("timestamp").unwrap().as_i64().unwrap();
            let v = x.get("volume24").unwrap().as_f64().unwrap().to_string();
            QTicker::new(QEx::Mexc, s, QSt::Trading, QKind::Futures, ts, &p, &v)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.mexc.com/api/v3/ticker/24hr";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("symbol").unwrap().as_str().unwrap();
            let p = x.get("lastPrice").unwrap().as_str().unwrap();
            let v = x.get("volume").unwrap().as_str().unwrap();
            QTicker::new(QEx::Mexc, s, QSt::Trading, QKind::Spot, ts, p, v)
        })
        .collect::<Vec<_>>()
    }
}

mod kucoin_int {
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api-futures.kucoin.com/api/v1/allTickers";
        let res = reqwest::blocking::get(url).unwrap();
        serde_json::from_str::<serde_json::Value>(&res.text().unwrap())
            .unwrap()
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
            .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.kucoin.com/api/v1/market/allTickers";
        let res = reqwest::blocking::get(url).unwrap();
        let res = serde_json::from_str::<serde_json::Value>(&res.text().unwrap()).unwrap();
        let res_data = res.get("data").unwrap();
        let ts = res_data.get("time").unwrap().as_i64().unwrap();
        res_data
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
            .collect::<Vec<_>>()
    }
}

mod gateio_int {
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://fx-api.gateio.ws/api/v4/futures/usdt/tickers";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("contract").unwrap().as_str().unwrap();
            let p = x.get("highest_bid").unwrap().as_str().unwrap();
            let v = x.get("volume_24h").unwrap().as_str().unwrap();
            QTicker::new(QEx::Gateio, s, QSt::Trading, QKind::Futures, ts, p, v)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.gateio.ws/api/v4/spot/tickers";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("currency_pair").unwrap().as_str().unwrap();
            let p_raw = x.get("lowest_ask").unwrap().as_str().unwrap();
            let p = if p_raw == "" { "-1.0" } else { p_raw };
            // XXX: collect base_volume and quote_volume
            let v = x.get("base_volume").unwrap().as_str().unwrap();
            QTicker::new(QEx::Gateio, s, QSt::Trading, QKind::Spot, ts, p, v)
        })
        .collect::<Vec<_>>()
    }
}

mod bingx_int {
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
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
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://open-api.bingx.com/openApi/spot/v1/ticker/price";
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
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
        .collect::<Vec<_>>()
    }
}

mod htx_int {
    use crate::{QEx, QKind, QSt, QTicker};

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.hbdm.com/v2/linear-swap-ex/market/detail/batch_merged";
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
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
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QTicker> {
        let url = "https://api.huobi.pro/market/tickers";
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .get("data")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let s = x.get("symbol").unwrap().as_str().unwrap();
            let p = x.get("ask").unwrap().as_f64().unwrap().to_string();
            let v = x.get("vol").unwrap().as_f64().unwrap().to_string();
            QTicker::new(QEx::Htx, s, QSt::Trading, QKind::Spot, ts, &p, &v)
        })
        .collect::<Vec<_>>()
    }
}

fn write_to_queue(rx: std::sync::mpsc::Receiver<QTicker>) {
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
            Ok(v) => queue_tickers.push(v),
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

#[derive(Debug, serde::Serialize)]
struct QTicker {
    ex: QEx,
    s: String,
    st: QSt,
    k: QKind,
    ts: i64,
    p: f64,
    v: f64,
}

impl QTicker {
    fn new(ex: QEx, s: &str, st: QSt, k: QKind, ts: i64, p: &str, v: &str) -> Self {
        if !Self::is_millis(ts) {
            panic!("ts={} is not in millis", ts);
        }
        Self {
            ex: ex.into(),
            s: s.into(),
            st,
            k: k.into(),
            ts,
            p: p.parse::<f64>().unwrap(),
            v: v.parse::<f64>().unwrap(),
        }
    }

    fn is_millis(ts: i64) -> bool {
        let threshold = 365 * 24 * 60 * 60 * 1000;
        ts >= threshold
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum QKind {
    Spot,
    Futures,
}

#[derive(Debug, serde::Serialize)]
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

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum QSt {
    Settling,
    PendingTrading,
    Trading,
    NotFound,
}

#[cfg(test)]
mod test {
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
    }

    #[test]
    fn test_f64_parse() {
        assert_eq!("0.0".parse::<f64>().unwrap(), 0.0);
        assert_eq!("0".parse::<f64>().unwrap(), 0.0);
    }
}
