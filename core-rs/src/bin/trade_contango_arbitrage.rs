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
    price Float64
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
    JSONExtractFloat(data, 'p') price
FROM default.trade_contango_arbitrage_v1_queue;
-- last price on spot vs last price on derivatives in last 60 seconds for contango
-- XXX: unify symbols
-- XXX: calc how old every ticker is
WITH t AS (
    SELECT
        exchange,
        symbol,
        kind,
        price,
        timestamp,
        replaceRegexpOne(symbol, '(10*)', '') symbol_int_1,
        ROW_NUMBER() OVER(
            PARTITION BY exchange, symbol_int_1, kind
            ORDER BY timestamp DESC
        ) _rownum
    FROM default.trade_contango_arbitrage_v1
    FINAL
    WHERE timestamp >= (now() - toIntervalSecond(60))
        AND length(replaceRegexpOne(symbol, '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) = length(symbol)
)
SELECT
    t1.symbol_int_1,
    floor(t2.price - t1.price, 2) der_spot_abs,
    floor(der_spot_abs / t1.price * 100, 2) der_spot_rel,
    [t1.symbol, t2.symbol] symbols,
    [t1.exchange, t2.exchange] exchanges,
    [t1.price, t2.price] prices,
    [t1.kind, t2.kind] kinds
FROM (SELECT * FROM t WHERE _rownum = 1 AND kind = 'spot') t1
INNER JOIN (SELECT * FROM t WHERE _rownum = 1 AND kind = 'derivatives') t2
    ON t1.symbol_int_1 = t2.symbol_int_1
ORDER BY der_spot_rel
LIMIT 25
"#;

fn main() {
    env_logger::init();
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
            log::info!("done {}_tickers len={} ex={}", t0.k, tickers.len(), t0.ex);
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
    use crate::QueueTicker;
    use exchanges_arbitrage::domain;

    #[allow(dead_code)]
    fn listen_derivatives_tickers_stream(tx: &std::sync::mpsc::Sender<QueueTicker>) {
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
            let data_bid1_price = match data.get("bid1Price") {
                Some(v) => v.as_str().unwrap(),
                _ => continue,
            };
            let ticker = QueueTicker::new("bybit", data_symbol, "derivatives", ts, data_bid1_price);
            log::debug!("derivatives-ticker={ticker:?}");
            tx.send(ticker).unwrap();
        }
    }

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
        let res = domain::bybit::fetch_derivatives_tickers();
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
                let symbol = x.get("symbol").unwrap().as_str().unwrap();
                let bid_1_price = x.get("bid1Price").unwrap().as_str().unwrap();
                QueueTicker::new("bybit", symbol, "derivatives", ts, bid_1_price)
            })
            .collect::<Vec<_>>()
    }

    #[allow(dead_code)]
    fn listen_spot_tickers_stream(tx: &std::sync::mpsc::Sender<QueueTicker>) {
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
            log::debug!("spot-ticker={ticker:?}");
            tx.send(ticker).unwrap();
        }
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
        let res = domain::bybit::fetch_spot_tickers();
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
                let symbol = x.get("symbol").unwrap().as_str().unwrap();
                let ask_1_price = x.get("ask1Price").unwrap().as_str().unwrap();
                QueueTicker::new("bybit", symbol, "spot", ts, ask_1_price)
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
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
        let url = "https://fapi.binance.com/fapi/v1/ticker/price";
        serde_json::from_str::<serde_json::Value>(
            &reqwest::blocking::get(url).unwrap().text().unwrap(),
        )
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let price = x.get("price").unwrap().as_str().unwrap();
            let ts = x.get("time").unwrap().as_i64().unwrap();
            QueueTicker::new("binance", symbol, "derivatives", ts, price)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
        let url = "https://api.binance.com/api/v3/ticker/price";
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let price = x.get("price").unwrap().as_str().unwrap();
            QueueTicker::new("binance", symbol, "spot", ts, price)
        })
        .collect::<Vec<_>>()
    }
}

mod mexc_int {
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let ask1 = match x.get("ask1") {
                Some(v) => v.as_f64().unwrap().to_string(),
                _ => "-1.0".into(),
            };
            let timestamp = x.get("timestamp").unwrap().as_i64().unwrap();
            QueueTicker::new("mexc", symbol, "derivatives", timestamp, &ask1)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
        let url = "https://api.mexc.com/api/v3/ticker/price";
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let price = x.get("price").unwrap().as_str().unwrap();
            QueueTicker::new("mexc", symbol, "spot", ts, price)
        })
        .collect::<Vec<_>>()
    }
}

mod kucoin_int {
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
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
                let symbol = x.get("symbol").unwrap().as_str().unwrap();
                let ts = x.get("ts").unwrap().as_u64().unwrap();
                let ts_str = (ts / 1_000_000) as i64;
                let price = x.get("price").unwrap().as_str().unwrap();
                QueueTicker::new("kucoin", symbol, "derivatives", ts_str, price)
            })
            .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
        let url = "https://api.kucoin.com/api/v1/market/allTickers";
        let res = reqwest::blocking::get(url).unwrap();
        let res = serde_json::from_str::<serde_json::Value>(&res.text().unwrap()).unwrap();
        let res_data = res.get("data").unwrap();
        let time = res_data.get("time").unwrap().as_i64().unwrap();
        res_data
            .get("ticker")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                let symbol = x.get("symbol").unwrap().as_str().unwrap();
                let price = x.get("high").unwrap().as_str().unwrap();
                QueueTicker::new("kucoin", symbol, "spot", time, price)
            })
            .collect::<Vec<_>>()
    }
}

mod gateio_int {
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("contract").unwrap().as_str().unwrap();
            let highest_bid = x.get("highest_bid").unwrap().as_str().unwrap();
            QueueTicker::new("gateio", symbol, "derivatives", ts, highest_bid)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("currency_pair").unwrap().as_str().unwrap();
            let price_raw = x.get("lowest_ask").unwrap().as_str().unwrap();
            let price = if price_raw == "" { "-1.0" } else { price_raw };
            QueueTicker::new("gateio", symbol, "spot", ts, price)
        })
        .collect::<Vec<_>>()
    }
}

mod bingx_int {
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let price = x.get("bidPrice").unwrap().as_str().unwrap();
            QueueTicker::new("bingx", symbol, "derivatives", ts, price)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let trade = x
                .get("trades")
                .unwrap()
                .as_array()
                .unwrap()
                .first()
                .unwrap();
            let timestamp = trade.get("timestamp").unwrap().as_i64().unwrap();
            let price = trade.get("price").unwrap().as_str().unwrap();
            QueueTicker::new("bingx", symbol, "spot", timestamp, price)
        })
        .collect::<Vec<_>>()
    }
}

mod htx_int {
    use crate::QueueTicker;

    pub fn fetch_derivatives_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("contract_code").unwrap().as_str().unwrap();
            let ts = x.get("ts").unwrap().as_i64().unwrap();
            let bid_raw = x.get("bid").unwrap();
            let price = if bid_raw.is_null() {
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
            QueueTicker::new("htx", symbol, "derivatives", ts, &price)
        })
        .collect::<Vec<_>>()
    }

    pub fn fetch_spot_tickers_from_api() -> Vec<QueueTicker> {
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
            let symbol = x.get("symbol").unwrap().as_str().unwrap();
            let price = x.get("ask").unwrap().as_f64().unwrap().to_string();
            QueueTicker::new("htx", symbol, "spot", ts, &price)
        })
        .collect::<Vec<_>>()
    }
}

fn write_to_queue(rx: std::sync::mpsc::Receiver<QueueTicker>) {
    let mut queue_tickers = vec![];
    loop {
        match rx.recv() {
            Ok(v) => queue_tickers.push(v),
            Err(e) => {
                log::error!("e={:?}", e);
                continue;
            }
        }
        if queue_tickers.len() == 500 {
            log::info!("produce len={}", queue_tickers.len());
            let queue_tickers_to_produce = queue_tickers
                .iter()
                .map(|x| serde_json::to_string(&x).unwrap())
                .collect::<Vec<_>>();
            let _ = RedpandaPort::connect_produce_messages_sync(
                "tickers-contango-arbitrage",
                &queue_tickers_to_produce,
            );
            queue_tickers.clear();
        }
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
        if !Self::is_millis(ts) {
            panic!("ts={} is not in millis", ts);
        }
        Self {
            ex: ex.into(),
            s: s.into(),
            k: k.into(),
            ts,
            p: p.parse::<f64>().unwrap(),
        }
    }

    fn is_millis(ts: i64) -> bool {
        let threshold = 365 * 24 * 60 * 60 * 1000;
        ts >= threshold
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum QueueTickerType {
    Spot,
    Derivatives,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum QueueTickerExchange {
    Bybit,
    Binance,
    Mexc,
    Kucoin,
    Gateio,
    Bingx,
    Htx,
}

#[cfg(test)]
mod test {
    #[test]
    fn test_validate_millis() {
        let t = 365 * 24 * 60 * 60 * 1000;
        assert_eq!(crate::QueueTicker::is_millis(t - 1), false);
        assert_eq!(crate::QueueTicker::is_millis(t), true);
        assert_eq!(crate::QueueTicker::is_millis(t + 1), true);
    }

    #[test]
    fn test_enums_serialization() {
        use crate::{QueueTickerExchange, QueueTickerType};
        assert_eq!(
            serde_json::to_string(&QueueTickerType::Spot).unwrap(),
            "\"spot\""
        );
        assert_eq!(
            serde_json::to_string(&QueueTickerType::Derivatives).unwrap(),
            "\"derivatives\""
        );
        assert_eq!(
            serde_json::to_string(&QueueTickerExchange::Bybit).unwrap(),
            "\"bybit\""
        );
    }
}
