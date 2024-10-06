use exchanges_arbitrage::TradesTPortClickhouse;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::io::{Read, Seek, SeekFrom, Write};

use clap::Parser;

#[tokio::main]
async fn main() {
    env_logger::init();
    let run_args = RunArgs::parse();
    let trades_t_port = TradesTPortClickhouse::new_and_connect();
    match (run_args.exchange.as_str(), run_args.kind.as_str()) {
        ("binance", "trades") => {
            let symbol = "BTCUSDT";
            for date_iso in gen_dates_range("2024-04-07", "2024-09-07") {
                log::info!("run for date_iso={}", date_iso);
                let vec = Binance::new()
                    .fetch_parse_trades(symbol, &date_iso)
                    .await
                    .iter()
                    .map(|x| x.to_trades_t_row("binance", symbol, &date_iso))
                    .collect::<Vec<_>>();
                log::debug!("insert trades to clickhouse");
                trades_t_port.insert_batch_chunked(&vec).await;
            }
        }
        _ => log::error!("unknown exchange or kind"),
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct RunArgs {
    #[arg(short, long, help = "Exchange name")]
    exchange: String,
    #[arg(short, long, help = "Data type")]
    kind: String,
}

struct Binance {}

impl Binance {
    fn new() -> Self {
        Self {}
    }

    async fn fetch_parse_trades(&self, symbol: &str, date_iso: &str) -> Vec<TradeRaw> {
        let url_file = self
            .fetch_parse_file_url(BinanceDataKind::Trades, symbol, date_iso)
            .await;
        let csv_content = self.fetch_csv_content(&url_file).await;
        let mut rdr = csv::ReaderBuilder::new().from_reader(csv_content.as_bytes());
        let mut out = vec![];
        while let Some(result) = rdr.records().next() {
            let row = result.unwrap().deserialize::<TradeRaw>(None).unwrap();
            out.push(row);
        }
        out
    }

    async fn fetch_parse_file_url(
        &self,
        kind: BinanceDataKind,
        symbol: &str,
        date_iso: &str,
    ) -> String {
        let url_meta = "https://www.binance.com/bapi/bigdata\
                        /v1/public/bigdata/finance/exchange/listDownloadData2";
        let product_name = match kind {
            BinanceDataKind::Trades => "trades",
            BinanceDataKind::Klines => "klines",
        };
        let body_meta = serde_json::json!({
            "bizType": "SPOT",
            "productName": product_name,
            "symbolRequestItems": [
                {
                    "endDay": date_iso,
                    "granularityList": ["1m"],
                    "interval": "daily",
                    "startDay": date_iso,
                    "symbol": symbol,
                }
            ],
        })
        .to_string();
        let res_meta = reqwest::Client::new()
            .post(url_meta)
            .body(body_meta)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();
        let t = res_meta.text().await.unwrap();
        let res_meta_obj: TradesMetaRes = serde_json::from_str(&t).unwrap();
        let l = res_meta_obj.data.downloadItemList;
        if l.len() != 1 {
            panic!("len(res_meta_obj.data.downloadItemList) != 1");
        }
        l[0].url.clone()
    }

    async fn fetch_csv_content(&self, url_file: &String) -> String {
        let csv_zip_bytes = reqwest::Client::new()
            .get(url_file.as_str())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut c = std::io::Cursor::new(Vec::new());
        c.write_all(&csv_zip_bytes).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();
        let reader = std::io::BufReader::new(c);
        let mut archive = zip::ZipArchive::new(reader).unwrap();
        assert_eq!(archive.len(), 1);
        let mut out_str = String::from("");
        archive
            .by_index(0)
            .unwrap()
            .read_to_string(&mut out_str)
            .unwrap();
        out_str
    }
}

#[allow(dead_code)]
enum BinanceDataKind {
    Trades,
    Klines,
}

struct RedpandaPort {}

impl RedpandaPort {
    ///
    /// Example:
    /// ```
    /// let vec = vec!["message str".to_string()];
    /// RedpandaPort::connect_produce_messages_chunked("trades", &vec).await;
    /// ```
    ///
    #[allow(dead_code)]
    async fn connect_produce_messages_chunked(topic_name: &str, vec: &Vec<String>) {
        let mut threads = vec![];
        let n = 25;
        log::debug!(
            "RedpandaPort::produce_messages_chunked \
            push trades to topic={} len={} in n={}",
            topic_name,
            vec.len(),
            n,
        );
        let vec_chunked: Vec<_> = vec.chunks(vec.len() / n + 1).collect();
        for i in 0..n {
            let chunk = vec_chunked[i].iter().cloned().collect::<Vec<_>>();
            let topic_name_cloned = topic_name.to_string().clone();
            let t = std::thread::spawn(move || {
                futures::executor::block_on(RedpandaPort::connect_produce_messages(
                    &topic_name_cloned,
                    &chunk,
                ))
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
    }

    async fn connect_produce_messages(topic_name: &str, vec: &Vec<String>) {
        log::debug!(
            "RedpandaPort::connect_produce_messages push trades to topic={} len={}",
            topic_name,
            vec.len()
        );
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .create()
            .expect("Producer creation failed");
        let futures = vec
            .into_iter()
            .map(|m| async move {
                producer
                    .clone()
                    .send(
                        FutureRecord::to(&topic_name.to_string())
                            .payload(&m.to_string())
                            .key(&"".to_string()),
                        5_000,
                    )
                    .await
            })
            .collect::<Vec<_>>();
        for future in futures {
            future.await.unwrap().unwrap();
        }
    }
}

fn gen_dates_range(start_iso: &str, end_iso: &str) -> Vec<String> {
    let mut left_date = chrono::NaiveDate::parse_from_str(start_iso, "%Y-%m-%d").unwrap();
    let right_date = chrono::NaiveDate::parse_from_str(end_iso, "%Y-%m-%d").unwrap();
    let mut out = vec![];
    while left_date < right_date {
        let date_iso = left_date.format("%Y-%m-%d").to_string();
        out.push(date_iso);
        left_date += chrono::TimeDelta::try_seconds(1 * 24 * 60 * 60).unwrap();
    }
    out
}

#[allow(dead_code)]
async fn fetch_parse_produce_klines() {
    let symbol = "BTCUSDT";
    let date_iso = "2024-04-07";
    let topic_name = "trades";
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .create()
        .expect("Producer creation failed");
    let url_file = {
        let url_meta = "https://www.binance.com/bapi/bigdata\
                        /v1/public/bigdata/finance/exchange/listDownloadData2";
        let body_meta = serde_json::json!({
            "bizType": "SPOT",
            "productName": "klines",
            "symbolRequestItems": [
                {
                    "endDay": date_iso,
                    "granularityList": ["1m"],
                    "interval": "daily",
                    "startDay": date_iso,
                    "symbol": symbol,
                }
            ],
        })
        .to_string();
        let res_meta = reqwest::Client::new()
            .post(url_meta)
            .body(body_meta)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();
        let t = res_meta.text().await.unwrap();
        let res_meta_obj: TradesMetaRes = serde_json::from_str(&t).unwrap();
        let l = res_meta_obj.data.downloadItemList;
        if l.len() != 1 {
            panic!("len(res_meta_obj.data.downloadItemList) != 1");
        }
        l[0].url.clone()
    };
    log::debug!("fetch file content");
    let csv_str = {
        let csv_zip_bytes = reqwest::Client::new()
            .get(url_file.as_str())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut c = std::io::Cursor::new(Vec::new());
        c.write_all(&csv_zip_bytes).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();
        let reader = std::io::BufReader::new(c);
        let mut archive = zip::ZipArchive::new(reader).unwrap();
        assert_eq!(archive.len(), 1);
        let mut out_str = String::from("");
        archive
            .by_index(0)
            .unwrap()
            .read_to_string(&mut out_str)
            .unwrap();
        out_str
    };
    log::debug!("parse trades");
    let trades_raw = {
        let mut out = vec![];
        let mut rdr = csv::ReaderBuilder::new().from_reader(csv_str.as_bytes());
        while let Some(result) = rdr.records().next() {
            let row: TradeRaw = result.unwrap().deserialize(None).unwrap();
            out.push(row);
        }
        out
    };
    log::debug!(
        "push trades to topic={} len={}",
        topic_name,
        trades_raw.len()
    );
    let futures = trades_raw
        .into_iter()
        .map(|trade| async move {
            let o = TradeQueue::new_from_raw(&trade, "", symbol, date_iso);
            let m = serde_json::to_string(&o).unwrap();
            producer
                .clone()
                .send(
                    FutureRecord::to(&topic_name.to_string())
                        .payload(&m.to_string())
                        .key(&"".to_string()),
                    5_000,
                )
                .await
        })
        .collect::<Vec<_>>();
    for future in futures {
        future.await.unwrap().unwrap();
    }
}

#[derive(Debug, serde::Deserialize)]
struct TradesMetaRes {
    data: TradesMetaResData,
}

#[allow(non_snake_case)]
#[derive(Debug, serde::Deserialize)]
struct TradesMetaResData {
    downloadItemList: Vec<TradesMetaResDataListItem>,
}

#[derive(Debug, serde::Deserialize)]
struct TradesMetaResDataListItem {
    url: String,
}

#[derive(Debug, serde::Deserialize)]
struct TradeRaw {
    id: u64,
    price: f64,
    qty: f64,
    base_qty: f64,
    time: u64,
    is_buyer: String,
    is_maker: String,
}

impl TradeRaw {
    fn to_trades_t_row(
        &self,
        exchange: &str,
        symbol: &str,
        date_iso: &str,
    ) -> exchanges_arbitrage::TradesRow {
        let is_buyer = match self.is_buyer.as_str() {
            "True" => true,
            "False" => false,
            _ => panic!("unknown is_buyer"),
        };
        let is_maker = match self.is_maker.as_str() {
            "True" => true,
            "False" => false,
            _ => panic!("unknown is_maker"),
        };
        exchanges_arbitrage::TradesRow {
            id: self.id,
            price: self.price,
            qty: self.qty,
            base_qty: self.base_qty,
            time: time::OffsetDateTime::from_unix_timestamp_nanos((self.time * 1_000_000) as i128)
                .unwrap(),
            is_buyer,
            is_maker,
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            date_iso: time::Date::parse(
                date_iso,
                time::macros::format_description!("[year]-[month]-[day]"),
            )
            .unwrap(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct TradeQueue {
    id: u64,
    price: f64,
    qty: f64,
    base_qty: f64,
    time: u64,
    is_buyer: bool,
    is_maker: bool,
    exchange: String,
    symbol: String,
    date_iso: String,
}

impl TradeQueue {
    fn new_from_raw(trade_raw: &TradeRaw, exchange: &str, symbol: &str, date_iso: &str) -> Self {
        let is_buyer = match trade_raw.is_buyer.as_str() {
            "True" => true,
            "False" => false,
            _ => panic!("unknown is_buyer"),
        };
        let is_maker = match trade_raw.is_maker.as_str() {
            "True" => true,
            "False" => false,
            _ => panic!("unknown is_maker"),
        };
        Self {
            id: trade_raw.id,
            price: trade_raw.price,
            qty: trade_raw.qty,
            base_qty: trade_raw.base_qty,
            time: trade_raw.time,
            is_buyer,
            is_maker,
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            date_iso: date_iso.to_string(),
        }
    }
}
