use chrono::Datelike;
use postgres::{Client, NoTls};
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use struct_field_names_as_array::FieldNamesAsArray;

pub mod constants;
pub mod domain;
pub mod port;
pub mod repository;

pub fn timestamp_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn pool(threads: &[thread::JoinHandle<()>]) {
    loop {
        thread::sleep(Duration::from_millis(1000));
        if threads.iter().any(|x| x.is_finished()) {
            log::warn!("some thread is not alive, finishing...");
            break;
        }
    }
}

pub struct OrderBookCache {
    bids: HashMap<String, f64>,
    asks: HashMap<String, f64>,
    last_update_id: u64,
}

impl Default for OrderBookCache {
    fn default() -> Self {
        let bids = HashMap::<String, f64>::new();
        let asks = HashMap::<String, f64>::new();
        Self {
            bids,
            asks,
            last_update_id: 0,
        }
    }
}

impl OrderBookCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_orders(&mut self, u: u64, bids: &Vec<[String; 2]>, asks: &Vec<[String; 2]>) {
        if u <= self.last_update_id {
            return;
        }
        for bid in bids {
            let amount = bid[1].parse::<f64>().unwrap();
            self.bids.insert(bid[0].clone(), amount);
            if bid[1] == "0.00000000" || bid[1] == "0" {
                self.bids.remove(&bid[0].clone());
            }
        }
        for ask in asks {
            let amount = ask[1].parse::<f64>().unwrap();
            self.asks.insert(ask[0].clone(), amount);
            if ask[1] == "0.00000000" || ask[1] == "0" {
                self.asks.remove(&ask[0].clone());
            }
        }
        self.last_update_id = u
    }

    pub fn get_last_bid(&self) -> Option<[f64; 2]> {
        if self.bids.is_empty() {
            return None;
        }
        let mut bids: Vec<[f64; 2]> = self
            .bids
            .iter()
            .map(|(key, val)| [key.parse::<f64>().unwrap(), *val])
            .collect();
        bids.sort_by(|a, b| a[0].total_cmp(&b[0]));
        return Some(*bids.last().unwrap());
    }

    pub fn get_last_ask(&self) -> Option<[f64; 2]> {
        if self.asks.is_empty() {
            return None;
        }
        let mut asks: Vec<[f64; 2]> = self
            .asks
            .iter()
            .map(|(key, val)| [key.parse::<f64>().unwrap(), *val])
            .collect();
        asks.sort_by(|a, b| a[0].total_cmp(&b[0]));
        return Some(*asks.first().unwrap());
    }

    pub fn calc_market_price(&self) -> Option<f64> {
        if let (Some(last_bid), Some(last_ask)) = (self.get_last_bid(), self.get_last_ask()) {
            let market_price = last_bid[0] + (last_ask[0] - last_bid[0]) / 2_f64;
            return Some(market_price);
        }
        None
    }

    pub fn calc_spread_abs(&self) -> Option<f64> {
        if let (Some(last_bid), Some(last_ask)) = (self.get_last_bid(), self.get_last_ask()) {
            return Some(last_ask[0] - last_bid[0]);
        }
        None
    }
}

/// NOTE: exists to use serde_json::to_string_pretty
#[derive(serde::Serialize)]
struct TelegramMessage<'a> {
    message: &'a str,
    action: &'a str,
    now: &'a str,
}

pub struct TelegramBotPort {
    token: String,
    chat_id: String,
}

impl TelegramBotPort {
    pub fn new_from_envs() -> Self {
        let token = env::var("TELEGRAM_BOT_API_KEY").unwrap();
        let chat_id = env::var("TELEGRAM_BOT_CHAT_ID").unwrap();
        Self { token, chat_id }
    }

    pub fn notify<'a>(&self, message: &'a str, parse_mode: Option<&'a str>) {
        let url_str = format!(
            "https://api.telegram.org/bot{}/sendMessage\
            ?chat_id={}&text={}&parse_mode={}",
            self.token,
            self.chat_id,
            message,
            parse_mode.unwrap_or(""),
        );
        let url_encoded = url::Url::parse(&url_str).unwrap();
        let response = reqwest::blocking::get(url_encoded).unwrap();
        let response_text = response.text().unwrap();
        log::debug!("response_text: {}", response_text);
    }

    pub fn notify_markdown(&self, message: &str) {
        let message_formatted = format!("```%0A{}```", message)
            // XXX: create issue in reqwest repository
            //      \n in encoded variant should %0A, not %20
            .replace('\n', "%0A");
        self.notify(message_formatted.as_str(), Some("Markdown"));
    }

    pub fn notify_pretty(&self, message: String, action: String) {
        let now = chrono::prelude::Utc::now().to_string();
        let message = serde_json::to_string_pretty(&TelegramMessage {
            message: &message,
            action: &action,
            now: now.as_str(),
        })
        .unwrap();
        self.notify_markdown(message.as_str());
    }
}

// TODO: use clap
#[derive(Clone)]
pub struct Args {
    pub write_needed: bool,
    pub show_binance: bool,
    pub symbols_amount: Option<usize>,
    pub command: String,
}

impl Args {
    pub fn new_and_parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut write_needed = false;
        let mut show_binance = false;
        let mut symbols_amount = None;
        let mut command = "".to_string();
        for i in 0..args.len() {
            match args[i].as_str() {
                "--write" => write_needed = true,
                "--show-binance" => show_binance = true,
                "--symbols-amount" => symbols_amount = Some(args[i + 1].parse::<usize>().unwrap()),
                "--command" => command.clone_from(&args[i + 1]),
                _ => {}
            }
        }
        if command.is_empty() {
            panic!("--command must be passed");
        }
        Self {
            write_needed,
            show_binance,
            symbols_amount,
            command,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FlatTicker {
    pub ts_millis: i64,
    pub data_symbol: String,
    pub data_last_price: f64,
    pub exchange: String,
}

impl FlatTicker {
    /// Creates struct and validates millis
    pub fn new_with_millis(
        ts_millis: i64,
        data_symbol: &str,
        data_last_price: f64,
        exchange: &str,
    ) -> Result<Self, String> {
        let year = chrono::DateTime::from_timestamp_millis(ts_millis)
            .unwrap()
            .year();
        if year < 2000 {
            return Err(format!("invalid year={year} for millis param"));
        }
        let o = Self {
            ts_millis,
            data_symbol: data_symbol.to_string(),
            data_last_price,
            exchange: exchange.to_string(),
        };
        Ok(o)
    }
}

#[derive(Clone, Debug)]
pub struct FlatDepth {
    pub update_id: u64,
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum CandleTimeframe {
    Hours(i64),
    Minutes(i64),
    Days(i64),
}

#[derive(Debug)]
pub struct Candle {
    pub exchange: String,
    pub symbol: String,
    pub open_time: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub timeframe: CandleTimeframe,
}

impl Candle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange: String,
        symbol: String,
        open_time: i64,
        open: f64,
        close: f64,
        high: f64,
        low: f64,
        timeframe: CandleTimeframe,
    ) -> Self {
        Self {
            exchange,
            symbol,
            open_time,
            open,
            close,
            high,
            low,
            timeframe,
        }
    }

    pub fn new_from_ticker(ticker: &FlatTicker, timeframe: &CandleTimeframe) -> Self {
        Self {
            exchange: ticker.exchange.clone(),
            symbol: ticker.data_symbol.clone(),
            open_time: ticker.ts_millis,
            open: ticker.data_last_price,
            close: ticker.data_last_price,
            high: ticker.data_last_price,
            low: ticker.data_last_price,
            timeframe: timeframe.clone(),
        }
    }

    pub fn apply_ticker(&mut self, ticker: &FlatTicker) {
        self.close = ticker.data_last_price;
        self.high = f64::max(self.high, self.close);
        self.low = f64::min(self.low, self.close);
    }

    pub fn expired(&self, ticker: &FlatTicker) -> bool {
        let interval_millis = match self.timeframe {
            CandleTimeframe::Minutes(n) => n * 60 * 1000,
            CandleTimeframe::Hours(n) => n * 60 * 60 * 1000,
            CandleTimeframe::Days(n) => n * 24 * 60 * 60 * 1000,
        };
        let current_start_min = ticker.ts_millis / interval_millis;
        let candle_start_min = self.open_time / interval_millis;
        current_start_min > candle_start_min
    }

    /// Waits for beginning of the next candle
    pub fn wait_for_next(rx: &std::sync::mpsc::Receiver<FlatTicker>) -> FlatTicker {
        let candle_start_time_millis = ((std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            / 60_000)
            + 1)
            * 60_000;
        log::info!("wait for the next minute beginning");
        loop {
            let ticker = rx.recv().unwrap();
            if ticker.ts_millis >= candle_start_time_millis {
                log::info!("first found candle timestamp is {}", ticker.ts_millis);
                break ticker;
            }
        }
    }

    pub fn to_json_string(&self) -> String {
        let timeframe = match self.timeframe {
            CandleTimeframe::Minutes(n) => format!("{n}m"),
            CandleTimeframe::Hours(n) => format!("{n}h"),
            CandleTimeframe::Days(n) => format!("{n}d"),
        };
        serde_json::json!({
            "exchange": self.exchange,
            "symbol": self.symbol,
            "open_time": self.open_time,
            "open": self.open,
            "close": self.close,
            "high": self.high,
            "low": self.low,
            "timeframe": timeframe,
        })
        .to_string()
    }
}

pub fn connect_to_postgres() -> Client {
    match Client::configure()
        .host(env::var("POSTGRES_HOST").unwrap().as_str())
        .port(env::var("POSTGRES_PORT").unwrap().parse::<u16>().unwrap())
        .user(env::var("POSTGRES_USER").unwrap().as_str())
        .password(env::var("POSTGRES_PASSWORD").unwrap().as_str())
        .dbname(env::var("POSTGRES_DBNAME").unwrap().as_str())
        .connect(NoTls)
    {
        Ok(client) => client,
        Err(error) => panic!("postgres connection error: {}", error),
    }
}

pub struct TickersPort {
    client_postgres: Client,
}

impl TickersPort {
    pub fn new_and_connect() -> Self {
        Self {
            client_postgres: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client_postgres
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.tickers (
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                last_price real NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert(&mut self, x: &FlatTicker) -> Result<(), String> {
        let query = format!(
            "INSERT INTO public.tickers(exchange, symbol, timestamp, last_price)
            VALUES ('{}','{}',to_timestamp({})::timestamp,{});",
            x.exchange,
            x.data_symbol,
            x.ts_millis / 1000,
            x.data_last_price
        );
        match self.client_postgres.batch_execute(query.as_str()) {
            Ok(v) => Ok(v),
            Err(error) => Err(format!("error: {}", error)),
        }
    }

    pub fn remove_on_date(&mut self, date: &chrono::NaiveDate) -> Result<(), String> {
        let date_str = date.to_string();
        log::debug!("remove records on {date_str} in public.tickers table",);
        let query = format!("delete from public.tickers where timestamp::date = '{date_str}';");
        self.client_postgres.batch_execute(query.as_str()).unwrap();
        Ok(())
    }

    pub fn insert_batch(&mut self, list: &[FlatTicker]) -> Result<(), String> {
        let n = 1_000_000;
        for i in 0..(list.len() / n + 1) {
            let (j, k) = (i * n, usize::min(i * n + n, list.len()));
            log::debug!("write [{},{}]", j, k);
            let values = &list[j..k]
                .iter()
                .map(|x| {
                    format!(
                        "('{}','{}',to_timestamp({})::timestamp,{})",
                        x.exchange,
                        x.data_symbol,
                        x.ts_millis / 1000,
                        x.data_last_price
                    )
                })
                .collect::<Vec<String>>()
                .join(",");
            let query = format!(
                "INSERT INTO public.tickers(exchange, symbol, timestamp, last_price)
                VALUES {values};",
            );
            self.client_postgres.batch_execute(query.as_str()).unwrap();
        }
        Ok(())
    }

    pub fn fetch(&mut self, start_time_secs: i64, end_time_secs: i64) -> Vec<FlatTicker> {
        let q = format!(
            "
            SELECT exchange, symbol, extract(epoch from timestamp), last_price
            FROM public.tickers
            where extract(epoch from timestamp) >= {start_time_secs}
                and extract(epoch from timestamp) <= {end_time_secs}
            order by timestamp;
        "
        );
        self.client_postgres
            .query(&q, &[])
            .unwrap()
            .iter()
            .map(|x| {
                FlatTicker::new_with_millis(
                    (x.get::<_, f64>(2) * 1000.0) as i64,
                    x.get(1),
                    x.get::<_, f32>(3) as f64,
                    x.get(0),
                )
                .unwrap()
            })
            .collect()
    }
}

pub struct DebugsPortClickhouse {
    client: clickhouse::Client,
}

impl DebugsPortClickhouse {
    pub fn new_and_connect() -> Self {
        let client = clickhouse::Client::default().with_url("http://127.0.0.1:18123");
        Self { client }
    }

    pub async fn insert_batch(&self, debugs: &Vec<DebugsRow>) {
        let mut insert = self.client.insert("debugs").unwrap();
        for debug_row in debugs {
            insert.write(debug_row).await.unwrap();
        }
        insert.end().await.unwrap();
    }
}

#[derive(clickhouse::Row, serde::Serialize)]
pub struct DebugsRow {
    pub run_id: String,
    pub kind: String,
    pub content: String,
}

pub struct TradesTPortClickhouse {
    client: clickhouse::Client,
}

impl TradesTPortClickhouse {
    pub fn new_and_connect() -> Self {
        let client = clickhouse::Client::default().with_url("http://127.0.0.1:18123");
        Self { client }
    }

    ///
    /// ### Examples
    /// ```no_run
    /// use exchanges_arbitrage::TradesTPortClickhouse;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let _ = TradesTPortClickhouse::new_and_connect()
    ///         .fetch("2024-01-01", "2025-01-01").await;
    /// }
    /// ```
    ///
    pub async fn fetch(&mut self, start_time_iso: &str, end_time_iso: &str) -> Vec<FlatTicker> {
        let query = format!(
            "
            SELECT {}
            FROM default.trades_t_raw
            WHERE time BETWEEN '{}' AND '{}'
            ORDER BY id ASC
            ",
            TradesRow::FIELD_NAMES_AS_ARRAY.join(","),
            start_time_iso,
            end_time_iso
        );
        self.client
            .query(query.as_str())
            .fetch_all::<TradesRow>()
            .await
            .unwrap()
            .iter()
            .map(|x| {
                let ts_millis = (x.time.unix_timestamp_nanos() / 1_000_000) as i64;
                FlatTicker::new_with_millis(ts_millis, &x.symbol, x.price, &x.exchange).unwrap()
            })
            .collect()
    }

    pub fn fetch_in_parallel(
        start_timestamp: &chrono::NaiveDateTime,
        end_timestamp: &chrono::NaiveDateTime,
    ) -> Vec<FlatTicker> {
        let n = 25;
        let start_millis = start_timestamp.and_utc().timestamp_millis();
        let interval_millis = end_timestamp.and_utc().timestamp_millis() - start_millis;
        let step_millis = interval_millis / n;
        let cache = vec![vec![]; n as usize];
        let cache_arc = std::sync::Arc::new(std::sync::Mutex::new(cache));
        (0..n)
            .map(|i| {
                let l = start_millis + step_millis * i + 1000;
                let r = start_millis + step_millis * (i + 1);
                let l_ts_str = chrono::DateTime::from_timestamp_millis(l)
                    .unwrap()
                    .naive_utc()
                    .to_string();
                let r_ts_str = chrono::DateTime::from_timestamp_millis(r)
                    .unwrap()
                    .naive_utc()
                    .to_string();
                let cache_arc_int = cache_arc.clone();
                std::thread::spawn(move || {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(async {
                            log::debug!("fetch [{},{},{}]", &i, l_ts_str, r_ts_str);
                            let mut port = TradesTPortClickhouse::new_and_connect();
                            let tickers = port.fetch(&l_ts_str, &r_ts_str).await;
                            cache_arc_int.lock().unwrap()[i as usize] = tickers;
                        })
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|t| t.join().unwrap());
        return cache_arc
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
    }

    pub async fn insert_batch_chunked(&self, vec: &[TradesRow]) {
        let slice_size = 100_000;
        let mut i = 0;
        while i < vec.len() {
            let mut insert = self.client.insert("trades_t_raw").unwrap();
            let mut to_write_amount = 0;
            for _ in 0..slice_size {
                if i == vec.len() {
                    break;
                }
                insert.write(&vec[i]).await.unwrap();
                to_write_amount += 1;
                i += 1;
            }
            log::debug!(
                "TradesTPortClickhouse::insert_batch_chunked \
                insert tick with slice_size={slice_size} to_write_amount={to_write_amount}",
            );
            insert.end().await.unwrap();
        }
    }
}

pub struct MlflowPort {
    experiment_id: Option<String>,
    run_id: Option<String>,
    base_url: String,
}

impl Default for MlflowPort {
    fn default() -> Self {
        let base_url = "http://127.0.0.1:5000/api/2.0/mlflow".into();
        Self {
            experiment_id: None,
            run_id: None,
            base_url,
        }
    }
}

impl MlflowPort {
    pub fn new() -> Self {
        Self::default()
    }

    ///
    /// Fetches experiment metadata.
    /// If experiemnt doesn't exist creates it.
    ///
    pub async fn set_experiment(&mut self, experiment_name: &str) {
        let url_experiments_get_by_name = format!(
            "{}/experiments/get-by-name?experiment_name={}",
            self.base_url, experiment_name
        );
        let url_experiments_create = format!("{}/experiments/create", self.base_url);
        let res_get_experiment = reqwest::get(url_experiments_get_by_name)
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let error_code_default = serde_json::json!("");
        let error_code_get_experiment = res_get_experiment
            .get("error_code")
            .unwrap_or(&error_code_default)
            .as_str()
            .unwrap();
        if error_code_get_experiment == "RESOURCE_DOES_NOT_EXIST" {
            log::debug!("experiment={} haven't found -> create", experiment_name);
            let body = serde_json::json!({
                "name": experiment_name,
            })
            .to_string();
            let res_create_experiment = reqwest::Client::new()
                .post(url_experiments_create)
                .body(body)
                .header("Content-Type", "application/json")
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
            let res_experiment_id = res_create_experiment
                .get("experiment_id")
                .unwrap()
                .as_str()
                .unwrap();
            self.experiment_id = Some(res_experiment_id.into());
        } else {
            log::debug!("already exists experiment={}", experiment_name);
            let res_experiment_id = res_get_experiment
                .get("experiment")
                .unwrap()
                .as_object()
                .unwrap()
                .get("experiment_id")
                .unwrap()
                .as_str()
                .unwrap();
            self.experiment_id = Some(res_experiment_id.into());
        }
        log::debug!("experiment_id={}", self.experiment_id.as_ref().unwrap());
    }

    ///
    /// Create run in experiment.
    ///
    pub async fn start_run(&mut self) {
        let url_runs_create = format!("{}/runs/create", self.base_url);
        let run_name = format!("run-name-{}", rand::thread_rng().gen::<u32>());
        let body = serde_json::json!({
            "experiment_id": self.experiment_id.as_ref().unwrap(),
            "run_name": run_name,
            "start_time": (timestamp_secs() * 1000.0) as u64,
        })
        .to_string();
        let res_create_run = reqwest::Client::new()
            .post(url_runs_create)
            .body(body)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let run_id = res_create_run
            .get("run")
            .unwrap()
            .as_object()
            .unwrap()
            .get("info")
            .unwrap()
            .as_object()
            .unwrap()
            .get("run_id")
            .unwrap()
            .as_str()
            .unwrap();
        self.run_id = Some(run_id.into());
        log::debug!("mlflow run_id={:#?}", run_id);
    }

    pub async fn log_parameter(&self, key: &str, value: &str) {
        let url_runs_log_parameter = format!("{}/runs/log-parameter", self.base_url);
        let body = serde_json::json!({
            "run_id": self.run_id.as_ref().unwrap(),
            "key": key,
            "value": value,
        })
        .to_string();
        let _ = reqwest::Client::new()
            .post(url_runs_log_parameter)
            .body(body)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();
        log::debug!("log_parameter key={key} value={value}");
    }

    pub async fn log_metric(&self, key: &str, value: f64) {
        let url_runs_log_metric = format!("{}/runs/log-metric", self.base_url);
        let body = serde_json::json!({
            "run_id": self.run_id.as_ref().unwrap(),
            "key": key,
            "value": value,
            "timestamp": (timestamp_secs() * 1000.0) as u64,
        })
        .to_string();
        let _ = reqwest::Client::new()
            .post(url_runs_log_metric)
            .body(body)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();
        log::debug!("log_metric key={key} value={value}");
    }

    pub async fn runs_search(&self) -> Vec<serde_json::Value> {
        let url_runs_search = format!("{}/runs/search", self.base_url);
        let body = serde_json::json!({
            "experiment_ids": [self.experiment_id.as_ref().unwrap()],
            "max_results": 50000,
        })
        .to_string();
        let res_runs_search = reqwest::Client::new()
            .post(url_runs_search)
            .body(body)
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let res_runs = res_runs_search.get("runs").unwrap().as_array().unwrap();
        res_runs.to_vec()
    }
}

#[derive(
    clickhouse::Row,
    serde::Deserialize,
    serde::Serialize,
    struct_field_names_as_array::FieldNamesAsArray,
)]
pub struct TradesRow {
    pub id: u64,
    pub price: f64,
    pub qty: f64,
    pub base_qty: f64,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time: time::OffsetDateTime,
    pub is_buyer: bool,
    pub is_maker: bool,
    pub exchange: String,
    pub symbol: String,
    #[serde(with = "clickhouse::serde::time::date")]
    pub date_iso: time::Date,
}

pub enum TrailingThresholdReason {
    Continue,
    ReachStopLoss(f64),
    ReachThrailingStop(f64),
}

#[derive(Copy, Clone)]
pub struct TrailingThreshold {
    pub trailing_threshold: Option<f64>,
    pub open_price: f64,
    pub open_timestamp_millis: i64,
    stop_loss_rel_val: f64,
    trailing_threshold_rel_val: f64,
    trailing_threshold_min_incr_rel_val: f64,
}

impl TrailingThreshold {
    ///
    /// * `stop_loss_rel_val` - Percent from open-price decreasing to fire stop-loss. `0.005` means `0.5%` of open-price decrease will fire stop-loss event.
    /// * `trailing_threshold_rel_val` - Percent from `ticker-price - open-price` to increase and then decrease to fire trailing-threshold. `0.5` means  situation when price ups for `50%` of `ticker-price - open-price` then decreases below this mark will fire trailing-threshold event.
    /// * `trailing_threshold_min_incr_rel_val` - Percent of open-price after which thrailing-threshold will start to calculate. `0.002` means on a moment when ticker price ups `0.2%` of open-price trailing-threshold wil be started to calculate, till this moment trailing-threshold is idle.
    ///
    pub fn new(
        open_price: f64,
        open_timestamp_millis: i64,
        stop_loss_rel_val: f64,
        trailing_threshold_rel_val: f64,
        trailing_threshold_min_incr_rel_val: f64,
    ) -> Self {
        Self {
            trailing_threshold: None,
            open_price,
            open_timestamp_millis,
            stop_loss_rel_val,
            trailing_threshold_rel_val,
            trailing_threshold_min_incr_rel_val,
        }
    }

    pub fn apply_and_make_decision(&mut self, ticker: &FlatTicker) -> TrailingThresholdReason {
        let stop_loss_abs_val = self.open_price * (1.0 - self.stop_loss_rel_val);
        if ticker.data_last_price <= stop_loss_abs_val {
            return TrailingThresholdReason::ReachStopLoss(stop_loss_abs_val);
        }
        if ticker.data_last_price <= self.open_price {
            return TrailingThresholdReason::Continue;
        }
        let next_threshold = self.open_price
            + (ticker.data_last_price - self.open_price) * self.trailing_threshold_rel_val;
        if self.trailing_threshold.is_none() {
            let calc_start_abs_val =
                self.open_price * (1.0 + self.trailing_threshold_min_incr_rel_val);
            if ticker.data_last_price > calc_start_abs_val {
                self.trailing_threshold = Some(next_threshold);
            }
            return TrailingThresholdReason::Continue;
        }
        let curr_threshold = self.trailing_threshold.unwrap();
        if ticker.data_last_price <= curr_threshold {
            return TrailingThresholdReason::ReachThrailingStop(curr_threshold);
        }
        if curr_threshold < next_threshold {
            log::debug!(
                "moving threshold to new pos {next_threshold}, start_price={}",
                self.open_price
            );
            self.trailing_threshold = Some(next_threshold);
        }
        TrailingThresholdReason::Continue
    }
}

pub struct HistoryRow {
    strategy: String,
    cause: String,
    exchange: String,
    symbol: String,
    created_at: u128,
    commit_hash: String,
    trailing_threshold: f64,
    spread: f64,
    open_price: f64,
    profit_abs: f64,
    profit_rel: f64,
    last_price: f64,
}

impl HistoryRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        strategy: &str,
        cause: &str,
        exchange: &str,
        symbol: &str,
        open_price: f64,
        trailing_threshold: f64,
        spread: f64,
        profit_abs: f64,
        profit_rel: f64,
        last_price: f64,
    ) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        Self {
            strategy: strategy.to_string(),
            cause: cause.to_string(),
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            created_at,
            commit_hash,
            trailing_threshold,
            spread,
            open_price,
            profit_abs,
            profit_rel,
            last_price,
        }
    }
}

pub struct HistoryPort {
    client: Client,
}

impl HistoryPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.history_trades (
                created_at TIMESTAMP NOT NULL,
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
                commit_hash varchar NOT NULL,
                strategy varchar NOT NULL,
                cause varchar NOT NULL,
                open_price real NOT NULL,
                trailing_threshold real NOT NULL,
                profit_abs real NOT NULL,
                profit_rel real NOT NULL,
                last_price real NOT NULL,
                spread real NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert_hist(&mut self, x: &HistoryRow) -> Result<(), String> {
        let query = format!(
            "INSERT INTO public.history_trades
                (created_at, exchange, symbol, commit_hash, strategy, cause, open_price, \
                    trailing_threshold, profit_abs, profit_rel, last_price, spread)
            VALUES (to_timestamp({})::timestamp, '{}','{}','{}','{}','{}',{},{},{},{},{},{});",
            x.created_at / 1000,
            x.exchange,
            x.symbol,
            x.commit_hash,
            x.strategy,
            x.cause,
            x.open_price,
            x.trailing_threshold,
            x.profit_abs,
            x.profit_rel,
            x.last_price,
            x.spread,
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => Ok(v),
            Err(error) => Err(format!("error: {}", error)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{Candle, CandleTimeframe, FlatTicker, TrailingThreshold, TrailingThresholdReason};

    fn create_ticker_on_ts(ts_millis: i64) -> FlatTicker {
        FlatTicker::new_with_millis(ts_millis, "", 0.0, "").unwrap()
    }

    fn create_ticker_on_price(price: f64) -> FlatTicker {
        let now_millis = chrono::Utc::now().timestamp_millis();
        FlatTicker::new_with_millis(now_millis, "", price, "").unwrap()
    }

    #[test]
    fn test_candle_1m_timeframe() {
        let start_millis = 1714893660000;
        let mut tickers = vec![];
        for i in 0..(3 * 60 * 60 * 1000) {
            tickers.push(create_ticker_on_ts(start_millis + i));
        }
        let mut current_candle = Candle::new_from_ticker(&tickers[0], &CandleTimeframe::Minutes(1));
        let mut candles = vec![];
        assert_eq!(tickers.len(), 10800000);
        for ticker in tickers {
            if current_candle.expired(&ticker) {
                candles.push(current_candle);
                current_candle = Candle::new_from_ticker(&ticker, &CandleTimeframe::Minutes(1));
            } else {
                current_candle.apply_ticker(&ticker)
            }
        }
        assert_eq!(candles.len(), 179);
    }

    #[test]
    fn test_candle_1h_timeframe() {
        let start_millis = 1714893660000;
        let mut tickers = vec![];
        for i in 0..(3 * 60 * 60 * 1000) {
            tickers.push(create_ticker_on_ts(start_millis + i));
        }
        let mut current_candle = Candle::new_from_ticker(&tickers[0], &CandleTimeframe::Hours(1));
        let mut candles = vec![];
        assert_eq!(tickers.len(), 10800000);
        for ticker in tickers {
            if current_candle.expired(&ticker) {
                candles.push(current_candle);
                current_candle = Candle::new_from_ticker(&ticker, &CandleTimeframe::Hours(1));
            } else {
                current_candle.apply_ticker(&ticker)
            }
        }
        assert_eq!(candles.len(), 3);
    }

    #[test]
    fn test_candle_timeframe_threshold() {
        let start_millis = 1714893660000;
        let tickers = (0..(60_001))
            .into_iter()
            .map(|i| create_ticker_on_ts(start_millis + i))
            .collect::<Vec<FlatTicker>>();
        {
            let current_candle = Candle::new_from_ticker(&tickers[0], &CandleTimeframe::Minutes(1));
            let mut expired_amount = 0;
            for i in 0..tickers.len() {
                let ticker = &tickers[i];
                if current_candle.expired(ticker) {
                    expired_amount += 1;
                }
            }
            assert_eq!(expired_amount, 1);
        }
        {
            let current_candle = Candle::new_from_ticker(&tickers[0], &CandleTimeframe::Minutes(1));
            let mut expired_amount = 0;
            for i in 0..(tickers.len() - 1) {
                let ticker = &tickers[i];
                if current_candle.expired(ticker) {
                    println!("i: {i}, current_candle: {current_candle:?}, ticker: {ticker:?}",);
                    expired_amount += 1;
                }
            }
            assert_eq!(expired_amount, 0);
        }
    }

    #[test]
    fn test_invalid_ts_millis_in_flat_ticker() {
        match FlatTicker::new_with_millis(1714893719999, "", 0.0, "") {
            Ok(_) => {}
            Err(_) => assert_eq!(0, 1),
        };
        match FlatTicker::new_with_millis(1714893719, "", 0.0, "") {
            Ok(_) => assert_eq!(0, 1),
            Err(_) => {}
        };
    }

    #[test]
    fn test_trailing_threshold_reach_trailing_threshold() {
        let mut t = TrailingThreshold::new(60_000_f64, 0, 0.005, 0.5, 0.002);
        match t.apply_and_make_decision(&create_ticker_on_price(60_119_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert!(t.trailing_threshold.is_none());
        match t.apply_and_make_decision(&create_ticker_on_price(60_120_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert!(t.trailing_threshold.is_none());
        match t.apply_and_make_decision(&create_ticker_on_price(60_121_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert_eq!(t.trailing_threshold.unwrap(), 60060.5_f64);
        match t.apply_and_make_decision(&create_ticker_on_price(61_000_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert_eq!(t.trailing_threshold.unwrap(), 60_500_f64);
        match t.apply_and_make_decision(&create_ticker_on_price(60_501_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert_eq!(t.trailing_threshold.unwrap(), 60_500_f64);
        match t.apply_and_make_decision(&create_ticker_on_price(62_000_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert_eq!(t.trailing_threshold.unwrap(), 61_000_f64);
        match t.apply_and_make_decision(&create_ticker_on_price(61_001_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert_eq!(t.trailing_threshold.unwrap(), 61_000_f64);
        match t.apply_and_make_decision(&create_ticker_on_price(61_000_f64)) {
            TrailingThresholdReason::ReachThrailingStop(trailing_threshold) => {
                assert_eq!(trailing_threshold, 61_000_f64)
            }
            _ => assert_eq!(0, 1),
        };
    }

    #[test]
    fn test_trailing_threshold_reach_stop_loss() {
        let mut t = TrailingThreshold::new(60_000_f64, 0, 0.005, 0.5, 0.002);
        match t.apply_and_make_decision(&create_ticker_on_price(59_701_f64)) {
            TrailingThresholdReason::Continue => {}
            _ => assert_eq!(0, 1),
        };
        assert!(t.trailing_threshold.is_none());
        match t.apply_and_make_decision(&create_ticker_on_price(59_700_f64)) {
            TrailingThresholdReason::ReachStopLoss(stop_loss_threshold) => {
                assert_eq!(stop_loss_threshold, 59_700_f64)
            }
            _ => assert_eq!(0, 1),
        };
        assert!(t.trailing_threshold.is_none());
    }

    ///
    /// Covers why do exit doesn't happen on such chart https://prnt.sc/vwhKy6TXX_J-.
    ///
    #[test]
    fn test_trailing_threshold_reach_trailing_threshold_case_1() {
        let candles = [
            [62073.39, 64624.37, 61809.93, 64953.85],
            [64624.37, 64547.18, 64184.61, 65179.0],
            [64547.18, 64639.02, 64098.13, 65099.0],
            [64639.02, 64775.93, 64490.0, 64943.87],
            [64775.94, 64777.7, 64420.0, 65213.31],
            [64777.7, 64870.74, 64691.28, 65450.0],
            [64870.73, 64965.19, 64770.0, 65440.57],
            [64965.19, 64903.71, 64436.51, 65069.07],
            [64903.71, 64452.12, 63820.57, 64965.78],
            [64452.12, 64437.17, 64333.33, 64707.7],
            [64437.18, 64005.76, 63506.57, 64533.29],
            [64005.76, 64627.8, 63929.18, 64808.54],
            [64627.8, 64202.63, 64084.86, 64660.86],
            [64202.63, 64325.25, 64085.44, 64393.96],
            [64325.26, 64044.11, 63677.36, 64334.84],
            [64044.12, 64398.32, 64044.11, 64444.91],
            [64398.31, 64012.5, 64000.01, 64494.03],
            [64012.49, 63818.01, 62953.9, 64124.0],
        ];
        let mut t = TrailingThreshold::new(62073.39, 0, 0.005, 0.5, 0.002);
        for candle_prices in candles.iter() {
            for price in candle_prices.iter() {
                let decision = match t.apply_and_make_decision(&create_ticker_on_price(*price)) {
                    TrailingThresholdReason::ReachStopLoss(_) => "ReachStopLoss",
                    TrailingThresholdReason::ReachThrailingStop(_) => "ReachThrailingStop",
                    TrailingThresholdReason::Continue => "Continue",
                };
                println!("{price:.2} {decision}")
            }
        }
    }
}
