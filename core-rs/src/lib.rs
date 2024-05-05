use chrono::Datelike;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

pub fn pool(threads: &Vec<thread::JoinHandle<()>>) {
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

impl OrderBookCache {
    pub fn new() -> Self {
        let bids = HashMap::<String, f64>::new();
        let asks = HashMap::<String, f64>::new();
        Self {
            bids,
            asks,
            last_update_id: 0,
        }
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
        if self.bids.len() == 0 {
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
        if self.asks.len() == 0 {
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

    pub fn notify_markdown<'a>(&self, message: &'a str) {
        let message_formatted = format!("```%0A{}```", message)
            // XXX: create issue in reqwest repository
            //      \n in encoded variant should %0A, not %20
            .replace("\n", "%0A");
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
                "--command" => command = args[i + 1].clone(),
                _ => {}
            }
        }
        if command.len() == 0 {
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
        let year = chrono::DateTime::from_timestamp_millis(ts_millis as i64)
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

#[derive(Debug)]
pub enum CandleTimeframe {
    Hours(i64),
    Minutes(i64),
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
    pub fn new_from_ticker(ticker: &FlatTicker, timeframe: CandleTimeframe) -> Self {
        Self {
            exchange: ticker.exchange.clone(),
            symbol: ticker.data_symbol.clone(),
            open_time: ticker.ts_millis,
            open: ticker.data_last_price,
            close: ticker.data_last_price,
            high: ticker.data_last_price,
            low: ticker.data_last_price,
            timeframe,
        }
    }

    pub fn apply_ticker(&mut self, ticker: &FlatTicker) {
        self.close = ticker.data_last_price;
        self.high = f64::max(self.high, self.close);
        self.low = f64::max(self.low, self.close);
    }

    pub fn expired(&self, ticker: &FlatTicker) -> bool {
        let interval_millis = match self.timeframe {
            CandleTimeframe::Minutes(n) => n * 60 * 1000,
            CandleTimeframe::Hours(n) => n * 60 * 60 * 1000,
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
    client: Client,
}

impl TickersPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
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
            VALUES {};",
            format!(
                "('{}','{}',to_timestamp({})::timestamp,{})",
                x.exchange,
                x.data_symbol,
                x.ts_millis / 1000,
                x.data_last_price
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }

    pub fn remove_on_date(&mut self, date: &chrono::NaiveDate) -> Result<(), String> {
        let date_str = date.to_string();
        log::debug!("remove records on {date_str} in public.tickers table",);
        let query = format!("delete from public.tickers where timestamp::date = '{date_str}';");
        self.client.batch_execute(query.as_str()).unwrap();
        Ok(())
    }

    pub fn insert_batch(&mut self, list: &Vec<FlatTicker>) -> Result<(), String> {
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
            self.client.batch_execute(query.as_str()).unwrap();
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
        self.client
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

pub enum TrailingThresholdReason {
    Continue,
    ReachStopLoss(f64),
    ReachThrailingStop(f64),
}

#[derive(Copy, Clone)]
pub struct TrailingThreshold {
    pub start_price: f64,
    trailing_threshold: Option<f64>,
}

impl TrailingThreshold {
    pub fn new(start_price: f64) -> Self {
        Self {
            trailing_threshold: None,
            start_price,
        }
    }

    pub fn apply_and_make_decision(&mut self, ticker: &FlatTicker) -> TrailingThresholdReason {
        let stop_loss_rel_val = 0.005; // TODO: calc this on prev data
        let trailing_rel_val = 0.5;
        let bottom_threshold = self.start_price * (1.0 - stop_loss_rel_val);
        if ticker.data_last_price <= bottom_threshold {
            return TrailingThresholdReason::ReachStopLoss(bottom_threshold);
        }
        if ticker.data_last_price <= self.start_price {
            return TrailingThresholdReason::Continue;
        }
        let next_threshold =
            self.start_price + (ticker.data_last_price - self.start_price) * trailing_rel_val;
        if self.trailing_threshold.is_none() {
            log::debug!("initialize threshold with {}", next_threshold);
            self.trailing_threshold = Some(next_threshold);
            return TrailingThresholdReason::Continue;
        }
        if ticker.data_last_price < self.trailing_threshold.unwrap() {
            return TrailingThresholdReason::ReachThrailingStop(self.trailing_threshold.unwrap());
        }
        log::debug!(
            "moving threshold to new pos {}, start_price={}",
            next_threshold,
            self.start_price
        );
        self.trailing_threshold = Some(next_threshold);
        return TrailingThresholdReason::Continue;
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
    pub fn new(
        strategy: &str,
        cause: &str,
        exchange: &String,
        symbol: &String,
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
            exchange: exchange.clone(),
            symbol: symbol.clone(),
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
            CREATE TABLE IF NOT EXISTS history_trades (
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
            VALUES {};",
            format!(
                "(to_timestamp({})::timestamp, '{}','{}','{}','{}','{}',{},{},{},{},{},{})",
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
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{Candle, CandleTimeframe, FlatTicker};

    fn create_ticker(ts_millis: i64) -> FlatTicker {
        FlatTicker::new_with_millis(ts_millis, "", 0.0, "").unwrap()
    }

    #[test]
    fn test_candle_1m_timeframe() {
        let start_millis = 1714893660000;
        let mut tickers = vec![];
        for i in 0..(3 * 60 * 60 * 1000) {
            tickers.push(create_ticker(start_millis + i));
        }
        let mut current_candle = Candle::new_from_ticker(&tickers[0], CandleTimeframe::Minutes(1));
        let mut candles = vec![];
        assert_eq!(tickers.len(), 10800000);
        for ticker in tickers {
            if current_candle.expired(&ticker) {
                candles.push(current_candle);
                current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Minutes(1));
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
            tickers.push(create_ticker(start_millis + i));
        }
        let mut current_candle = Candle::new_from_ticker(&tickers[0], CandleTimeframe::Hours(1));
        let mut candles = vec![];
        assert_eq!(tickers.len(), 10800000);
        for ticker in tickers {
            if current_candle.expired(&ticker) {
                candles.push(current_candle);
                current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Hours(1));
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
            .map(|i| create_ticker(start_millis + i))
            .collect::<Vec<FlatTicker>>();
        {
            let current_candle = Candle::new_from_ticker(&tickers[0], CandleTimeframe::Minutes(1));
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
            let current_candle = Candle::new_from_ticker(&tickers[0], CandleTimeframe::Minutes(1));
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
}
