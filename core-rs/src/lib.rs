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
        self.low = f64::min(self.low, self.close);
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
    pub open_price: f64,
    pub open_timestamp_millis: i64,
    pub trailing_threshold: Option<f64>,
}

impl TrailingThreshold {
    pub fn new(open_price: f64, open_timestamp_millis: i64) -> Self {
        Self {
            open_price,
            open_timestamp_millis,
            trailing_threshold: None,
        }
    }

    pub fn apply_and_make_decision(&mut self, ticker: &FlatTicker) -> TrailingThresholdReason {
        let stop_loss_rel_val = 0.005; // TODO: calc this on prev data
        let stop_loss_abs_val = self.open_price * (1.0 - stop_loss_rel_val);
        if ticker.data_last_price <= stop_loss_abs_val {
            return TrailingThresholdReason::ReachStopLoss(stop_loss_abs_val);
        }
        if ticker.data_last_price <= self.open_price {
            return TrailingThresholdReason::Continue;
        }
        let trailing_rel_val = 0.5;
        let trailing_threshold_rel_val = 0.002; // TODO: calc this on prev data
        let next_threshold =
            self.open_price + (ticker.data_last_price - self.open_price) * trailing_rel_val;
        if self.trailing_threshold.is_none() {
            let calc_start_abs_val = self.open_price * (1.0 + trailing_threshold_rel_val);
            if ticker.data_last_price <= calc_start_abs_val {
                // log::debug!(
                //     "price haven't reached level to start calculating \
                //     trailing threshold calc_start_abs_val={calc_start_abs_val},\
                //     ticker.data_last_price={}",
                //     ticker.data_last_price,
                // );
            } else {
                // log::debug!("initialize threshold with {}", next_threshold);
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

pub struct DebugCandlesPort {
    client: Client,
}

impl DebugCandlesPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.debug_candles (
                created_at TIMESTAMP NOT NULL,
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
                open_time TIMESTAMP NOT NULL,
                open real NOT NULL,
                close real NOT NULL,
                high real NOT NULL,
                low real NOT NULL,
                timeframe varchar NOT NULL,
                commit_hash varchar NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn clear_table(&mut self) {
        log::info!("delete all rows in public.debug_candles");
        self.client
            .batch_execute("delete from public.debug_candles")
            .unwrap();
    }

    pub fn insert_batch(&mut self, list: &Vec<Candle>) -> Result<(), String> {
        let create_at_secs = chrono::Utc::now().timestamp();
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        let values = list
            .iter()
            .map(|x| {
                let timeframe = match x.timeframe {
                    CandleTimeframe::Hours(n) => format!("{n}h"),
                    CandleTimeframe::Minutes(n) => format!("{n}m"),
                };
                format!(
                    "(to_timestamp({})::timestamp, '{}', '{}', to_timestamp({})::timestamp,
                    {}, {}, {}, {}, '{}', '{}')",
                    create_at_secs,
                    x.exchange,
                    x.symbol,
                    x.open_time / 1000,
                    x.open,
                    x.close,
                    x.high,
                    x.low,
                    timeframe,
                    commit_hash,
                )
            })
            .collect::<Vec<String>>()
            .join(",");
        let query = format!(
            "INSERT INTO public.debug_candles
                (created_at, exchange, symbol, open_time, open, close, high, low,
                    timeframe, commit_hash)
                VALUES {values};",
        );
        log::info!("save {} debug candles", list.len());
        self.client.batch_execute(query.as_str()).unwrap();
        Ok(())
    }
}

pub struct DebugEmasPort {
    client: Client,
}

impl DebugEmasPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.debug_emas (
                created_at TIMESTAMP NOT NULL,
                value_ema real NOT NULL,
                commit_hash varchar NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn clear_table(&mut self) {
        log::info!("delete all rows in public.debug_emas");
        self.client
            .batch_execute("delete from public.debug_emas")
            .unwrap();
    }

    pub fn insert_batch(&mut self, list: &Vec<f64>) -> Result<(), String> {
        let create_at_secs = chrono::Utc::now().timestamp();
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        let values = list
            .iter()
            .map(|x| {
                format!(
                    "(to_timestamp({})::timestamp, {}, '{}')",
                    create_at_secs, x, commit_hash
                )
            })
            .collect::<Vec<String>>()
            .join(",");
        let query = format!(
            "INSERT INTO public.debug_emas
                (created_at, value_ema, commit_hash)
                VALUES {values};",
        );
        log::info!("save {} debug emas", list.len());
        self.client.batch_execute(query.as_str()).unwrap();
        Ok(())
    }
}

pub struct BacktestOut {
    open_price: f64,
    close_price: f64,
    open_timestamp_millis: i64,
    close_timestamp_millis: i64,
    close_reason: String,
}

impl BacktestOut {
    pub fn new(
        open_price: f64,
        close_price: f64,
        open_timestamp_millis: i64,
        close_timestamp_millis: i64,
        close_reason: String,
    ) -> Self {
        Self {
            open_price,
            close_price,
            open_timestamp_millis,
            close_timestamp_millis,
            close_reason,
        }
    }
}

pub struct BacktestsPort {
    client: Client,
}

impl BacktestsPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.backtests (
                created_at TIMESTAMP NOT NULL,
                open_price real NOT NULL,
                close_price real NOT NULL,
                open_timestamp TIMESTAMP NOT NULL,
                close_timestamp TIMESTAMP NOT NULL,
                close_reason varchar NOT NULL,
                strategy_name varchar NOT NULL,
                commit_hash varchar NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn clear_table(&mut self) {
        log::info!("delete all rows in public.backtests");
        self.client
            .batch_execute("delete from public.backtests")
            .unwrap();
    }

    pub fn insert_batch(
        &mut self,
        list: &Vec<BacktestOut>,
        strategy_name: String,
    ) -> Result<(), String> {
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        let values = list
            .iter()
            .map(|x| {
                format!(
                    "(now(), {}, {}, to_timestamp({})::timestamp,
                    to_timestamp({})::timestamp, '{}', '{}', '{}')",
                    x.open_price,
                    x.close_price,
                    x.open_timestamp_millis / 1000,
                    x.close_timestamp_millis / 1000,
                    x.close_reason,
                    strategy_name,
                    commit_hash,
                )
            })
            .collect::<Vec<String>>()
            .join(",");
        let query = format!(
            "INSERT INTO public.backtests
                (created_at, open_price, close_price, open_timestamp,
                    close_timestamp, close_reason, strategy_name, commit_hash)
                VALUES {values};",
        );
        log::info!("save {} debug emas", list.len());
        self.client.batch_execute(query.as_str()).unwrap();
        Ok(())
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
            tickers.push(create_ticker_on_ts(start_millis + i));
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
            .map(|i| create_ticker_on_ts(start_millis + i))
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

    #[test]
    fn test_trailing_threshold_reach_trailing_threshold() {
        let mut t = TrailingThreshold::new(60_000_f64, 0);
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
        let mut t = TrailingThreshold::new(60_000_f64, 0);
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

    /// Cover exit on the next chart https://prnt.sc/kKfOTb_rqRJ7.
    #[test]
    fn test_trailing_threshold_reach_trailing_threshold_case_1() {
        return;
        let open_price = 62310.50;
        let tickers_prices = vec![
            61809.93, 64953.85, 64184.61, 65179.0, 64098.13, 65099.0, 64490.0, 64943.87, 64420.0,
            65213.31, 64691.28, 65450.0, 64770.0, 65440.57, 64436.51, 65069.07, 63820.57, 64965.78,
            64333.33, 64707.7, 63506.57, 64533.29, 63929.18, 64808.54, 64084.86, 64660.86,
            64085.44, 64393.96, 63677.36, 64334.84, 64044.11, 64444.91, 64000.01, 64494.03,
            62953.9, 64124.0, 63418.0, 64263.41, 63090.07, 63777.03, 63530.0, 64092.29, 63766.0,
            64040.67, 63815.53, 64268.58, 63987.17, 64250.08, 63939.0, 64222.22, 63922.36,
            64159.99, 63661.54, 63988.0, 63277.0, 63778.32, 63377.22, 63781.82, 63557.74, 63924.05,
            63575.83, 63935.32, 63798.92, 64047.92, 63746.0, 63984.79, 63790.0, 64960.37, 64456.54,
            64969.39, 64739.23, 65419.0, 64837.19, 65310.99, 64791.79, 65000.01, 64622.12,
            65031.25, 64622.13, 64825.39, 64555.0, 64778.0, 64613.51, 64988.92, 64608.0, 64960.0,
            64737.5, 65100.0, 64921.0, 65377.0, 65093.99, 65695.56, 64843.98, 65195.98, 64984.44,
            65244.65, 64941.79, 65200.0, 64899.84, 65175.0, 64920.26, 65222.07, 64763.15, 64986.72,
            64785.04, 64986.0, 64954.5, 65440.95, 64934.0, 65375.41, 64778.69, 65244.9, 64790.0,
            65036.82, 64880.0, 65132.07, 64237.5, 65087.92, 64530.89, 64875.98, 64596.09, 64862.5,
            64528.38, 64856.05, 64362.5, 64722.04, 64646.16, 65167.19, 64569.07, 65110.0, 64866.0,
            65063.0, 64758.93, 65158.08, 64604.56, 65640.0, 64500.0, 64862.5, 64754.0, 66090.69,
            65509.24, 65878.17, 65701.99, 66414.81, 66150.8, 66412.6, 65979.33, 66479.8, 65905.73,
            66204.44, 65801.76, 66055.17, 65890.0, 66154.87, 65702.0, 66108.0, 65626.87, 65995.48,
            65693.98, 66260.54, 65962.16, 66477.53, 65665.86, 66085.46, 65913.52, 66419.56,
            66133.35, 66824.28, 66293.05, 66691.22, 66141.07, 66609.33, 66394.52, 66760.56,
            66342.1, 66558.91, 66526.94, 67200.0, 66790.0, 67232.35, 66712.96, 67032.83, 66777.4,
            67183.01, 66558.31, 66912.0, 66419.55, 66756.0, 66181.1, 66582.67, 66192.0, 66609.31,
            66304.0, 66646.0, 66020.0, 66451.19, 65963.4, 66260.47, 66046.63, 66309.3, 66058.99,
            66310.44, 65852.35, 66236.13, 65765.81, 66165.0, 66049.4, 66590.92, 66420.0, 67130.0,
            66328.44, 66930.47, 66350.91, 66807.91, 66584.61, 66905.99, 66589.47, 66846.0, 66390.0,
            66783.57, 65980.0, 66484.96, 66258.86, 66464.0, 66278.59, 66455.76, 66120.0, 66433.31,
            66390.0, 66747.73, 66682.33, 66900.0, 66441.32, 66766.0, 66514.44, 66714.43, 66589.01,
            67065.0, 66633.0, 67070.43, 66552.37, 66792.0, 66588.0, 66883.68, 66485.3, 66743.01,
            66272.0, 66560.51, 66203.05, 66517.85, 66361.73, 66631.01, 66351.1, 66737.0, 65834.28,
            66355.99, 64732.34, 66200.0, 64519.03, 65243.73, 64348.0, 64959.74, 64766.33, 65156.95,
            64070.15, 64920.0, 63736.7, 64486.0, 63606.06, 64240.01, 64000.0, 64447.91, 63956.49,
            64404.0, 63825.72, 64339.83, 64226.6, 64741.47, 64046.74, 64657.15, 63860.0, 64338.58,
            64128.3, 64464.29, 64058.0, 64397.78, 64025.5, 64377.6, 64195.7, 64391.99, 63824.07,
            64379.85, 63708.2, 64176.86, 63809.13, 64192.0, 63337.25, 63966.03, 63447.37, 63994.23,
        ];
        let mut t = TrailingThreshold::new(open_price, 0);
        for p in tickers_prices {
            let out = match t.apply_and_make_decision(&create_ticker_on_price(p)) {
                TrailingThresholdReason::ReachThrailingStop(v) => {
                    format!("reach-trailing-stop {v}")
                }
                TrailingThresholdReason::ReachStopLoss(v) => format!("reach-stop-loss {v}"),
                TrailingThresholdReason::Continue => "continue".to_string(),
            };
            println!("{p:.2}: {out}")
        }
        assert_eq!(0, 1)
    }
}
