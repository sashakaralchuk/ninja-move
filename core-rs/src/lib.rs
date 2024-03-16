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

    pub fn apply_orders(
        &mut self,
        u: u64,
        bids: &Vec<[String; 2]>,
        asks: &Vec<[String; 2]>,
    ) {
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
}

/// NOTE: exists to use serde_json::to_string_pretty
#[derive(serde::Serialize)]
struct TelegramMessage<'a> {
    message: &'a str,
    action: &'a str,
    now: &'a str,
}

pub struct TelegramBotRepository {
    token: String,
    chat_id: String,
}

impl TelegramBotRepository {
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

    pub fn notify_pretty(&self, message: &'static str, action: &'static str) {
        let now = chrono::prelude::Utc::now().to_string();
        let message = serde_json::to_string_pretty(&TelegramMessage {
            message,
            action,
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
                "--symbols-amount" => {
                    symbols_amount = Some(args[i + 1].parse::<usize>().unwrap())
                }
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
    pub ts: u128,
    pub data_symbol: String,
    pub data_last_price: f64,
    pub exchange: String,
}

#[derive(Debug)]
pub struct Candle {
    pub exchange: String,
    pub symbol: String,
    pub open_time: u128,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
}

impl Candle {
    pub fn new_from_ticker(ticker: &FlatTicker) -> Self {
        Self {
            exchange: ticker.exchange.clone(),
            symbol: ticker.data_symbol.clone(),
            open_time: ticker.ts,
            open: ticker.data_last_price,
            close: ticker.data_last_price,
            high: ticker.data_last_price,
            low: ticker.data_last_price,
        }
    }

    pub fn apply_ticker(&mut self, ticker: &FlatTicker) {
        self.close = ticker.data_last_price;
        self.high = f64::max(self.high, self.close);
        self.low = f64::max(self.low, self.close);
    }

    pub fn expired(&self) -> bool {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let current_start_min = now_millis / 60_000;
        let candle_start_min = self.open_time / 60_000;
        current_start_min > candle_start_min
    }

    /// Waits for beginning of the next candle
    pub fn wait_for_next(
        rx: &std::sync::mpsc::Receiver<FlatTicker>,
    ) -> FlatTicker {
        let candle_start_time_millis = ((std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            / 60_000)
            + 1)
            * 60_000;
        log::info!("wait for the next minute beginning");
        loop {
            let ticker = rx.recv().unwrap();
            if ticker.ts >= candle_start_time_millis {
                log::info!("first found candle timestamp is {}", ticker.ts);
                break ticker;
            }
        }
    }
}
