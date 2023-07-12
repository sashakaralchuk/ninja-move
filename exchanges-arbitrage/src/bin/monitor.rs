#[macro_use] extern crate prettytable;

use std::ops::{Add, AddAssign};
use std::thread;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use chrono::{Timelike, Utc};
use prettytable::{Table, format};

use exchanges_arbitrage::{OrderBookCache, pool, Args};
use exchanges_arbitrage::repository::{monitoring, queue};
use exchanges_arbitrage::constants::{SYMBOLS_GATEIO, SYMBOLS_BINANCE};
use exchanges_arbitrage::domain::{gateio, binance};

struct PrecisionF64 {
    target: f64,
    diff: f64,
}

impl Default for PrecisionF64 {
    fn default() -> Self {
        let target = 10_f64.powi(9);
        let diff = 1.0 / target;
        Self { target, diff }
    }
}

impl PrecisionF64 {
    pub fn str_to_u64(&self, v: &String) -> u64 {
        (v.parse::<f64>().unwrap() * self.target) as u64
    }

    pub fn f64_to_u64(&self, v: f64) -> u64 {
        (v * self.target) as u64
    }
}

struct CachedWriteV1 {
    exchange: String,
    threshold_to_write: usize,
    precision: PrecisionF64,
    repository: monitoring::RepositoryV2,
    last_prices: HashMap<String, Option<f64>>,
    list_to_write: Vec::<monitoring::InsertParamV2>,
}

impl CachedWriteV1 {
    pub fn new(
        exchange: String,
        repository: monitoring::RepositoryV2,
        streams: &Vec<String>,
    ) -> Self {
        let threshold_to_write = 1000;
        let precision = PrecisionF64::default();
        let last_prices = streams
            .iter()
            .map(|stream| (stream.clone(), None))
            .collect::<HashMap<String, Option<f64>>>();
        let list_to_write = Vec::<monitoring::InsertParamV2>::new();
        Self {
            exchange,
            threshold_to_write,
            precision,
            repository,
            last_prices,
            list_to_write,
        }
    }

    pub fn tick(
        &mut self,
        updated_stream: String,
        timestamp: u64,
        cache: &HashMap<String, OrderBookCache>,
        symbol_from_stream: impl Fn(&String) -> String,
    ) {
        let last_bid_opt = cache[&updated_stream].get_last_bid();
        let last_ask_opt = cache[&updated_stream].get_last_ask();
        if last_bid_opt.is_none() || last_ask_opt.is_none() {
            return;
        }
        let last_bid = last_bid_opt.unwrap()[0];
        let last_ask = last_ask_opt.unwrap()[0];
        // market price
        let next_price = last_ask + (last_bid - last_ask) / 2.0;
        let prev_price_opt = self.last_prices[&updated_stream];
        if prev_price_opt.is_none() {
            self.last_prices.insert(updated_stream, Some(next_price));
            return;
        }
        let prev_price = self.last_prices[&updated_stream].unwrap();
        if (prev_price - next_price).abs() < self.precision.diff {
            return;
        }
        let content = monitoring::InsertParam{
            symbol: symbol_from_stream(&updated_stream),
            exchange: self.exchange.clone(),
            last_bid: self.precision.f64_to_u64(last_bid),
            last_ask: self.precision.f64_to_u64(last_ask),
        };
        self.list_to_write.push(monitoring::InsertParamV2{content, timestamp});
        log::debug!(
            "[{}] price updated! stream: {}, \
            last_bid: {}, last_ask: {}, \
            obj to write: {:?}",
            self.exchange, updated_stream,
            last_bid, last_ask,
            self.list_to_write.last().unwrap(),
        );
        self.last_prices.insert(updated_stream, Some(next_price));
        if self.list_to_write.len() != self.threshold_to_write {
            return;
        }
        log::info!(
            "[{}][handle_write] write to table, len: {}",
            self.exchange, self.list_to_write.len(),
        );
        self.repository.insert_batch(&self.list_to_write).unwrap();
        self.list_to_write.clear();
    }
}

struct CachedWriteV2 {
    exchange: String,
    threshold_to_write: usize,
    repository: monitoring::RepositoryV2Ticker,
    list_to_write: Vec<monitoring::InsertParamV2Ticker>,
}

impl CachedWriteV2 {
    pub fn new(
        exchange: String,
        repository: monitoring::RepositoryV2Ticker,
    ) -> Self {
        let threshold_to_write = 1000;
        let list_to_write= Vec::<monitoring::InsertParamV2Ticker>::new();
        Self { exchange, threshold_to_write, repository, list_to_write }
    }

    pub fn tick(&mut self, content: monitoring::InsertParamV2Ticker) {
        self.list_to_write.push(content);
        if self.list_to_write.len() != self.threshold_to_write {
            return;
        }
        log::info!(
            "[{}][handle_write] write to table, len: {}",
            self.exchange, self.list_to_write.len(),
        );
        self.repository.insert_batch(&self.list_to_write).unwrap();
        self.list_to_write.clear();
    }
}

/// Shows/writes binance symbols spreads
pub fn monitor_v1_binance(
    symbols_amount: Option<usize>,
    write_needed: bool,
    show_binance: bool,
) {
    let streams = {
        let amount = match symbols_amount {
            Some(v) => v,
            None => SYMBOLS_BINANCE.len(),
        };
        let mut out = SYMBOLS_BINANCE[0..amount].iter()
            .map(|symbol| format!("{}@depth@100ms", symbol.to_lowercase()))
            .collect::<Vec<String>>();
        out.sort();
        out
    };

    let caches = Arc::new(Mutex::new(
        streams.iter()
            .map(|stream| (stream.clone(), OrderBookCache::new()))
            .collect::<HashMap<String, OrderBookCache>>()
    ));
    let (tx, rx) = mpsc::channel();

    let caches_listen = caches.clone();
    let caches_print = caches.clone();
    let streams_show = streams.clone();
    let streams_write = streams.clone();

    let handle_listen = move || {
        let on_message = |msg_obj: binance::Depth| {
            caches_listen.lock()
                .unwrap()
                .get_mut(&msg_obj.stream.clone())
                .unwrap()
                .apply_orders(
                    msg_obj.data.u,
                    &msg_obj.data.b,
                    &msg_obj.data.a,
                );
            match tx.send((msg_obj.stream, msg_obj.data.E)) {
                Ok(_) => {}
                Err(e) => log::error!("[binance][listen] error: {}", e)
            }
        };
        binance::TradingWs::listen_depth(&streams, on_message);
    };
    let handle_show = move || loop {
        thread::sleep(Duration::from_millis(1000));
        let cache = caches_print.lock().unwrap();
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row![
            "stream",
            "last_bid",
            "last_ask",
            "market_price",
            "diff_absolute",
            "diff_relative",
        ]);
        for symbol in streams_show.iter() {
            if let (Some(last_bid), Some(last_ask)) = (
                cache[symbol].get_last_bid(),
                cache[symbol].get_last_ask(),
            ) {
                let market_price = last_bid[0] + (last_ask[0] - last_bid[0]) / 2.0;
                let diff_absolute = last_ask[0] - last_bid[0];
                let diff_relative = diff_absolute / market_price;
                table.add_row(row![
                    symbol,
                    format!("{:.2}", last_bid[0]),
                    format!("{:.2}", last_ask[0]),
                    format!("{:.2}", market_price),
                    format!("{:.2}", diff_absolute),
                    format!("{:.6}", diff_relative),
                ]);
            } else {
                table.add_row(row![symbol, "-", "-", "-", "-", "-"]);
            }
        }
        print!("{esc}[2J{esc}[1;10H\n", esc = 27 as char);
        table.printstd();
    };
    let handle_write = move || {
        let symbol_from_stream = |stream: &String| stream
            .split("@")
            .collect::<Vec<&str>>()
            .first()
            .unwrap()
            .to_uppercase();
        let mut cached_write = CachedWriteV1::new(
            String::from("binance"),
            monitoring::RepositoryV2::new_and_connect(),
            &streams_write,
        );
        loop {
            let (updated_stream, timestamp) = rx.recv().unwrap();
            let cache = &caches.lock().unwrap();
            cached_write.tick(updated_stream, timestamp, cache, symbol_from_stream);
        }
    };

    let mut threads = vec![thread::spawn(handle_listen)];
    if show_binance {
        threads.push(thread::spawn(handle_show));
    }
    if write_needed {
        threads.push(thread::spawn(handle_write));
    }

    pool(&threads);
}

/// Shows/writes gateio symbols spreads
pub fn monitor_v1_gateio(
    symbols_amount: Option<usize>,
    write_needed: bool,
) {
    let symbols: Vec<String> = {
        let amount = match symbols_amount {
            Some(v) => v,
            None => SYMBOLS_GATEIO.len(),
        };
        let mut out = SYMBOLS_GATEIO[0..amount].iter()
            .map(|symbol| symbol.to_string())
            .collect::<Vec<String>>();
        out.sort();
        out
    };
    let caches = Arc::new(Mutex::new(
        symbols.clone().iter()
            .map(|symbol| (symbol.to_string(), OrderBookCache::new()))
            .collect::<HashMap<String, OrderBookCache>>()
    ));
    let (tx, rx) = mpsc::channel();

    let caches_listen = caches.clone();
    let symbols_write = symbols.clone();

    let handle_listen = move || {
        let on_message = |msg_obj: gateio::Depth| {
            caches_listen.lock()
                .unwrap()
                .get_mut(&msg_obj.result.s)
                .unwrap()
                .apply_orders(
                    msg_obj.result.u,
                    &msg_obj.result.b,
                    &msg_obj.result.a,
                );
            match tx.send((msg_obj.result.s, msg_obj.result.t)) {
                Ok(_) => {}
                Err(e) => log::error!("[gateio][listen] error: {}", e)
            }
        };
        gateio::TradingWs::listen_depth(&symbols, on_message);
    };
    let handle_write = move || {
        // XXX: do pause at the beginning to avoid fancy prices in table
        let symbol_from_stream = |stream: &String| stream
            .replace("_", "")
            .to_uppercase();
        let mut cached_write = CachedWriteV1::new(
            String::from("gateio"),
            monitoring::RepositoryV2::new_and_connect(),
            &symbols_write,
        );
        loop {
            let (updated_stream, timestamp) = rx.recv().unwrap();
            let cache = &caches.lock().unwrap();
            cached_write.tick(updated_stream, timestamp, cache, symbol_from_stream);
        }
    };

    let mut threads = vec![thread::spawn(handle_listen)];
    if write_needed {
        threads.push(thread::spawn(handle_write));
    }

    pool(&threads);
}

fn monitor_v1(args: &Args) {
    let args_binance = args.clone();
    let args_gateio = args.clone();
    pool(&vec![
        thread::spawn(move || monitor_v1_binance(
            args_binance.symbols_amount,
            args_binance.write_needed,
            args_binance.show_binance,
        )),
        thread::spawn(move || monitor_v1_gateio(
            args_gateio.symbols_amount,
            args_gateio.write_needed,
        )),
    ]);
}

fn monitor_v2_binance() {
    let (tx, rx) = mpsc::channel();

    let handle_listen = move || {
        let precision = PrecisionF64::default();
        let on_message = |msg_obj: Vec<binance::MiniTicker>| {
            for ticker in msg_obj.iter() {
                let content = monitoring::InsertParamV2Ticker {
                    symbol: ticker.s.clone(),
                    exchange: "binance".to_string(),
                    close_price: precision.str_to_u64(&ticker.c),
                    high_price: precision.str_to_u64(&ticker.h),
                    low_price: precision.str_to_u64(&ticker.l),
                    timestamp: ticker.E,
                };
                tx.send(content).unwrap();
            }
        };
        binance::TradingWs::listen_mini_tickers_arr(on_message);
    };
    let handle_write = move || {
        let mut cached_write = CachedWriteV2::new(
            String::from("binance"),
            monitoring::RepositoryV2Ticker::new_and_connect(),
        );
        loop {
            let content = rx.recv().unwrap();
            cached_write.tick(content);
        }
    };

    pool(&vec![
        thread::spawn(handle_listen),
        thread::spawn(handle_write),
    ]);
}

fn monitor_v2_gateio() {
    let (tx, rx) = mpsc::channel();

    let handle_listen = move || {
        let precision = PrecisionF64::default();
        let on_message = |ticker: gateio::Ticker| {
            let symbol = ticker.result.currency_pair
                .replace("_", "")
                .to_uppercase();
            let param = monitoring::InsertParamV2Ticker {
                symbol,
                exchange: "gateio".to_string(),
                close_price: precision.str_to_u64(&ticker.result.last),
                high_price: precision.str_to_u64(&ticker.result.highest_bid),
                low_price: precision.str_to_u64(&ticker.result.lowest_ask),
                timestamp: ticker.time_ms,
            };
            tx.send(param).unwrap();
        };
        let symbols = SYMBOLS_GATEIO.iter()
            .map(|x| x.clone())
            .collect::<Vec<&str>>();
        gateio::TradingWs::listen_mini_tickers(&symbols, on_message);
    };
    let handle_write = move || {
        let mut cached_write = CachedWriteV2::new(
            String::from("gateio"),
            monitoring::RepositoryV2Ticker::new_and_connect(),
        );
        loop {
            let content = rx.recv().unwrap();
            cached_write.tick(content);
        }
    };

    pool(&vec![
        thread::spawn(handle_listen),
        thread::spawn(handle_write),
    ]);
}

fn monitor_v2(_args: &Args) {
    pool(&vec![
        thread::spawn(move || monitor_v2_binance()),
        thread::spawn(move || monitor_v2_gateio()),
    ]);
}

fn put_postgres_data_to_druid() {
    let topic = "monitor-exchanges";

    let mut repository = monitoring::Repository::new_and_connect();
    let mut producer = queue::Producer::new_and_connect();

    for row in repository.symbols_amount().unwrap() {
        let symbol = row.get::<usize, String>(0);
        let amount = row.get::<usize, i64>(1);
        let mut messages = Vec::new();
        log::info!("process symbol {}, amount: {}", symbol, amount);
        for row_target in repository.symbol_raw(&symbol).unwrap() {
            let symbol = row_target.get::<usize, String>(0);
            let exchange = row_target.get::<usize, String>(1);
            let last_bid = row_target.get::<usize, i32>(2) as u64;
            let last_ask = row_target.get::<usize, i32>(3) as u64;
            let timestamp = row_target.get::<usize, SystemTime>(4)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            messages.push(serde_json::json!({
                "symbol": symbol,
                "exchange": exchange,
                "last_bid": last_bid,
                "last_ask": last_ask,
                "timestamp": timestamp,
            }).to_string());
        }
        producer.send_all(topic, &messages).unwrap();
        repository.delete_raw(&symbol).unwrap()
    }
}

fn generate_mat_views() {
    let interval = chrono::Duration::hours(4);
    let mut repository = monitoring::Repository::new_and_connect();
    let (mut start, end) = repository.start_end_dates().unwrap();
    log::info!("start: {:?}, end: {:?}", start, end);
    let to_strict_millis = {
        start.timestamp_millis() % 1000 +
            (start.second() * 1000) as i64 +
            (start.minute() * 1000 * 60)  as i64
    };
    start.add_assign(chrono::Duration::milliseconds(-to_strict_millis));
    while start.lt(&end) && {
        let seconds_till_now = Utc::now().timestamp() - start.timestamp();
        seconds_till_now > interval.num_hours() * 60 * 60
    } {
        repository.spread_mat_view(&start, &start.add(interval));
        start.add_assign(interval);
    }
}

fn main() {
    env_logger::init();

    let args = Args::new_and_parse();
    match args.command.as_str() {
        "v1" => monitor_v1(&args),
        "v2" => monitor_v2(&args),
        "generate-mat-views" => generate_mat_views(),
        "put-postgres-data-to-druid" => put_postgres_data_to_druid(),
        _ => panic!("command not found"),
    }
}
