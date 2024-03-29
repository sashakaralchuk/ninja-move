use std::sync::mpsc;
use std::thread;

use exchanges_arbitrage::domain::bybit::{self, TradingHttpTrait};
use exchanges_arbitrage::port::two_top_intersects::{
    CandlesPort, HistoryPort, HistoryRow,
};
use exchanges_arbitrage::{pool, Args, Candle, FlatTicker};

#[derive(Debug)]
struct TwoTopSignal {
    high_value: f64,
}

struct TwoTopIntersection {
    high_values: Vec<f64>,
}

impl TwoTopIntersection {
    fn new() -> Self {
        // XXX: implement aka ring buffer here
        let high_values = Vec::new();
        Self { high_values }
    }

    fn apply_candle(&mut self, candle: &Candle) {
        while !self.high_values.is_empty()
            && self.high_values.last().unwrap() < &candle.high
        {
            self.high_values.pop();
        }
        self.high_values.push(candle.high);
    }

    fn fire(&mut self, ticker_price: f64) -> Option<TwoTopSignal> {
        if !self.high_values.is_empty()
            && ticker_price > *self.high_values.last().unwrap()
        {
            let high_value = self.high_values.pop().unwrap();
            return Some(TwoTopSignal { high_value });
        }
        None
    }

    fn log_hist(
        port: &mut HistoryPort,
        cause: &str,
        threshold: f64,
        order: &bybit::BybitOrder,
        ticker: &FlatTicker,
    ) {
        let profit_abs = threshold - order.avg_fill_price;
        let profit_rel = profit_abs / order.avg_fill_price;
        let hist = HistoryRow::new(
            cause,
            &ticker.exchange,
            &ticker.data_symbol,
            order.avg_fill_price,
            threshold,
            profit_abs,
            profit_rel,
            ticker.data_last_price,
        );
        port.insert_hist(&hist).unwrap();
        log::debug!(
            "close order open_price={} trailing_threshold={} \
    profit_abs={:.2} profit_rel={:.4}",
            order.avg_fill_price,
            threshold,
            profit_abs,
            profit_rel,
        );
    }
}

fn trade() {
    let symbol = "BTCUSDC".to_string();
    let symbol_receive = symbol.clone();
    let symbol_calc = symbol.clone();
    let symbol_trade = symbol.clone();
    let (tx_tickers_calc_signals, rx_tickers_calc_signals) = mpsc::channel();
    let (tx_tickers_trade_signals, rx_tickers_trade_signals) = mpsc::channel();
    let (tx_calc_trade_signals, rx_calc_trade_signals) = mpsc::channel();
    let receive_tickers = move || {
        let on_message = |m: FlatTicker| {
            tx_tickers_calc_signals.send(m.clone()).unwrap();
            tx_tickers_trade_signals.send(m).unwrap();
        };
        bybit::TradingWs::listen_tickers(on_message, &symbol_receive);
    };
    let calc_signals =
        move || {
            let mut two_top_intersection = TwoTopIntersection::new();
            let prev_candles = CandlesPort::new_and_connect()
                .fetch_last_candles(200, "bybit", symbol_calc.as_str());
            for candle in prev_candles.iter() {
                two_top_intersection.apply_candle(&candle)
            }
            let start_ticker = Candle::wait_for_next(&rx_tickers_calc_signals);
            let mut current_candle = Candle::new_from_ticker(&start_ticker);
            loop {
                let ticker = rx_tickers_calc_signals.recv().unwrap();
                if current_candle.expired() {
                    log::debug!("candle expired {:?}", current_candle);
                    two_top_intersection.apply_candle(&current_candle);
                    current_candle = Candle::new_from_ticker(&ticker);
                } else {
                    current_candle.apply_ticker(&ticker)
                }
                match two_top_intersection.fire(ticker.data_last_price) {
                    Some(signal) => tx_calc_trade_signals.send(signal).unwrap(),
                    None => {}
                }
            }
        };
    let trade_signals = move || {
        let mut history_port = HistoryPort::new_and_connect();
        let trading = bybit::TradingHttpDebug::new_from_envs();
        loop {
            // XXX: use single message(one shot) channels
            let signal = {
                let mut signal = None;
                while let Ok(t) = rx_calc_trade_signals.try_recv() {
                    signal = Some(t);
                }
                signal.unwrap_or(rx_calc_trade_signals.recv().unwrap())
            };
            let last_ticker = {
                let mut ticker = None;
                while let Ok(t) = rx_tickers_trade_signals.try_recv() {
                    ticker = Some(t);
                }
                ticker.unwrap_or(rx_tickers_trade_signals.recv().unwrap())
            };
            log::info!(
                "trade, issued on price {}, last ticker price is {}",
                signal.high_value,
                last_ticker.data_last_price
            );
            let order = trading
                .place_market_order(0.0, symbol_trade.as_str(), "Buy")
                .unwrap();
            let n = 10;
            log::info!("wait {} tickers for price move", n);
            // TODO: think about how and when to start looking for closing an order
            for _ in 0..n {
                rx_tickers_trade_signals.recv().unwrap();
            }
            let stop_loss_rel_val = 0.005; // TODO: calc this on prev data
            let trailing_rel_val = 0.5;
            let mut trailing_threshold = None;
            loop {
                let ticker = rx_tickers_trade_signals.recv().unwrap();
                let bottom_threshold =
                    order.avg_fill_price * (1.0 - stop_loss_rel_val);
                if ticker.data_last_price <= bottom_threshold {
                    TwoTopIntersection::log_hist(
                        &mut history_port,
                        "reach-stop-loss",
                        bottom_threshold,
                        &order,
                        &ticker,
                    );
                    trading
                        .place_market_order(0.0, symbol_trade.as_str(), "Sell")
                        .unwrap();
                    break;
                }
                if ticker.data_last_price <= order.avg_fill_price {
                    continue;
                }
                let next_threshold = f64::max(
                    order.avg_fill_price
                        + (ticker.data_last_price - order.avg_fill_price)
                            * trailing_rel_val,
                    trailing_threshold.unwrap_or(0.0),
                );
                if trailing_threshold.is_none() {
                    log::debug!("initialize threshold with {}", next_threshold);
                    trailing_threshold = Some(next_threshold);
                    continue;
                }
                if ticker.data_last_price < trailing_threshold.unwrap() {
                    TwoTopIntersection::log_hist(
                        &mut history_port,
                        "reach-trailing-stop",
                        trailing_threshold.unwrap(),
                        &order,
                        &ticker,
                    );
                    trading
                        .place_market_order(0.0, symbol_trade.as_str(), "Sell")
                        .unwrap();
                    break;
                }
                log::debug!("moving threshold to new pos {}", next_threshold);
                trailing_threshold = Some(next_threshold);
            }
        }
    };
    pool(&vec![
        thread::spawn(receive_tickers),
        thread::spawn(calc_signals),
        thread::spawn(trade_signals),
    ]);
}

fn create_tables() {
    CandlesPort::new_and_connect().create_table();
    HistoryPort::new_and_connect().create_table();
    log::info!("database tables created");
}

fn listen_save_candles() {
    // XXX: re-write without useless thread
    //      p.s. error "expected a closure that implements
    //      the `Fn` trait, but this closure only implements
    //      `FnMut`" must be handled
    let symbol = "BTCUSDC".to_string();
    let symbol_receive = symbol.clone();
    let symbol_calc = symbol.clone();
    let (tx, rx) = mpsc::channel();
    let receive_tickers = move || {
        let on_message = |m: FlatTicker| {
            tx.send(m.clone()).unwrap();
        };
        bybit::TradingWs::listen_tickers(on_message, &symbol_receive);
    };
    let calc_signals = move || {
        let mut port = CandlesPort::new_and_connect();
        let mut current_candle = Candle::new_from_ticker(&FlatTicker {
            ts: 0,
            data_symbol: symbol_calc,
            data_last_price: 0.0,
            exchange: "bybit".to_string(),
        });
        loop {
            let ticker = rx.recv().unwrap();
            if current_candle.expired() {
                log::info!("candle expired: {:?}", current_candle);
                current_candle = Candle::new_from_ticker(&ticker);
                port.insert_candle(&current_candle).unwrap();
            } else {
                current_candle.apply_ticker(&ticker)
            }
        }
    };
    pool(&vec![
        thread::spawn(receive_tickers),
        thread::spawn(calc_signals),
    ]);
}

fn main() {
    env_logger::init();
    let args = Args::new_and_parse();
    match args.command.as_str() {
        "trade" => trade(),
        "create-tables" => create_tables(),
        "listen-save-candles" => listen_save_candles(),
        _ => panic!("command not found"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Candle, TwoTopIntersection};

    fn create_candle(high: f64) -> Candle {
        Candle {
            exchange: "".to_string(),
            symbol: "".to_string(),
            open_time: 0,
            low: 0.0,
            close: 0.0,
            open: 0.0,
            high,
        }
    }

    fn l_to_str(l: &Vec<f64>) -> String {
        l.iter()
            .map(|&v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    #[test]
    fn test_2top_intersects_expected_behaviour() {
        let mut two_top_intersection = TwoTopIntersection::new();
        two_top_intersection.apply_candle(&create_candle(70_000_f64));
        two_top_intersection.apply_candle(&create_candle(71_000_f64));
        assert_eq!(l_to_str(&two_top_intersection.high_values), "71000");
        two_top_intersection.apply_candle(&create_candle(70_800_f64));
        two_top_intersection.apply_candle(&create_candle(70_400_f64));
        assert_eq!(
            l_to_str(&two_top_intersection.high_values),
            "71000,70800,70400"
        );
        two_top_intersection.apply_candle(&create_candle(70_600_f64));
        assert_eq!(
            l_to_str(&two_top_intersection.high_values),
            "71000,70800,70600"
        );
        two_top_intersection.apply_candle(&create_candle(70_100_f64));
        two_top_intersection.apply_candle(&create_candle(69000_f64));
        two_top_intersection.apply_candle(&create_candle(68000_f64));
        assert_eq!(
            l_to_str(&two_top_intersection.high_values),
            "71000,70800,70600,70100,69000,68000"
        );
        assert!(two_top_intersection.fire(67_000_f64).is_none());
        assert!(two_top_intersection.fire(68_001_f64).is_some());
    }
}
