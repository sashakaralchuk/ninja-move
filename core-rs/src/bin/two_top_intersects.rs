use std::sync::mpsc;
use std::thread;

use exchanges_arbitrage::domain::bybit::{self, TradingHttpTrait};
use exchanges_arbitrage::port::two_top_intersects::{
    CandlesPort, HistoryPort, HistoryRow, SpreadPort, SpreadRow,
};
use exchanges_arbitrage::{pool, Args, Candle, FlatTicker, OrderBookCache, TelegramBotPort};

#[derive(Debug)]
struct TwoTopSignal {
    high_value: f64,
}

#[derive(Clone)]
enum TwoTopStrategy {
    Default,
    WithSpread,
}

impl TwoTopStrategy {
    fn parse_from_env() -> TwoTopStrategy {
        match std::env::var("TWO_TOP_STRATEGY") {
            Ok(v) => match v.as_str() {
                "default" => TwoTopStrategy::Default,
                "with-spread" => TwoTopStrategy::WithSpread,
                _ => panic!("invalid strategy"),
            },
            Err(_) => panic!("invalid strategy"),
        }
    }
}

struct TwoTopIntersection<'a> {
    strategy: TwoTopStrategy,
    high_values: Vec<f64>,
    history_port: Option<&'a mut HistoryPort>,
    telegram_port: Option<&'a TelegramBotPort>,
}

impl<'a> TwoTopIntersection<'a> {
    fn new(
        strategy: TwoTopStrategy,
        history_port: Option<&'a mut HistoryPort>,
        telegram_port: Option<&'a TelegramBotPort>,
    ) -> Self {
        // XXX: implement aka ring buffer here
        let high_values = Vec::new();
        Self {
            strategy,
            high_values,
            history_port,
            telegram_port,
        }
    }

    fn apply_candle(&mut self, candle: &Candle) {
        while !self.high_values.is_empty() && self.high_values.last().unwrap() < &candle.high {
            self.high_values.pop();
        }
        self.high_values.push(candle.high);
    }

    fn fire(&mut self, ticker_price: f64) -> Option<TwoTopSignal> {
        if !self.high_values.is_empty() && ticker_price > *self.high_values.last().unwrap() {
            let high_value = self.high_values.pop().unwrap();
            return Some(TwoTopSignal { high_value });
        }
        None
    }

    fn log_hist(
        &mut self,
        cause: &str,
        threshold: f64,
        order: &bybit::BybitOrder,
        ticker: &FlatTicker,
    ) {
        let profit_abs = threshold - order.avg_fill_price;
        let profit_rel = profit_abs / order.avg_fill_price;
        let strategy_str = match self.strategy {
            TwoTopStrategy::Default => "two-top-intersection-default",
            TwoTopStrategy::WithSpread => "two-top-intersection-with-spread",
        };
        let hist = HistoryRow::new(
            strategy_str,
            cause,
            &ticker.exchange,
            &ticker.data_symbol,
            order.avg_fill_price,
            threshold,
            profit_abs,
            profit_rel,
            ticker.data_last_price,
        );
        self.history_port
            .as_mut()
            .unwrap()
            .insert_hist(&hist)
            .unwrap();
        let m = format!(
            "close order open_price={} trailing_threshold={} profit_abs={:.2} profit_rel={:.4}",
            order.avg_fill_price, threshold, profit_abs, profit_rel,
        );
        self.telegram_port
            .unwrap()
            .notify_pretty(m.clone(), strategy_str.to_string());
        log::debug!("{}", m);
    }
}

enum Reason {
    Continue,
    ReachStopLoss,
    ReachThrailingStop,
}

struct DecisionOut {
    reason: Reason,
    bottom_threshold: f64,
    trailing_threshold: f64,
}

impl DecisionOut {
    fn new(reason: Reason, bottom_threshold: f64, trailing_threshold: f64) -> Self {
        Self {
            reason,
            bottom_threshold,
            trailing_threshold,
        }
    }
}

struct TrailingThreshold {
    start_price: f64,
    trailing_threshold: Option<f64>,
}

impl TrailingThreshold {
    fn new(start_price: f64) -> Self {
        Self {
            trailing_threshold: None,
            start_price,
        }
    }

    fn apply_and_make_decision(&mut self, ticker: &FlatTicker) -> DecisionOut {
        let stop_loss_rel_val = 0.005; // TODO: calc this on prev data
        let trailing_rel_val = 0.5;
        let bottom_threshold = self.start_price * (1.0 - stop_loss_rel_val);
        if ticker.data_last_price <= bottom_threshold {
            return DecisionOut::new(Reason::ReachStopLoss, bottom_threshold, 0.0);
        }
        if ticker.data_last_price <= self.start_price {
            return DecisionOut::new(Reason::Continue, 0.0, 0.0);
        }
        let next_threshold =
            self.start_price + (ticker.data_last_price - self.start_price) * trailing_rel_val;
        if self.trailing_threshold.is_none() {
            log::debug!("initialize threshold with {}", next_threshold);
            self.trailing_threshold = Some(next_threshold);
            return DecisionOut::new(Reason::Continue, 0.0, 0.0);
        }
        if ticker.data_last_price < self.trailing_threshold.unwrap() {
            return DecisionOut::new(
                Reason::ReachThrailingStop,
                0.0,
                self.trailing_threshold.unwrap(),
            );
        }
        log::debug!(
            "moving threshold to new pos {}, start_price={}",
            next_threshold,
            self.start_price
        );
        self.trailing_threshold = Some(next_threshold);
        return DecisionOut::new(Reason::Continue, 0.0, 0.0);
    }
}

fn trade() {
    let symbol = "BTCUSDC".to_string();
    let symbol_receive = symbol.clone();
    let symbol_calc = symbol.clone();
    let symbol_trade = symbol.clone();
    let two_top_strategy = TwoTopStrategy::parse_from_env();
    let two_top_strategy_calc = two_top_strategy.clone();
    let two_top_strategy_trade = two_top_strategy.clone();
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
    let calc_signals = move || {
        let mut two_top_intersection = TwoTopIntersection::new(two_top_strategy_calc, None, None);
        let prev_candles =
            CandlesPort::new_and_connect().fetch_last_candles(200, "bybit", symbol_calc.as_str());
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
        let _telegram_port = TelegramBotPort::new_from_envs();
        let mut two_top_intersection =
            TwoTopIntersection::new(two_top_strategy_trade, Some(&mut history_port), None);
        let trading = bybit::TradingHttpDebug::new_from_envs();
        loop {
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
            let order = {
                let mut o = trading
                    .place_market_order(0.0, symbol_trade.as_str(), "Buy")
                    .unwrap();
                // NOTE: debug-purpose
                o.avg_fill_price = last_ticker.data_last_price;
                o
            };
            {
                let n = 10;
                log::info!("wait {} tickers for price move", n);
                // TODO: think about how and when to start looking for closing an order
                for _ in 0..n {
                    rx_tickers_trade_signals.recv().unwrap();
                }
            };
            let mut threshold = TrailingThreshold::new(order.avg_fill_price);
            loop {
                let ticker = rx_tickers_trade_signals.recv().unwrap();
                let decision_out = threshold.apply_and_make_decision(&ticker);
                match decision_out.reason {
                    Reason::ReachStopLoss => {
                        two_top_intersection.log_hist(
                            "reach-stop-loss",
                            decision_out.bottom_threshold,
                            &order,
                            &ticker,
                        );
                        trading
                            .place_market_order(0.0, symbol_trade.as_str(), "Sell")
                            .unwrap();
                        break;
                    }
                    Reason::ReachThrailingStop => {
                        two_top_intersection.log_hist(
                            "reach-trailing-stop",
                            decision_out.trailing_threshold,
                            &order,
                            &ticker,
                        );
                        trading
                            .place_market_order(0.0, symbol_trade.as_str(), "Sell")
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
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
    SpreadPort::new_and_connect().create_table();
    log::info!("database tables created");
}

fn listen_save_candles() {
    let symbol = "BTCUSDC".to_string();
    let mut candles_port = CandlesPort::new_and_connect();
    let mut current_candle = Candle::new_from_ticker(&FlatTicker {
        ts: 0,
        data_symbol: symbol.clone(),
        data_last_price: 0.0,
        exchange: "bybit".to_string(),
    });
    let on_message = |event: bybit::EventWs| match event {
        bybit::EventWs::Ticker(ticker) => {
            if current_candle.expired() {
                log::info!("candle expired: {:?}", current_candle);
                current_candle = Candle::new_from_ticker(&ticker);
                candles_port.insert_candle(&current_candle).unwrap();
            } else {
                current_candle.apply_ticker(&ticker)
            }
        }
        _ => {}
    };
    let config = bybit::ConfigWs::new(symbol, false, true);
    bybit::TradingWs::listen_ws(&config, on_message);
}

fn listen_save_spreads() {
    let symbol = "BTCUSDC".to_string();
    let mut order_book = OrderBookCache::new();
    let mut spread_port = SpreadPort::new_and_connect();
    let on_message = |event: bybit::EventWs| match event {
        bybit::EventWs::Depth(depth) => {
            order_book.apply_orders(depth.update_id, &depth.bids, &depth.asks);
            if let Some(spread_abs) = order_book.calc_spread_abs() {
                let row = SpreadRow::new(&"bybit".to_string(), &symbol, spread_abs);
                spread_port.insert_spread(&row).unwrap();
            }
        }
        _ => {}
    };
    let config = bybit::ConfigWs::new(symbol.clone(), true, false);
    bybit::TradingWs::listen_ws(&config, on_message);
}

fn main() {
    env_logger::init();
    let args = Args::new_and_parse();
    match args.command.as_str() {
        "trade" => trade(),
        "create-tables" => create_tables(),
        "listen-save-candles" => listen_save_candles(),
        "listen-save-spreads" => listen_save_spreads(),
        _ => panic!("command not found"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Candle, TwoTopIntersection, TwoTopStrategy};

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
    fn test_2top_intersects_default_positive_behaviour() {
        let mut two_top_intersection = TwoTopIntersection::new(TwoTopStrategy::Default, None, None);
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

    // #[test]
    // fn test_trailing_threshold_default_positive_behaviour() {}

    // #[test]
    // fn test_trailing_threshold_with_spread_positive_behaviour() {}
}
