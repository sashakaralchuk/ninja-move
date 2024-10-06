use clap::Parser;

#[tokio::main]
async fn main() {
    env_logger::init();
    let run_args = RunArgs::parse();
    match run_args.command.as_str() {
        "run-backtest" => trade::run_backtest().await,
        "run-trading" => trade::run_trading(),
        "debug-trades-download-write" => debug::trades_download_write(),
        "debug-trades-read-agg-write" => debug::trades_read_agg_write(),
        "debug-draw-chart-on-klines" => debug::draw_chart_on_klines(None),
        "debug-listen-save-bybit-tickers" => debug::listen_save_bybit_tickers(),
        "debug-read-calc-write-emas" => debug::read_calc_write_emas(),
        "debug-read-save-tickers-to-database" => debug::read_save_tickers_to_database(),
        "debug-calc-2024-04-07-candles" => debug::calc_2024_04_07_candles(),
        _ => log::error!("unknown command"),
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct RunArgs {
    #[arg(short, long, help = "Command to run")]
    command: String,
}

mod trade {
    use exchanges_arbitrage::{
        Candle, CandleTimeframe, FlatTicker, TradesTPortClickhouse, TrailingThreshold,
        TrailingThresholdReason,
    };

    pub fn run_trading() {
        unimplemented!()
    }

    pub async fn run_backtest() {
        let config = BacktestConfig::new_from_envs();
        log::info!(
            "fetch tickers for [{}, {}]",
            config.start_timestamp.to_string(),
            config.end_timestamp.to_string()
        );
        let tickers = TradesTPortClickhouse::new_and_connect()
            .fetch(
                &config.start_timestamp.to_string(),
                &config.end_timestamp.to_string(),
            )
            .await;
        log::info!(
            "divide tickers on (hist, backtest) tickers.len={}",
            tickers.len()
        );
        let mut hist_tickers = vec![];
        let mut backtest_tickers = vec![];
        for ticker in tickers.iter() {
            if ticker.ts_millis <= config.trading_start_timestamp.and_utc().timestamp_millis() {
                hist_tickers.push(ticker.clone());
            } else {
                backtest_tickers.push(ticker.clone())
            }
        }
        let mut debug_candles = vec![];
        let mut current_candle =
            Candle::new_from_ticker(&hist_tickers[0], CandleTimeframe::Hours(1));
        log::info!(
            "fill hist state (current_candle, strategy) hist_tickers.len={}",
            hist_tickers.len(),
        );
        let mut strategy = {
            let mut strategy_int = StrategyEmas::new();
            for ticker in hist_tickers {
                if current_candle.expired(&ticker) {
                    strategy_int.apply_candle(&current_candle);
                    if config.save_debug_candles {
                        debug_candles.push(current_candle);
                    }
                    current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Hours(1));
                } else {
                    current_candle.apply_ticker(&ticker)
                }
            }
            strategy_int
        };
        let mut backtests = vec![];
        let mut debug_emas = vec![];
        let mut threshold: Option<TrailingThreshold> = None;
        log::info!(
            "run backtest backtest_tickers.len()={}",
            backtest_tickers.len()
        );
        for ticker in backtest_tickers {
            if current_candle.expired(&ticker) {
                log::debug!("candle expired {:?}", current_candle);
                strategy.apply_candle(&current_candle);
                if config.save_debug_candles {
                    debug_candles.push(current_candle);
                }
                if config.save_debug_emas {
                    debug_emas.push(strategy.calc_ema());
                }
                current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Hours(1));
            } else {
                current_candle.apply_ticker(&ticker)
            }
            match threshold {
                Some(_) => match threshold.as_mut().unwrap().apply_and_make_decision(&ticker) {
                    TrailingThresholdReason::ReachStopLoss(bottom_threshold) => {
                        if config.save_backtest_outs {
                            let t = threshold.unwrap();
                            let b = serde_json::json!({
                                "open_price": t.open_price,
                                "open_timestamp_millis": t.open_timestamp_millis,
                                "close_timestamp_millis": ticker.ts_millis,
                                "bottom_threshold": bottom_threshold,
                                "reason": "reach-stop-loss",
                            })
                            .to_string();
                            backtests.push(b);
                        }
                        threshold = None;
                    }
                    TrailingThresholdReason::ReachThrailingStop(trailing_threshold) => {
                        if config.save_backtest_outs {
                            let t = threshold.unwrap();
                            let b = serde_json::json!({
                                "open_price": t.open_price,
                                "open_timestamp_millis": t.open_timestamp_millis,
                                "close_timestamp_millis": ticker.ts_millis,
                                "trailing_threshold": trailing_threshold,
                                "reason": "reach-thrailing-stop",
                            })
                            .to_string();
                            backtests.push(b);
                        }
                        threshold = None;
                    }
                    TrailingThresholdReason::Continue => {}
                },
                None => match strategy.fire_with_res(&ticker) {
                    Ok(_) => {
                        threshold = Some(TrailingThreshold::new(
                            ticker.data_last_price,
                            ticker.ts_millis,
                        ))
                    }
                    Err(_) => {}
                },
            }
        }
        // TODO: workout how librdkafka works (and why redpanda problem exists? not enought disk? what's the approach to write in parallel data to queue?)
        // TODO: speed up loading of tickers
        // TODO: read matplotlib and mplfinance doc
        // TODO: what's rust iter() diff with into_iter() and what are the ways to iterate through array
        // TODO: what are ref and deref
        // TODO: use https://github.com/rust-lang/rust-clippy
        let debugs_port = exchanges_arbitrage::DebugsPortClickhouse::new_and_connect();
        {
            let content = serde_json::json!({
                "debug_candles.len()": debug_candles.len(),
                "backtests.len()": backtests.len(),
                // TODO: save run info to mlflow
                "config": serde_json::to_string(&config).unwrap(),
            })
            .to_string();
            log::info!("finish run_id={} content={}", config.run_id, content);
            let row = exchanges_arbitrage::DebugsRow {
                run_id: config.run_id.clone(),
                kind: "run-info".into(),
                content,
            };
            let _ = debugs_port.insert_batch(&vec![row]).await;
        }
        if config.save_debug_candles && debug_candles.len() > 0 {
            log::info!("save debug_candles to default.debugs");
            let vec = debug_candles
                .iter()
                .map(|x| exchanges_arbitrage::DebugsRow {
                    run_id: config.run_id.clone(),
                    kind: "candle".into(),
                    content: x.to_json_string(),
                })
                .collect::<Vec<_>>();
            let _ = debugs_port.insert_batch(&vec).await;
        }
        if config.save_backtest_outs && backtests.len() > 0 {
            log::info!("save backtests to default.debugs");
            let vec = backtests
                .iter()
                .map(|x| exchanges_arbitrage::DebugsRow {
                    run_id: config.run_id.clone(),
                    kind: "backtest".into(),
                    content: x.into(),
                })
                .collect::<Vec<_>>();
            let _ = debugs_port.insert_batch(&vec).await;
        }
        if config.save_debug_emas && debug_emas.len() > 0 {
            log::info!("save debug_emas to default.debugs");
            let vec = debug_emas
                .iter()
                .map(|x| exchanges_arbitrage::DebugsRow {
                    run_id: config.run_id.clone(),
                    kind: "ema".into(),
                    content: x.to_string(),
                })
                .collect::<Vec<_>>();
            let _ = debugs_port.insert_batch(&vec).await;
        }
    }

    #[derive(Debug, serde::Serialize)]
    struct BacktestConfig {
        save_backtest_outs: bool,
        save_debug_candles: bool,
        save_debug_emas: bool,
        run_id: String,
        commit_hash: String,
        start_timestamp: chrono::NaiveDateTime,
        end_timestamp: chrono::NaiveDateTime,
        trading_start_timestamp: chrono::NaiveDateTime,
    }

    impl BacktestConfig {
        fn new_from_envs() -> Self {
            let save_backtest_outs =
                Self::parse_bool_from_int("TRADE_EMAS_BACKTEST_SAVE_BACKTESTS_OUTS");
            let save_debug_candles =
                Self::parse_bool_from_int("TRADE_EMAS_BACKTEST_SAVE_DEBUG_CANDLES");
            let save_debug_emas = Self::parse_bool_from_int("TRADE_EMAS_BACKTEST_SAVE_DEBUG_EMAS");
            let run_id = uuid::Uuid::new_v4().to_string();
            let commit_hash = std::env::var("COMMIT_HASH_STR").unwrap();
            let start_timestamp = Self::parse_timestamp("TRADE_EMAS_BACKTEST_START_TIMESTAMP");
            let end_timestamp = Self::parse_timestamp("TRADE_EMAS_BACKTEST_END_TIMESTAMP");
            let trading_start_timestamp =
                Self::parse_timestamp("TRADE_EMAS_BACKTEST_TRADING_START_TIMESTAMP");
            Self {
                save_backtest_outs,
                save_debug_candles,
                save_debug_emas,
                run_id,
                commit_hash,
                start_timestamp,
                end_timestamp,
                trading_start_timestamp,
            }
        }

        fn parse_timestamp(key: &str) -> chrono::NaiveDateTime {
            chrono::NaiveDateTime::parse_from_str(&std::env::var(key).unwrap(), "%Y-%m-%d %H:%M:%S")
                .unwrap()
        }

        fn parse_bool_from_int(key: &str) -> bool {
            std::env::var(key).unwrap().parse::<u8>().unwrap() > 0
        }
    }

    struct RingBuffer {
        p: i32,
        list: Vec<f64>,
    }

    impl RingBuffer {
        fn new(size: usize) -> Self {
            let mut list = Vec::with_capacity(size);
            unsafe {
                list.set_len(size);
            }
            Self { p: -1, list }
        }

        fn add(&mut self, n: f64) {
            let i = (self.p + 1) % self.list.len() as i32;
            self.p = i;
            self.list[i as usize] = n;
        }

        fn all(&self) -> Vec<f64> {
            let mut out = vec![];
            let mut i = (self.p + 1) % self.list.len() as i32;
            for _ in 0..self.list.len() {
                out.push(self.list[i as usize]);
                i = (i + 1) % self.list.len() as i32;
            }
            out.into_iter().filter(|x| *x != 0.0).collect()
        }
    }

    ///
    /// Calcs the same way as pd.Series(...).ewm(span=len, adjust=False).mean()
    ///
    pub fn calc_emas(prices: &Vec<f64>, len: i32) -> Vec<f64> {
        let len_f64 = len as f64;
        let smoothing = 2.0;
        let mut emas = vec![prices[0]];
        for i in 1..prices.len() {
            let k = smoothing / (1.0 + len_f64);
            let ema = prices[i] * k + *emas.last().unwrap() * (1.0 - k);
            emas.push(ema);
        }
        emas
    }

    struct StrategyEmas {
        ema_len: usize,
        prices_len: usize,
        prices: RingBuffer,
    }

    impl StrategyEmas {
        fn new() -> Self {
            let ema_len = 12;
            let prices_len = ema_len + 30; // 12 means ema(12)
            Self {
                prices: RingBuffer::new(prices_len),
                ema_len,
                prices_len,
            }
        }

        fn apply_candle(&mut self, candle: &Candle) {
            self.prices.add(candle.close);
        }

        fn fire_with_res(&self, ticker: &FlatTicker) -> std::result::Result<(), ()> {
            if self.prices.all().len() < self.prices_len {
                return Err(());
            }
            let ema = self.calc_ema();
            let eps = 0.001;
            let p = ticker.data_last_price;
            if p - p * eps <= ema && ema <= p + p * eps {
                return Ok(());
            }
            Err(())
        }

        fn calc_ema(&self) -> f64 {
            let emas = calc_emas(&self.prices.all(), self.ema_len as i32);
            *emas.last().unwrap()
        }
    }

    #[cfg(test)]
    mod tests {
        use exchanges_arbitrage::{Candle, CandleTimeframe, FlatTicker};

        use crate::trade::{RingBuffer, StrategyEmas};

        fn l_to_str(l: &Vec<f64>) -> String {
            l.iter()
                .map(|&v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }

        fn create_ticker_on_price(price: f64) -> FlatTicker {
            let now_millis = chrono::Utc::now().timestamp_millis();
            FlatTicker::new_with_millis(now_millis, "", price, "").unwrap()
        }

        #[test]
        fn test_use_ring_buffer() {
            let mut ring_buffer = RingBuffer::new(3);
            assert_eq!(ring_buffer.p, -1);
            ring_buffer.add(10.0);
            assert_eq!(ring_buffer.p, 0);
            assert_eq!(l_to_str(&ring_buffer.all()), "10");
            ring_buffer.add(20.0);
            assert_eq!(ring_buffer.p, 1);
            assert_eq!(l_to_str(&ring_buffer.all()), "10,20");
            ring_buffer.add(30.0);
            assert_eq!(ring_buffer.p, 2);
            assert_eq!(l_to_str(&ring_buffer.all()), "10,20,30");
            ring_buffer.add(40.0);
            assert_eq!(ring_buffer.p, 0);
            assert_eq!(l_to_str(&ring_buffer.all()), "20,30,40");
        }

        /// Illustrates such kind of trade https://prnt.sc/fBNkfJdq3Dku.
        #[test]
        fn test_strategy_fire_positive() {
            let candles_raw: Vec<(i64, f64, f64, f64, f64)> = vec![
                (1712448000000, 68896.0, 68999.99, 69150.0, 68824.0),
                (1712451600000, 68999.99, 69541.65, 69655.16, 68998.57),
                (1712455200000, 69541.65, 69300.0, 69777.0, 69300.0),
                (1712458800000, 69300.01, 69408.05, 69510.0, 69250.0),
                (1712462400000, 69408.04, 69304.44, 69493.98, 69053.97),
                (1712466000000, 69304.44, 69397.76, 69464.0, 69267.07),
                (1712469600000, 69397.76, 69244.09, 69414.87, 69110.81),
                (1712473200000, 69244.08, 69364.01, 69482.44, 69213.84),
                (1712476800000, 69364.0, 69479.99, 69555.0, 69333.72),
                (1712480400000, 69479.99, 69289.99, 69493.25, 69199.01),
                (1712484000000, 69290.0, 69328.14, 69495.15, 69236.0),
                (1712487600000, 69328.14, 69357.0, 69407.72, 69200.0),
                (1712491200000, 69357.0, 69335.99, 69415.59, 69210.0),
                (1712494800000, 69336.0, 69349.93, 69550.0, 69285.71),
                (1712498400000, 69349.93, 69578.44, 69664.67, 69220.0),
                (1712502000000, 69578.44, 69543.74, 69886.03, 69360.44),
                (1712505600000, 69543.73, 70074.0, 70326.29, 69483.9),
                (1712509200000, 70074.01, 69798.01, 70112.0, 69658.19),
                (1712512800000, 69798.02, 69390.01, 69828.0, 69234.14),
                (1712516400000, 69390.01, 69068.01, 69508.71, 68849.77),
                (1712520000000, 69068.01, 69309.69, 69403.7, 68960.01),
                (1712523600000, 69309.7, 69031.3, 69413.1, 69031.29),
                (1712527200000, 69031.3, 69200.0, 69344.4, 68901.0),
                (1712530800000, 69200.0, 69360.39, 69477.18, 69188.05),
                (1712534400000, 69360.38, 69325.99, 69432.0, 69043.24),
                (1712538000000, 69326.0, 69437.02, 69841.03, 69285.7),
                (1712541600000, 69437.03, 69228.0, 69437.03, 69126.36),
                (1712545200000, 69228.0, 69458.95, 69500.0, 69134.99),
                (1712548800000, 69458.95, 69382.0, 69500.0, 69280.0),
                (1712552400000, 69382.01, 69701.99, 69801.0, 69382.0),
                (1712556000000, 69702.0, 69790.29, 69818.04, 69632.73),
                (1712559600000, 69790.29, 70698.63, 70850.0, 69790.28),
                (1712563200000, 70698.64, 71968.0, 72111.99, 70642.0),
                (1712566800000, 71968.0, 72197.25, 72650.0, 71650.32),
                (1712570400000, 72197.26, 72323.98, 72374.0, 71792.06),
                (1712574000000, 72323.99, 72404.0, 72647.86, 72218.27),
                (1712577600000, 72404.0, 72180.02, 72797.99, 72137.48),
                (1712581200000, 72180.01, 71944.31, 72310.01, 71691.35),
                (1712584800000, 71944.31, 71726.0, 71985.38, 71245.17),
                (1712588400000, 71725.99, 71773.98, 71987.0, 71598.45),
                (1712592000000, 71773.98, 71740.1, 72021.56, 71556.53),
                (1712595600000, 71740.1, 71699.23, 71800.0, 71300.0),
                (1712599200000, 71699.23, 71908.61, 72104.05, 71672.34),
                (1712602800000, 71908.6, 71766.0, 71973.6, 71564.51),
                (1712606400000, 71766.0, 71711.99, 71818.77, 71500.37),
                (1712610000000, 71711.98, 71692.01, 71900.0, 71628.01),
                (1712613600000, 71692.01, 71916.09, 71960.0, 71599.9),
                (1712617200000, 71916.08, 71620.0, 72000.0, 71610.01),
                (1712620800000, 71620.0, 71419.99, 71758.19, 71320.01),
                (1712624400000, 71420.0, 71275.46, 71439.8, 70833.33),
                (1712628000000, 71275.45, 71477.44, 71618.95, 71156.77),
                (1712631600000, 71477.44, 71285.0, 71641.97, 71186.02),
                (1712635200000, 71285.0, 71208.01, 71339.79, 70917.2),
                (1712638800000, 71208.02, 71056.0, 71323.66, 70957.0),
                (1712642400000, 71055.99, 70765.69, 71265.47, 70560.0),
                (1712646000000, 70765.7, 70383.99, 70940.9, 70110.0),
                (1712649600000, 70383.98, 70500.01, 70600.0, 69576.61),
                (1712653200000, 70500.01, 70392.18, 70860.65, 70256.98),
                (1712656800000, 70392.18, 70566.64, 70697.04, 70234.99),
                (1712660400000, 70566.63, 70794.85, 70905.26, 70566.63),
                (1712664000000, 70794.84, 70690.0, 70977.0, 70654.38),
                (1712667600000, 70690.0, 69928.0, 70920.0, 69921.62),
                (1712671200000, 69928.0, 69358.32, 70429.04, 68611.0),
                (1712674800000, 69358.32, 69264.0, 69360.0, 68535.03),
                (1712678400000, 69264.0, 68601.24, 69320.0, 68516.15),
                (1712682000000, 68601.25, 68823.99, 68962.43, 68210.0),
                (1712685600000, 68823.99, 68879.23, 69025.89, 68769.79),
                (1712689200000, 68879.22, 69000.77, 69174.0, 68656.08),
                (1712692800000, 69000.78, 69171.19, 69208.2, 68962.31),
                (1712696400000, 69171.18, 69213.83, 69250.0, 68935.64),
                (1712700000000, 69213.83, 69325.65, 69439.6, 69144.59),
                (1712703600000, 69325.64, 69146.0, 69332.0, 68888.47),
                (1712707200000, 69146.0, 69138.81, 69304.26, 69025.97),
                (1712710800000, 69138.81, 69269.75, 69269.76, 68603.92),
                (1712714400000, 69269.75, 68651.98, 69594.15, 68466.63),
                (1712718000000, 68651.98, 69150.18, 69180.0, 68625.61),
                (1712721600000, 69150.18, 69134.33, 69199.99, 69005.55),
                (1712725200000, 69134.34, 69255.82, 69414.2, 69121.35),
                (1712728800000, 69255.82, 69369.25, 69479.8, 69227.62),
                (1712732400000, 69369.25, 69336.28, 69515.0, 69268.47),
                (1712736000000, 69336.27, 69011.92, 69336.28, 68800.0),
                (1712739600000, 69011.92, 69139.2, 69171.75, 68902.43),
                (1712743200000, 69139.19, 68932.01, 69180.0, 68629.99),
                (1712746800000, 68932.01, 69043.0, 69132.61, 68853.83),
                (1712750400000, 69043.0, 67976.0, 69204.28, 67559.99),
                (1712754000000, 67975.99, 68259.77, 68313.1, 67518.0),
                (1712757600000, 68259.77, 68703.05, 68974.58, 68009.94),
                (1712761200000, 68703.04, 68657.32, 68760.71, 68264.0),
                (1712764800000, 68657.31, 69500.0, 69536.0, 68556.0),
                (1712768400000, 69499.99, 69349.0, 69910.96, 69142.81),
                (1712772000000, 69350.1, 69314.33, 69411.34, 69071.0),
                (1712775600000, 69314.33, 70079.98, 70098.89, 69219.14),
                (1712779200000, 70084.0, 69812.01, 70136.0, 69740.0),
                (1712782800000, 69812.01, 70017.0, 70036.81, 69581.03),
                (1712786400000, 70017.01, 70528.0, 71172.08, 70017.0),
                (1712790000000, 70528.0, 70631.08, 70799.99, 70488.38),
                (1712793600000, 70631.08, 70520.01, 70663.21, 70285.13),
                (1712797200000, 70520.0, 70553.28, 70760.0, 70469.12),
                (1712800800000, 70553.28, 70995.66, 71062.0, 70500.0),
                (1712804400000, 70995.66, 70851.67, 71027.99, 70773.58),
                (1712808000000, 70851.66, 70604.01, 70888.35, 70378.11),
                (1712811600000, 70604.01, 70583.99, 70680.0, 70444.0),
                (1712815200000, 70583.99, 70717.82, 70848.76, 70564.0),
                (1712818800000, 70717.81, 70735.99, 71305.89, 70717.81),
                (1712822400000, 70736.0, 71039.39, 71256.27, 70617.25),
                (1712826000000, 71039.4, 70604.0, 71053.61, 70596.43),
                (1712829600000, 70604.0, 70459.99, 70785.05, 70421.56),
                (1712833200000, 70460.0, 69983.16, 70596.0, 69881.31),
                (1712836800000, 69983.16, 70858.83, 71100.0, 69867.76),
                (1712840400000, 70858.82, 70284.0, 70988.0, 69669.75),
                (1712844000000, 70284.01, 69600.54, 70350.01, 69584.0),
                (1712847600000, 69600.55, 70000.0, 70026.25, 69567.21),
                (1712851200000, 70000.0, 70011.99, 70133.17, 69574.88),
                (1712854800000, 70011.99, 70203.99, 70446.03, 70000.0),
                (1712858400000, 70203.99, 70099.75, 70429.32, 69951.18),
                (1712862000000, 70099.75, 70459.67, 70529.73, 70008.0),
                (1712865600000, 70459.67, 70499.62, 70746.09, 70383.8),
                (1712869200000, 70499.63, 70115.41, 70504.99, 70000.0),
                (1712872800000, 70115.41, 70151.26, 70259.46, 70033.52),
                (1712876400000, 70151.26, 70006.23, 70196.0, 69804.87),
                (1712880000000, 70006.22, 70301.63, 70336.38, 70002.22),
                (1712883600000, 70301.63, 70280.01, 70367.29, 70113.6),
                (1712887200000, 70280.01, 70635.93, 70775.0, 70208.73),
                (1712890800000, 70635.92, 70977.27, 70996.91, 70635.92),
                (1712894400000, 70977.27, 71115.99, 71227.46, 70812.56),
                (1712898000000, 71116.0, 70956.32, 71175.99, 70887.72),
                (1712901600000, 70956.32, 70871.05, 71012.0, 70713.7),
                (1712905200000, 70871.04, 70764.01, 70967.92, 70566.02),
                (1712908800000, 70764.0, 70759.99, 70893.58, 70582.52),
                (1712912400000, 70759.99, 70652.0, 70863.51, 70509.58),
                (1712916000000, 70652.0, 70786.4, 70868.0, 70601.23),
                (1712919600000, 70786.41, 70904.01, 70972.0, 70760.01),
                (1712923200000, 70904.0, 70534.24, 70975.54, 70323.96),
                (1712926800000, 70534.23, 69888.0, 70534.24, 69631.06),
                (1712930400000, 69888.0, 69270.01, 70090.55, 69236.95),
                (1712934000000, 69265.85, 69347.3, 69718.35, 69248.33),
                (1712937600000, 69347.3, 68840.54, 69580.0, 68679.61),
                (1712941200000, 68840.54, 68140.02, 68960.0, 67444.0),
                (1712944800000, 68140.01, 66492.0, 68383.63, 65086.86),
                (1712948400000, 66492.0, 66843.98, 67335.22, 66329.19),
                (1712952000000, 66843.98, 67120.0, 67440.0, 66700.0),
                (1712955600000, 67119.99, 66960.01, 67128.64, 66680.84),
                (1712959200000, 66960.0, 67053.33, 67270.53, 66828.01),
                (1712962800000, 67053.33, 67116.52, 67230.19, 66812.0),
                (1712966400000, 67116.52, 66913.27, 67142.86, 66737.64),
                (1712970000000, 66913.27, 65886.63, 66928.0, 65731.27),
                (1712973600000, 65886.64, 66535.99, 66593.41, 65871.49),
                (1712977200000, 66536.0, 67160.55, 67168.0, 66483.87),
            ];
            let candles = candles_raw
                .iter()
                .map(|x| {
                    Candle::new(
                        "".to_string(),
                        "".to_string(),
                        x.0,
                        x.1,
                        x.2,
                        x.3,
                        x.4,
                        CandleTimeframe::Hours(1),
                    )
                })
                .collect::<Vec<Candle>>();
            let mut strategy = StrategyEmas::new();
            for candle in candles.iter() {
                strategy.apply_candle(&candle);
            }
            match strategy.fire_with_res(&create_ticker_on_price(67269.0)) {
                Ok(_) => assert_eq!(0, 1),
                Err(_) => {}
            }
            match strategy.fire_with_res(&create_ticker_on_price(67270.0)) {
                Ok(_) => {}
                Err(_) => assert_eq!(0, 1),
            }
            match strategy.fire_with_res(&create_ticker_on_price(67400.0)) {
                Ok(_) => {}
                Err(_) => assert_eq!(0, 1),
            }
            match strategy.fire_with_res(&create_ticker_on_price(67404.0)) {
                Ok(_) => assert_eq!(0, 1),
                Err(_) => {}
            }
        }
    }
}

mod debug {
    use ::zip::ZipArchive;
    use exchanges_arbitrage::{domain::bybit, Candle, CandleTimeframe, FlatTicker, TickersPort};
    use plotters::prelude::*;
    use polars::prelude::*;
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        str::FromStr,
    };

    #[derive(serde::Deserialize)]
    struct ResTradesDataDownloadItem {
        url: String,
    }

    #[allow(non_snake_case)]
    #[derive(serde::Deserialize)]
    struct ResTradesData {
        downloadItemList: Vec<ResTradesDataDownloadItem>,
    }

    #[derive(serde::Deserialize)]
    struct ResTrades {
        data: ResTradesData,
        success: bool,
    }

    pub fn trades_download_write() {
        let dir_path = ".var/binance-local/spot/trades/1m/BTCUSDT/";
        match std::fs::create_dir_all(dir_path) {
            Ok(_) => log::info!("dir {} already exists", dir_path),
            Err(e) => log::info!("e: {:?}", e),
        }
        let mut current_date = chrono::NaiveDate::from_str("2023-08-01").unwrap();
        let end_date: chrono::NaiveDate = chrono::Utc::now().naive_utc().into();
        while current_date < end_date {
            let date_str = current_date.to_string();
            let mut df = download_binance_trades_df(&date_str);
            let filename = format!("BTCUSDT-trades-{}.csv", date_str);
            let out_path = format!("{}{}", dir_path, filename);
            let mut file = std::fs::File::create(out_path.clone()).unwrap();
            CsvWriter::new(&mut file).finish(&mut df).unwrap();
            log::info!("df {:?} written to {}", df.shape(), out_path);
            current_date += chrono::TimeDelta::try_days(1).unwrap();
        }
    }

    pub fn trades_read_agg_write() {
        let in_filepath = "\
            .var/binance-local/spot/trades/1m/BTCUSDT/\
            BTCUSDT-trades-2024-03-23.csv";
        let mut trades_df = CsvReader::from_path(in_filepath)
            .unwrap()
            .with_schema(Some(std::sync::Arc::from(get_trades_schema())))
            .finish()
            .unwrap()
            .sort(["time"], false, true)
            .unwrap()
            .lazy()
            .with_columns([
                (col("time").cast(DataType::UInt64) / lit(60_000) * lit(60_000)).alias("time_1m"),
            ])
            .group_by([col("time_1m")])
            .agg([
                col("price").last().alias("close"),
                col("price").first().alias("open"),
                col("price").max().alias("high"),
                col("price").min().alias("low"),
            ])
            .collect()
            .unwrap();
        let out_dir_path = ".var/binance-local/spot/klines/1m/BTCUSDT/";
        std::fs::create_dir_all(out_dir_path).unwrap_or_default();
        let out_path = format!("{out_dir_path}BTCUSDT-klines-2024-03-23.csv");
        let mut file = std::fs::File::create(out_path.clone()).unwrap();
        CsvWriter::new(&mut file).finish(&mut trades_df).unwrap();
        log::info!("df {:?} written to {}", trades_df.shape(), out_path);
    }

    fn download_binance_trades_df(date_str: &str) -> DataFrame {
        let trades_url = {
            let data_raw = serde_json::json!({
                "bizType": "SPOT",
                "productName": "trades",
                "symbolRequestItems": [{
                    "endDay": date_str,
                    "granularityList": [],
                    "interval": "daily",
                    "startDay": date_str,
                    "symbol": "BTCUSDT",
                }]
            })
            .to_string();
            let url = "\
                https://www.binance.com/bapi/bigdata/v1/public\
                /bigdata/finance/exchange/listDownloadData2";
            let res = reqwest::blocking::Client::new()
                .post(url)
                .body(data_raw)
                .header("content-type", "application/json")
                .send()
                .unwrap();
            let res_body = serde_json::from_str::<ResTrades>(&res.text().unwrap()).unwrap();
            assert!(res_body.success);
            assert_eq!(res_body.data.downloadItemList.len(), 1);
            res_body.data.downloadItemList[0].url.clone()
        };
        let csv_str = {
            let csv_zip_bytes = reqwest::blocking::Client::new()
                .get(trades_url)
                .send()
                .unwrap()
                .bytes()
                .unwrap();
            let mut c = std::io::Cursor::new(Vec::new());
            c.write_all(&csv_zip_bytes).unwrap();
            c.seek(SeekFrom::Start(0)).unwrap();
            let reader = std::io::BufReader::new(c);
            let mut archive = ZipArchive::new(reader).unwrap();
            assert_eq!(archive.len(), 1);
            let mut out_str = String::from("");
            archive
                .by_index(0)
                .unwrap()
                .read_to_string(&mut out_str)
                .unwrap();
            out_str
        };
        CsvReader::new(std::io::Cursor::new(csv_str.as_bytes()))
            .has_header(false)
            .with_schema(Some(std::sync::Arc::from(get_trades_schema())))
            .finish()
            .unwrap()
    }

    fn get_trades_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("id", DataType::String),
            Field::new("price", DataType::Float64),
            Field::new("qty", DataType::Float64),
            Field::new("base_qty", DataType::Float64),
            Field::new("time", DataType::String),
            Field::new("is_buyer", DataType::Boolean),
            Field::new("is_maker", DataType::Boolean),
        ])
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    struct Kline {
        time_1m: f64,
        time_1m_dt: String,
        close: f64,
        open: f64,
        high: f64,
        low: f64,
    }

    impl Kline {
        fn new_from_csv_record(record: csv::StringRecord) -> Self {
            let time_1m = record.get(0).unwrap().parse::<f64>().unwrap();
            let close = record.get(1).unwrap().parse::<f64>().unwrap();
            let open = record.get(2).unwrap().parse::<f64>().unwrap();
            let high = record.get(3).unwrap().parse::<f64>().unwrap();
            let low = record.get(4).unwrap().parse::<f64>().unwrap();
            Self {
                time_1m_dt: String::from(""),
                time_1m,
                close,
                open,
                high,
                low,
            }
        }
    }

    pub fn draw_chart_on_klines(_ema: Option<i32>) {
        fn parse_time(t: &str) -> chrono::NaiveDate {
            chrono::NaiveDate::from_str(t).unwrap()
        }
        let filepath = ".var/binance-local/spot/klines/1m/BTCUSDT/BTCUSDT-klines-2024-03-23.csv";
        let mut klines = csv::Reader::from_path(filepath)
            .unwrap()
            .records()
            .map(|x| Kline::new_from_csv_record(x.unwrap()))
            .collect::<Vec<Kline>>()[0..10]
            .to_vec();
        klines.sort_by(|a, b| b.time_1m.partial_cmp(&a.time_1m).unwrap());
        let tuple_slice = klines[0..10]
            .iter()
            .map(|x| (x.time_1m_dt.clone(), x.open, x.high, x.low, x.close));
        log::info!("klines: {:?}", &tuple_slice);
        let data = vec![
            ("2019-04-25", 130.06, 131.37, 128.83, 129.15),
            ("2019-04-24", 125.79, 125.85, 124.52, 125.01),
            ("2019-04-23", 124.1, 125.58, 123.83, 125.44),
            ("2019-04-22", 122.62, 124.0000, 122.57, 123.76),
            ("2019-04-18", 122.19, 123.52, 121.3018, 123.37),
            ("2019-04-17", 121.24, 121.85, 120.54, 121.77),
            ("2019-04-16", 121.64, 121.65, 120.1, 120.77),
            ("2019-04-15", 120.94, 121.58, 120.57, 121.05),
            ("2019-04-12", 120.64, 120.98, 120.37, 120.95),
            ("2019-04-11", 120.54, 120.85, 119.92, 120.33),
            ("2019-04-10", 119.76, 120.35, 119.54, 120.19),
            ("2019-04-09", 118.63, 119.54, 118.58, 119.28),
            ("2019-04-08", 119.81, 120.02, 118.64, 119.93),
            ("2019-04-05", 119.39, 120.23, 119.37, 119.89),
            ("2019-04-04", 120.1, 120.23, 118.38, 119.36),
            ("2019-04-03", 119.86, 120.43, 119.15, 119.97),
            ("2019-04-02", 119.06, 119.48, 118.52, 119.19),
            ("2019-04-01", 118.95, 119.1085, 118.1, 119.02),
            ("2019-03-29", 118.07, 118.32, 116.96, 117.94),
            ("2019-03-28", 117.44, 117.58, 116.13, 116.93),
            ("2019-03-27", 117.875, 118.21, 115.5215, 116.77),
            ("2019-03-26", 118.62, 118.705, 116.85, 117.91),
            ("2019-03-25", 116.56, 118.01, 116.3224, 117.66),
            ("2019-03-22", 119.5, 119.59, 117.04, 117.05),
            ("2019-03-21", 117.135, 120.82, 117.09, 120.22),
            ("2019-03-20", 117.39, 118.75, 116.71, 117.52),
            ("2019-03-19", 118.09, 118.44, 116.99, 117.65),
            ("2019-03-18", 116.17, 117.61, 116.05, 117.57),
            ("2019-03-15", 115.34, 117.25, 114.59, 115.91),
            ("2019-03-14", 114.54, 115.2, 114.33, 114.59),
        ];
        let root = BitMapBackend::new(".var/0.png", (1024, 768)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let (max_x, min_x) = (
            parse_time(data[0].0) + chrono::TimeDelta::try_days(1).unwrap(),
            parse_time(data[29].0) - chrono::TimeDelta::try_days(1).unwrap(),
        );
        let (min_y, max_y) = (50_f32, 140_f32);
        let mut chart = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(40)
            .caption("MSFT Stock Price", ("sans-serif", 50.0).into_font())
            .build_cartesian_2d(min_x..max_x, min_y..max_y)
            .unwrap();
        chart
            .configure_mesh()
            .light_line_style(WHITE)
            .draw()
            .unwrap();
        chart
            .draw_series(data.iter().map(|x| {
                CandleStick::new(parse_time(x.0), x.1, x.2, x.3, x.4, GREEN.filled(), RED, 15)
            }))
            .unwrap();
        chart
            .draw_series(LineSeries::new(
                data.iter()
                    .map(|x| (parse_time(x.0), x.1 - 20.0))
                    .collect::<Vec<(chrono::NaiveDate, f32)>>(),
                &GREEN,
            ))
            .unwrap();
        root.present().unwrap();
    }

    pub fn listen_save_bybit_tickers() {
        let mut tickers_port = TickersPort::new_and_connect();
        tickers_port.create_table();
        let config = bybit::ConfigWs::new("BTCUSDT".to_string(), false, true);
        let on_message = |event: bybit::EventWs| match event {
            bybit::EventWs::Ticker(ticker) => tickers_port.insert(&ticker).unwrap(),
            _ => {}
        };
        bybit::TradingWs::listen_ws(&config, on_message);
    }

    ///
    /// Reads, sorts in asc and uploads all available tickets from
    /// .var/binance-local/spot/trades/1m/BTCUSDT/* to database table
    ///
    pub fn read_save_tickers_to_database() {
        let start_date = chrono::NaiveDate::parse_from_str("2024-04-08", "%Y-%m-%d").unwrap();
        let end_date = chrono::Utc::now().date_naive() - chrono::Duration::try_days(1).unwrap();
        let mut date = start_date.clone();
        let mut tickers_port = TickersPort::new_and_connect();
        while date <= end_date {
            log::info!("{:?}", date);
            let date_str = date.to_string();
            let filepath = format!(
                "/Users/yy/dev/python/ninja-move/jupy/.var/binance-local/\
                spot/trades/1m/BTCUSDT/BTCUSDT-trades-{date_str}.csv"
            );
            log::info!("read file {filepath}");
            let mut tickers = csv::Reader::from_path(filepath)
                .unwrap()
                .records()
                .map(|x| {
                    let record = x.unwrap();
                    let ts_millis = record.get(4).unwrap().parse::<i64>().unwrap();
                    let data_last_price = record.get(1).unwrap().parse::<f64>().unwrap();
                    FlatTicker::new_with_millis(ts_millis, "BTCUSDT", data_last_price, "binance")
                        .unwrap()
                })
                .collect::<Vec<FlatTicker>>()
                .to_vec();
            tickers.sort_by(|a, b| a.ts_millis.cmp(&b.ts_millis));
            log::info!("remove prev tickers");
            tickers_port.remove_on_date(&date).unwrap();
            log::info!("insert {} tickers", tickers.len());
            tickers_port.insert_batch(&tickers).unwrap();
            date += chrono::Duration::try_days(1).unwrap();
        }
    }

    pub fn calc_2024_04_07_candles() {
        let start_time =
            chrono::NaiveDateTime::parse_from_str("2024-04-07 00:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        let end_time = start_time + chrono::Duration::try_days(1).unwrap();
        log::info!("start_time={}, end_time={}", start_time, end_time);
        let mut tickers_port = TickersPort::new_and_connect();
        let tickers = tickers_port.fetch(
            start_time.and_utc().timestamp(),
            end_time.and_utc().timestamp(),
        );
        log::info!("there are {} tickers", tickers.len());
        log::info!("{:?}", tickers[0]);
        let candles = {
            let interval_1h_millis = 60 * 60 * 1000;
            let mut out = vec![];
            let mut threshold_ts = tickers[0].ts_millis + interval_1h_millis;
            let mut current_candle =
                Candle::new_from_ticker(&tickers[0], CandleTimeframe::Minutes(1));
            for ticker in tickers.iter() {
                if ticker.ts_millis < threshold_ts {
                    current_candle.apply_ticker(&ticker);
                } else {
                    out.push(current_candle);
                    current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Minutes(1));
                    threshold_ts += interval_1h_millis;
                }
            }
            out
        };
        log::info!("there are {} candles", candles.len());
    }

    pub fn read_calc_write_emas() {
        let input_file_path =
            "/Users/yy/dev/python/ninja-move/jupy/.var/close-prices-2024-04-08.json";
        let output_file_path = "/Users/yy/dev/python/ninja-move/jupy/.var/emas-2024-04-08.json";
        let input_str = std::fs::read_to_string(input_file_path).unwrap();
        let input_prices = serde_json::from_str::<DebugClosePrices>(&input_str)
            .unwrap()
            .close_prices;
        let emas = crate::trade::calc_emas(&input_prices, 12);
        let emas_str = serde_json::json!({"emas": emas}).to_string();
        std::fs::write(output_file_path, emas_str).unwrap();
    }

    #[derive(serde::Deserialize)]
    struct DebugClosePrices {
        close_prices: Vec<f64>,
    }
}
