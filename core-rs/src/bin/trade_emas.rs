use polars::prelude::{DataType, Field, Schema};

fn main() {
    env_logger::init();
    let args = exchanges_arbitrage::Args::new_and_parse();
    match args.command.as_str() {
        "download-trades" => download_trades::main(),
        "agg-trades" => agg_trades::main(),
        "draw-graph" => draw_graph::main(None),
        "collect-bybit-tickers" => trade::collect_bybit_tickers(),
        "run-backtest" => trade::run_backtest(),
        "run-trading" => trade::run_trading(),
        "read-calc-write-emas" => trade::read_calc_write_emas(),
        "upload-tickers-to-database" => trade::upload_tickers_to_database(),
        "show-2024-04-07-candles" => trade::show_2024_04_07_candles(),
        _ => panic!("command not found"),
    }
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

mod download_trades {
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        str::FromStr,
    };

    use polars::prelude::{CsvReader, CsvWriter, DataFrame, SerReader, SerWriter};

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

    fn download_binance_df(date_str: &str) -> DataFrame {
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
        CsvReader::new(std::io::Cursor::new(csv_str.as_bytes()))
            .has_header(false)
            .with_schema(Some(std::sync::Arc::from(super::get_trades_schema())))
            .finish()
            .unwrap()
    }

    pub fn main() {
        let dir_path = ".var/binance-local/spot/trades/1m/BTCUSDT/";
        match std::fs::create_dir_all(dir_path) {
            Ok(_) => log::info!("dir {} already exists", dir_path),
            Err(e) => log::info!("e: {:?}", e),
        }
        let mut current_date = chrono::NaiveDate::from_str("2023-08-01").unwrap();
        let end_date: chrono::NaiveDate = chrono::Utc::now().naive_utc().into();
        while current_date < end_date {
            let date_str = current_date.to_string();
            let mut df = download_binance_df(&date_str);
            let filename = format!("BTCUSDT-trades-{}.csv", date_str);
            let out_path = format!("{}{}", dir_path, filename);
            let mut file = std::fs::File::create(out_path.clone()).unwrap();
            CsvWriter::new(&mut file).finish(&mut df).unwrap();
            log::info!("df {:?} written to {}", df.shape(), out_path);
            current_date += chrono::TimeDelta::try_days(1).unwrap();
        }
    }
}

mod agg_trades {
    use polars::prelude::*;

    pub fn main() {
        let in_filepath = "\
            .var/binance-local/spot/trades/1m/BTCUSDT/\
            BTCUSDT-trades-2024-03-23.csv";
        let mut trades_df = CsvReader::from_path(in_filepath)
            .unwrap()
            .with_schema(Some(std::sync::Arc::from(super::get_trades_schema())))
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
        // TODO: use debugger here
        let out_dir_path = ".var/binance-local/spot/klines/1m/BTCUSDT/";
        std::fs::create_dir_all(out_dir_path).unwrap_or_default();
        let out_path = format!("{out_dir_path}BTCUSDT-klines-2024-03-23.csv");
        let mut file = std::fs::File::create(out_path.clone()).unwrap();
        CsvWriter::new(&mut file).finish(&mut trades_df).unwrap();
        log::info!("df {:?} written to {}", trades_df.shape(), out_path);
    }
}

mod draw_graph {
    use plotters::prelude::*;
    use std::str::FromStr;

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

    fn parse_time(t: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::from_str(t).unwrap()
    }

    pub fn main(_ema: Option<i32>) {
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
}

mod trade {
    use exchanges_arbitrage::{
        domain::bybit, Candle, CandleTimeframe, FlatTicker, HistoryPort, HistoryRow, TickersPort,
        TrailingThreshold, TrailingThresholdReason,
    };

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

    /// Calcs the same way as pd.Series(...).ewm(span=len, adjust=False).mean()
    fn calc_emas(prices: &Vec<f64>, len: i32) -> Vec<f64> {
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

    struct StrategySignal {}

    impl StrategySignal {
        fn new() -> Self {
            Self {}
        }
    }

    struct Strategy {
        ema_len: usize,
        prices_len: usize,
        prices: RingBuffer,
    }

    impl Strategy {
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

        fn fire(&self, ticker: &FlatTicker) -> Option<StrategySignal> {
            let prices = self.prices.all();
            if prices.len() < self.prices_len {
                return None;
            }
            let ema = *calc_emas(&prices, self.ema_len as i32).last().unwrap();
            let eps = 0.001;
            let p = ticker.data_last_price;
            if p - p * eps <= ema && ema <= p + p * eps {
                return Some(StrategySignal::new());
            }
            None
        }
    }

    struct BacktestOut {
        // FIXME: add start and end times + save them in table
        start_price: f64,
        close_price: f64,
        reason: String,
    }

    impl BacktestOut {
        fn new(start_price: f64, close_price: f64, reason: String) -> Self {
            Self {
                start_price,
                close_price,
                reason,
            }
        }

        fn log_hist(history_port: &mut HistoryPort, backtests: &Vec<BacktestOut>) {
            // FIXME: write hist based on 2 entities - SignalStrategy, SellStrategy
            for backtest in backtests.iter() {
                let x = HistoryRow::new(
                    "backtest-ema-TODO",
                    backtest.reason.as_str(),
                    &"bybit".to_string(),
                    &"BTCUSDT".to_string(),
                    backtest.start_price,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    backtest.close_price,
                );
                history_port.insert_hist(&x).unwrap();
            }
        }
    }

    pub fn collect_bybit_tickers() {
        let mut tickers_port = TickersPort::new_and_connect();
        tickers_port.create_table();
        let config = bybit::ConfigWs::new("BTCUSDT".to_string(), false, true);
        let on_message = |event: bybit::EventWs| match event {
            bybit::EventWs::Ticker(ticker) => tickers_port.insert(&ticker).unwrap(),
            _ => {}
        };
        bybit::TradingWs::listen_ws(&config, on_message);
    }

    /// Reads, sorts in asc and uploads all available tickets from
    /// .var/binance-local/spot/trades/1m/BTCUSDT/* to database table
    pub fn upload_tickers_to_database() {
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

    pub fn show_2024_04_07_candles() {
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

    pub fn run_trading() {
        unimplemented!()
    }

    pub fn run_backtest() {
        let start_time =
            chrono::NaiveDateTime::parse_from_str("2024-04-07 17:45:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        let end_time =
            chrono::NaiveDateTime::parse_from_str("2024-04-08 17:45:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        let trading_start_time =
            chrono::NaiveDateTime::parse_from_str("2024-04-07 18:15:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        let mut tickers_port = TickersPort::new_and_connect();
        let tickers = tickers_port.fetch(
            start_time.and_utc().timestamp(),
            end_time.and_utc().timestamp(),
        );
        let mut hist_tickers = vec![];
        let mut backtest_tickers = vec![];
        for ticker in tickers.iter() {
            if ticker.ts_millis <= trading_start_time.and_utc().timestamp_millis() {
                hist_tickers.push(ticker.clone());
            } else {
                backtest_tickers.push(ticker.clone())
            }
        }
        log::info!(
            "run test hist_tickers.len={} backtest_tickers.len={}",
            hist_tickers.len(),
            backtest_tickers.len(),
        );
        // TODO: clarify that candles forms proper
        // TODO: than write the results and render them in notebooks
        // TODO: immediate aim - run with some configuration, than configurize it and run remaining
        // TODO: calc "strength" on candles in strategy
        // TODO: run backtest, including (timestamp, open_price, sell_price)
        // TODO: render backtest results
        // TODO: re-check config - candles timeframes
        let mut strategy = {
            let mut s = Strategy::new();
            let mut current_candle =
                Candle::new_from_ticker(&hist_tickers[0], CandleTimeframe::Hours(1));
            for ticker in hist_tickers {
                if current_candle.expired(&ticker) {
                    s.apply_candle(&current_candle);
                    current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Hours(1));
                } else {
                    current_candle.apply_ticker(&ticker)
                }
            }
            s
        };
        let mut current_candle =
            Candle::new_from_ticker(&backtest_tickers[0], CandleTimeframe::Hours(1));
        let mut backtests = vec![];
        let mut threshold: Option<TrailingThreshold> = None;
        for ticker in backtest_tickers {
            if current_candle.expired(&ticker) {
                log::debug!("candle expired {:?}", current_candle);
                strategy.apply_candle(&current_candle);
                current_candle = Candle::new_from_ticker(&ticker, CandleTimeframe::Minutes(1));
            } else {
                current_candle.apply_ticker(&ticker)
            }
            match threshold {
                Some(mut t) => match t.apply_and_make_decision(&ticker) {
                    TrailingThresholdReason::ReachStopLoss(bottom_threshold) => {
                        backtests.push(BacktestOut::new(
                            t.start_price,
                            bottom_threshold,
                            "reach-stop-loss".to_string(),
                        ));
                        threshold = None;
                    }
                    TrailingThresholdReason::ReachThrailingStop(trailing_threshold) => {
                        backtests.push(BacktestOut::new(
                            t.start_price,
                            trailing_threshold,
                            "reach-thrailing-stop".to_string(),
                        ));
                        threshold = None;
                    }
                    _ => {}
                },
                None => match strategy.fire(&ticker) {
                    Some(_) => {
                        threshold = Some(TrailingThreshold::new(ticker.data_last_price));
                    }
                    None => {}
                },
            }
        }
        log::info!("save backtest");
        let mut history_port = HistoryPort::new_and_connect();
        history_port.create_table();
        BacktestOut::log_hist(&mut history_port, &backtests)
    }

    #[derive(serde::Deserialize)]
    struct DebugClosePrices {
        close_prices: Vec<f64>,
    }

    pub fn read_calc_write_emas() {
        let input_file_path =
            "/Users/yy/dev/python/ninja-move/jupy/.var/close-prices-2024-04-08.json";
        let output_file_path = "/Users/yy/dev/python/ninja-move/jupy/.var/emas-2024-04-08.json";
        let input_str = std::fs::read_to_string(input_file_path).unwrap();
        let input_prices = serde_json::from_str::<DebugClosePrices>(&input_str)
            .unwrap()
            .close_prices;
        let emas = calc_emas(&input_prices, 12);
        let emas_str = serde_json::json!({"emas": emas}).to_string();
        std::fs::write(output_file_path, emas_str).unwrap();
    }

    #[cfg(test)]
    mod tests {
        use crate::trade::RingBuffer;

        fn l_to_str(l: &Vec<f64>) -> String {
            l.iter()
                .map(|&v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
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
    }
}
