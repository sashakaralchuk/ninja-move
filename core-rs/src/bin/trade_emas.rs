use polars::prelude::{DataType, Field, Schema};

fn main() {
    env_logger::init();
    let args = exchanges_arbitrage::Args::new_and_parse();
    match args.command.as_str() {
        "download-trades" => download_trades::main(),
        "agg-trades" => agg_trades::main(),
        "draw-graph" => draw_graph::main(None),
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

    use polars::prelude::{
        CsvReader, CsvWriter, DataFrame, SerReader, SerWriter,
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
            let res_body =
                serde_json::from_str::<ResTrades>(&res.text().unwrap())
                    .unwrap();
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
        let mut current_date =
            chrono::NaiveDate::from_str("2023-08-01").unwrap();
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
            .with_columns([(col("time").cast(DataType::UInt64) / lit(60_000)
                * lit(60_000))
            .alias("time_1m")])
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
        let root =
            BitMapBackend::new(".var/0.png", (1024, 768)).into_drawing_area();
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
                CandleStick::new(
                    parse_time(x.0),
                    x.1,
                    x.2,
                    x.3,
                    x.4,
                    GREEN.filled(),
                    RED,
                    15,
                )
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
