use std::collections::HashMap;

use lookup_exchanges::ClientPublic;
use lookup_exchanges::{self as lib, exchange::poloniex_public};
use std::cell::RefCell;
use std::error::Error;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::init();
    dotenv::dotenv()?;
    let config = Config::new_from_envs()?;
    let config_fill_absent_kline = config.clone();
    let client_clickhouse = clickhouse::Client::default().with_url(&config.clickhouse_url);
    let client_clickhouse_curr_kline = client_clickhouse.clone();
    let client_clickhouse_fill_absent_kline = client_clickhouse.clone();
    let (tx_curr_kline, mut rx_curr_kline): (Sender<lib::Kline>, Receiver<lib::Kline>) =
        channel(64);
    let (tx_fill_absent_kline, mut rx_fill_absent_kline) = channel(64);
    let curr_klines = RefCell::new(HashMap::new());
    let first_trade_ts = RefCell::new(HashMap::new());
    let client = match config.ex {
        Ex::Poloniex => poloniex_public::Client::new(),
    };
    let client_fill_absent_kline = client.clone();
    for symbol in config.symbols.clone() {
        let mut m_timeframes: HashMap<lib::KlineTimeframe, std::option::Option<lib::Kline>> =
            HashMap::new();
        let mut m_timeframes_ts: HashMap<lib::KlineTimeframe, std::option::Option<i64>> =
            HashMap::new();
        for timeframe in config.timeframes.clone() {
            m_timeframes.insert(timeframe.clone(), None);
            m_timeframes_ts.insert(timeframe.clone(), None);
            client
                .fetch_insert_klines(
                    &client_clickhouse,
                    config.start_date_millis,
                    &symbol,
                    &timeframe,
                )
                .await?;
        }
        curr_klines
            .borrow_mut()
            .insert(symbol.clone(), m_timeframes);
        first_trade_ts.borrow_mut().insert(symbol, m_timeframes_ts);
    }
    type Jh = tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
    let _jh_curr_kline: Jh = tokio::spawn(async move {
        while let Some(k) = rx_curr_kline.recv().await {
            log::info!("save kline to db k={:?}", k);
            let mut insert_klines = client_clickhouse_curr_kline.insert("klines")?;
            insert_klines.write(&k.to_row()?).await?;
            insert_klines.end().await?;
        }
        Ok(())
    });
    let _jh_fetch_insert_klines: Jh = tokio::spawn(async move {
        while let Some((pair, timeframe)) = rx_fill_absent_kline.recv().await {
            log::info!("fill absent kline in db pair={pair} timeframe={timeframe:?}");
            client_fill_absent_kline
                .fetch_insert_klines(
                    &client_clickhouse_fill_absent_kline,
                    config_fill_absent_kline.start_date_millis,
                    &pair,
                    &timeframe,
                )
                .await?;
        }
        Ok(())
    });
    let on_message = |event: lib::EventWs| async {
        if let lib::EventWs::Trade(t) = event {
            apply_trade(
                &t,
                &curr_klines,
                &first_trade_ts,
                &tx_curr_kline,
                &tx_fill_absent_kline,
            )
            .await?;
        }
        Ok(())
    };
    client
        .listen_ws_channel_v2(lib::ChannelWs::Trade, &config.symbols, on_message)
        .await?;
    Ok(())
}

async fn apply_trade(
    trade: &lib::RecentTrade,
    curr_klines: &RefCell<HashMap<String, HashMap<lib::KlineTimeframe, Option<lib::Kline>>>>,
    first_trade_ts: &RefCell<HashMap<String, HashMap<lib::KlineTimeframe, Option<i64>>>>,
    tx_curr_kline: &Sender<lib::Kline>,
    tx_fill_absent_kline: &Sender<(String, lib::KlineTimeframe)>,
) -> Result<(), Box<dyn Error>> {
    log::debug!("trade={:?}", trade);
    let mut curr_klines_ref = curr_klines.borrow_mut();
    let curr_timeframes = curr_klines_ref
        .get_mut(&trade.pair)
        .ok_or("no-pair-in-curr-klines")?;
    for (timeframe, curr_kline) in curr_timeframes.iter_mut() {
        let mut first_trade_ts_ref = first_trade_ts.borrow_mut();
        let first_trade_ts = first_trade_ts_ref
            .get_mut(&trade.pair)
            .ok_or("no-pair")?
            .get_mut(timeframe)
            .ok_or("no-timeframe")?;
        if first_trade_ts.is_none() {
            log::debug!(
                "fill first_trade_ts for ({}, {:?}) to miss constructing inconsistent kline",
                trade.pair,
                timeframe.to_str()
            );
            *first_trade_ts = Some(trade.timestamp);
            continue;
        }
        let first_trade_ts_threshold = first_trade_ts.ok_or("no-first-trade")?
            / timeframe.to_inserval_millis()
            * timeframe.to_inserval_millis()
            + timeframe.to_inserval_millis()
            - 1;
        if trade.timestamp <= first_trade_ts_threshold {
            log::debug!("skip trade={:?} to avoid inconsistent kline", trade);
            continue;
        }
        if curr_kline.is_none() {
            log::debug!("start constructing new kline");
            curr_kline.replace(lib::Kline::new_from_trade(timeframe.clone(), trade)?);
            tx_fill_absent_kline
                .send((trade.pair.clone(), timeframe.clone()))
                .await?;
            continue;
        }
        if curr_kline.as_mut().ok_or("no-curr-kline")?.expired(trade) {
            tx_curr_kline
                .send(curr_kline.as_mut().ok_or("no-curr-kline")?.clone())
                .await?;
            curr_kline.replace(lib::Kline::new_from_trade(timeframe.clone(), trade)?);
            continue;
        }
        curr_kline
            .as_mut()
            .ok_or("no-curr-kline")?
            .apply_recent_trade(trade)?;
    }
    Ok(())
}

fn _queries_clickhouse() {
    let _ = r#"
    CREATE TABLE default.klines (
        pair String,
        time_frame String,
        o Float64,
        h Float64,
        l Float64,
        c Float64,
        utc_begin DateTime,
        volume_bs__buy_base Float64,
        volume_bs__sell_base Float64,
        volume_bs__buy_quote Float64,
        volume_bs__sell_quote Float64
    )
    ENGINE = TinyLog;
    --
    SELECT t1.utc_begin, t2.utc_begin
    FROM (
        SELECT utc_begin
        FROM default.klines
        WHERE (pair = 'BTC_USDT') AND (time_frame = '1m')
    ) t1
    LEFT JOIN (
        SELECT utc_begin - toIntervalMinute(1) utc_begin
        FROM default.klines
        WHERE (pair = 'BTC_USDT') AND (time_frame = '1m')
    ) AS t2
        ON t1.utc_begin = t2.utc_begin
    WHERE t2.utc_begin = 0
    --
    CREATE TABLE default.temp_klines_2025_02_17 (
        pair String,
        time_frame String,
        utc_begin DateTime,
        o Float64,
        h Float64,
        l Float64,
        c Float64
    )
    ENGINE = TinyLog;
    --
    INSERT INTO default.temp_klines_2025_02_17
    SELECT
        'BTC_USDT' pair,
        '1m' time_frame,
        toDateTime(tupleElement(t, 'startTime') / 1000) utc_begin,
        toFloat64(tupleElement(t, 'open')) o,
        toFloat64(tupleElement(t, 'high')) h,
        toFloat64(tupleElement(t, 'low')) l,
        toFloat64(tupleElement(t, 'close')) c
    FROM (
        SELECT
            JSONExtract(
                arrayJoin(JSONExtractArrayRaw(line)),
                'Tuple(low String, high String, open String, close String, amount String, quantity String, buyTakerAmount String, buyTakerQuantity String, tradeCount UInt64, ts UInt64, weightedAverage String, interval String, startTime UInt64, closeTime UInt64)'
            ) t
        FROM url('https://api.poloniex.com/markets/BTC_USDT/candles?interval=MINUTE_1&startTime=1739750400000&limit=500', 'LineAsString')
    );
    --
    SELECT t1.utc_begin, t2.utc_begin,
        t1.o - t2.o diff_o,
        t1.o - t2.o diff_h,
        t1.o - t2.o diff_l,
        t1.o - t2.o diff_c
    FROM default.temp_klines_2025_02_17 t1
    LEFT JOIN (
        SELECT *
        FROM default.klines
        WHERE pair = 'BTC_USDT' AND time_frame = '1m'
    ) t2
        ON t1.utc_begin = t2.utc_begin
    WHERE t2.utc_begin != 0;
    "#;
}

#[derive(Clone)]
enum Ex {
    Poloniex,
}

#[derive(Clone)]
struct Config {
    clickhouse_url: String,
    symbols: Vec<String>,
    timeframes: Vec<lib::KlineTimeframe>,
    start_date_millis: i64,
    ex: Ex,
}

impl Config {
    pub fn new_from_envs() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Ok(Self {
            clickhouse_url: std::env::var("CLICKHOUSE_URL")?,
            symbols: std::env::var("CONFIG_SYMBOLS")?
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            timeframes: std::env::var("CONFIG_TIMEFRAMES")?
                .split(',')
                .map(lib::KlineTimeframe::new_from_str)
                .collect::<Result<Vec<lib::KlineTimeframe>, _>>()?,
            start_date_millis: chrono::NaiveDate::parse_from_str(
                &std::env::var("START_DATE")?,
                "%Y-%m-%d",
            )?
            .and_hms_opt(0, 0, 0)
            .ok_or("bad-ts")?
            .and_utc()
            .timestamp_millis(),
            ex: match std::env::var("EX")?.as_str() {
                "poloniex" => Ex::Poloniex,
                _ => panic!("unsupported exchange"),
            },
        })
    }
}
