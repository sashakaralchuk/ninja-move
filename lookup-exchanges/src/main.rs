use std::collections::HashMap;

use lib::ClientPublic;
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

mod poloniex_public {
    use crate::lib::{
        millis_to_hr_str, now_millis, ChannelWs, ClientPublic, EventWs, Kline, KlineTimeframe,
        RecentTrade, VBS,
    };
    use async_tungstenite::async_std::connect_async;
    use async_tungstenite::tungstenite::Message;
    use futures::StreamExt;
    use std::error::Error;
    use std::time::Duration;
    use tokio::time::timeout;

    const URL_WS: &str = "wss://ws.poloniex.com/ws/public";

    #[derive(Clone)]
    pub struct Client {}

    impl ClientPublic for Client {
        fn new() -> Self {
            Self {}
        }

        async fn listen_ws_channel_v2<OnMessage, Fut>(
            &self,
            channel: ChannelWs,
            symbols: &[String],
            on_message: OnMessage,
        ) -> Result<(), Box<dyn Error + Send + Sync>>
        where
            OnMessage: FnMut(EventWs) -> Fut,
            Fut: std::future::Future<Output = Result<(), Box<dyn Error>>>,
        {
            let (mut socket, _) = connect_async(URL_WS).await?;
            let channel_str = match channel {
                ChannelWs::Trade => "trades",
            };
            let sumbols_str = symbols
                .iter()
                .map(|s| format!(r#""{}""#, s))
                .collect::<Vec<String>>()
                .join(",");
            let subscribe_text = format!(
                r#"{{"event": "subscribe", "channel": ["{}"], "symbols": [{}]}}"#,
                channel_str, sumbols_str
            );
            log::info!("subscribe_text={subscribe_text}");
            socket.send(Message::Text(subscribe_text)).await?;
            let mut ping_last_millis = now_millis();
            let ping_threshold_millis = 15 * 1000; // NOTE: ping must happens every 30s
            let mut f = on_message;
            loop {
                if now_millis() > (ping_last_millis + ping_threshold_millis) {
                    log::debug!("ping");
                    socket
                        .send(Message::Text(r#"{"event": "ping"}"#.into()))
                        .await?;
                    ping_last_millis = now_millis();
                }
                let msg = match timeout(Duration::from_secs(1), socket.next()).await {
                    Ok(m) => m.ok_or("ws-no-msg")??,
                    Err(_) => continue,
                };
                let msg_str = msg.to_text()?;
                log::debug!("msg_str={msg_str}");
                if serde_json::from_str::<EventWsPingRaw>(msg_str).is_ok() {
                    continue;
                }
                if serde_json::from_str::<EventWsSubscribeRaw>(msg_str).is_ok() {
                    let _ = f(EventWs::Subscribe).await;
                    continue;
                }
                if let Ok(o) = serde_json::from_str::<EventWsTradeRaw>(msg_str) {
                    for trade in o.conv_to_recent_trade_vec() {
                        let _ = f(EventWs::Trade(trade)).await;
                    }
                    continue;
                }
                panic!("unknown msg_str={msg_str}");
            }
        }

        async fn fetch_insert_klines(
            &self,
            client_clickhouse: &clickhouse::Client,
            start_date_millis: i64,
            symbol: &String,
            timeframe: &KlineTimeframe,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let end_millis = now_millis();
            log::info!("load klines symbol={} timeframe={:?}", symbol, timeframe);
            let last_kline_utc_begin = client_clickhouse
                .query(
                    r#"
                            SELECT toUnixTimestamp(utc_begin)
                            FROM default.klines
                            WHERE pair = ? AND time_frame = ?
                            ORDER BY utc_begin DESC
                            LIMIT 1
                            "#,
                )
                .bind(symbol)
                .bind(&timeframe.to_str())
                .fetch::<i32>()?
                .next()
                .await?;
            let start_millis = match last_kline_utc_begin {
                Some(ts) => (ts as i64) * 1000 + timeframe.to_inserval_millis(),
                None => start_date_millis,
            };
            if start_millis >= end_millis - timeframe.to_inserval_millis() {
                return Ok(());
            }
            let klines = fetch_klines(symbol, timeframe.clone(), start_millis, end_millis).await?;
            log::info!(
                "save klines to db symbol={} timeframe={:?} klines.len()={}",
                symbol,
                timeframe,
                klines.len()
            );
            let mut insert_klines = client_clickhouse.insert("klines")?;
            for k in klines.iter() {
                insert_klines.write(&k.to_row()?).await?;
            }
            insert_klines.end().await?;
            Ok(())
        }
    }

    /// Fetches all klines except current.
    async fn fetch_klines(
        symbol: &str,
        timeframe: KlineTimeframe,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<Vec<Kline>, Box<dyn Error + Send + Sync>> {
        let interval_str = match timeframe {
            KlineTimeframe::Minute(1) => "MINUTE_1".to_string(),
            KlineTimeframe::Minute(15) => "MINUTE_15".to_string(),
            KlineTimeframe::Hour(1) => "HOUR_1".to_string(),
            KlineTimeframe::Day(1) => "DAY_1".to_string(),
            _ => panic!("inproper timeframe={timeframe:?}"),
        };
        let limit = 500;
        let interval_millis = timeframe.to_inserval_millis();
        let window_millis = interval_millis * (limit - 1);
        let end_adj_millis = end_millis / interval_millis * interval_millis - 1;
        log::debug!(
            "start_ts={} end_adj_millis={}",
            millis_to_hr_str(start_millis)?,
            millis_to_hr_str(end_adj_millis)?,
        );
        let mut klines = vec![];
        for i in 0..((end_millis - start_millis) / window_millis + 1) {
            let start_time = start_millis + i * window_millis;
            let end_time = i64::min(start_millis + (i + 1) * window_millis - 1, end_adj_millis);
            let url = format!(
                    "https://api.poloniex.com/markets/{}/candles?interval={}&startTime={}&endTime={}&limit={}",
                    symbol,
                    interval_str,
                    start_time,
                    end_time,
                    limit
                );
            log::debug!("url={url}");
            let res = reqwest::Client::new()
                .get(url)
                .header("Content-Type", "application/json")
                .send()
                .await?;
            let res_text = res.text().await?;
            let mut res_obj: Vec<EventHttpKlineRaw> = serde_json::from_str(&res_text)?;
            res_obj.sort_by(|a, b| a.12.cmp(&b.12));
            klines.extend(
                res_obj
                    .iter()
                    .map(|o| o.to_kline(symbol))
                    .collect::<Result<Vec<Kline>, _>>()?,
            );
            log::info!("res_obj={:?}", res_obj.len());
        }
        log::info!("klines.len()={}", klines.len());
        Ok(klines)
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    pub struct EventWsSubscribeRaw {
        pub event: String,
        pub channel: String,
        pub symbols: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    pub struct EventWsPingRaw {
        pub event: String,
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRaw {
        #[allow(dead_code)]
        pub channel: String,
        pub data: Vec<EventWsTradeRawData>,
    }

    #[allow(non_snake_case)]
    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRawData {
        symbol: String,
        amount: String,
        #[allow(dead_code)]
        quantity: String,
        takerSide: String,
        createTime: i64,
        price: String,
        id: String,
        #[allow(dead_code)]
        ts: i64,
    }

    impl EventWsTradeRaw {
        fn conv_to_recent_trade_vec(&self) -> Vec<RecentTrade> {
            self.data
                .iter()
                .map(|d| RecentTrade {
                    tid: d.id.clone(),
                    pair: d.symbol.clone(),
                    price: d.price.clone(),
                    amount: d.amount.clone(),
                    side: d.takerSide.clone(),
                    timestamp: d.createTime,
                })
                .collect()
        }
    }

    #[derive(serde::Deserialize)]
    pub struct EventHttpKlineRaw(
        #[serde(rename = "low")] String,
        #[serde(rename = "high")] String,
        #[serde(rename = "open")] String,
        #[serde(rename = "close")] String,
        #[serde(rename = "amount")] String,
        #[serde(rename = "quantity")] String,
        #[serde(rename = "buyTakerAmount")] String,
        #[serde(rename = "buyTakerQuantity")] String,
        #[allow(dead_code)]
        #[serde(rename = "tradeCount")]
        i64,
        #[allow(dead_code)]
        #[serde(rename = "ts")]
        i64,
        #[allow(dead_code)]
        #[serde(rename = "weightedAverage")]
        String,
        #[serde(rename = "interval")] String,
        #[serde(rename = "startTime")] i64,
        #[allow(dead_code)]
        #[serde(rename = "closeTime")]
        i64,
    );

    impl EventHttpKlineRaw {
        fn to_kline(&self, symbol: &str) -> Result<Kline, Box<dyn Error + Send + Sync>> {
            let time_frame = match self.11.as_str() {
                "MINUTE_1" => KlineTimeframe::Minute(1),
                "MINUTE_15" => KlineTimeframe::Minute(15),
                "HOUR_1" => KlineTimeframe::Hour(1),
                "DAY_1" => KlineTimeframe::Day(1),
                _ => panic!("unknown interval={}", self.11),
            };
            Ok(Kline {
                time_frame,
                pair: symbol.to_owned(),
                o: self.2.parse()?,
                h: self.1.parse()?,
                l: self.0.parse()?,
                c: self.3.parse()?,
                utc_begin: self.12,
                volume_bs: VBS {
                    buy_base: self.7.parse()?,
                    sell_base: self.5.parse()?,
                    buy_quote: self.6.parse()?,
                    sell_quote: self.4.parse()?,
                },
                last_tid: 0,
            })
        }
    }
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

mod lib {
    use std::error::Error;

    #[derive(Debug, Clone)]
    pub struct RecentTrade {
        pub tid: String,
        pub pair: String,
        pub price: String,
        pub amount: String,
        pub side: String,
        pub timestamp: i64,
    }

    #[derive(Debug, Clone)]
    pub struct Kline {
        pub pair: String,
        pub time_frame: KlineTimeframe,
        pub o: f64,
        pub h: f64,
        pub l: f64,
        pub c: f64,
        pub utc_begin: i64,
        pub volume_bs: VBS,
        pub last_tid: i64,
    }

    #[derive(Debug, Clone, serde::Serialize, Eq, PartialEq, Hash)]
    pub enum KlineTimeframe {
        Minute(i64),
        Hour(i64),
        Day(i64),
    }

    impl KlineTimeframe {
        pub fn new_from_str(s: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
            let n = s[0..s.len() - 1].parse::<i64>()?;
            Ok(match s.chars().last().ok_or("no-last")? {
                'm' => KlineTimeframe::Minute(n),
                'h' => KlineTimeframe::Hour(n),
                'd' => KlineTimeframe::Day(n),
                _ => panic!("unknown time frame s={}", s),
            })
        }

        pub fn to_str(&self) -> String {
            match self {
                KlineTimeframe::Minute(n) => format!("{}m", n),
                KlineTimeframe::Hour(n) => format!("{}h", n),
                KlineTimeframe::Day(n) => format!("{}d", n),
            }
        }

        pub fn to_inserval_millis(&self) -> i64 {
            match self {
                KlineTimeframe::Minute(n) => n * 60 * 1000,
                KlineTimeframe::Hour(n) => n * 60 * 60 * 1000,
                KlineTimeframe::Day(n) => n * 24 * 60 * 60 * 1000,
            }
        }
    }

    #[allow(clippy::upper_case_acronyms)]
    #[derive(Debug, Clone)]
    pub struct VBS {
        pub buy_base: f64,
        pub sell_base: f64,
        pub buy_quote: f64,
        pub sell_quote: f64,
    }

    impl Kline {
        pub fn new_from_trade(
            time_frame: KlineTimeframe,
            trade: &RecentTrade,
        ) -> Result<Self, Box<dyn Error>> {
            let utc_begin_millis = trade.timestamp / 60_000 * 60_000;
            let p: f64 = trade.price.parse()?;
            let mut k = Self {
                time_frame,
                pair: trade.pair.clone(),
                o: p,
                h: p,
                l: p,
                c: p,
                utc_begin: utc_begin_millis,
                volume_bs: VBS {
                    buy_base: 0.0,
                    sell_base: 0.0,
                    buy_quote: 0.0,
                    sell_quote: 0.0,
                },
                last_tid: trade.tid.parse::<i64>()? - 1,
            };
            k.apply_recent_trade(trade)?;
            Ok(k)
        }

        pub fn apply_recent_trade(&mut self, trade: &RecentTrade) -> Result<(), Box<dyn Error>> {
            let p: f64 = trade.price.parse()?;
            self.c = p;
            self.h = f64::max(self.h, p);
            self.l = f64::min(self.l, p);
            let tid = trade.tid.parse::<i64>()?;
            if self.last_tid + 1 != tid {
                panic!("unconsistent last_tid={} tid={}", self.last_tid, tid);
            }
            self.last_tid = tid;
            // XXX: adjust f64 calcs
            if trade.side == "buy" {
                self.volume_bs.buy_base = trade.amount.parse()?;
                self.volume_bs.buy_quote = trade.amount.parse::<f64>()? * p;
            } else {
                self.volume_bs.sell_base = trade.amount.parse()?;
                self.volume_bs.sell_quote = trade.amount.parse::<f64>()? * p;
            }
            Ok(())
        }

        pub fn expired(&self, recent_trade: &RecentTrade) -> bool {
            let interval_millis = self.time_frame.to_inserval_millis();
            let current_start_min = recent_trade.timestamp / interval_millis;
            let candle_start_min = self.utc_begin / interval_millis;
            current_start_min > candle_start_min
        }

        pub fn to_row(&self) -> Result<KlineRow, Box<dyn Error + Send + Sync>> {
            Ok(KlineRow {
                pair: self.pair.clone(),
                time_frame: self.time_frame.to_str(),
                o: self.o,
                h: self.h,
                l: self.l,
                c: self.c,
                utc_begin: time::OffsetDateTime::from_unix_timestamp(self.utc_begin / 1000)?,
                volume_bs__buy_base: self.volume_bs.buy_base,
                volume_bs__sell_base: self.volume_bs.sell_base,
                volume_bs__buy_quote: self.volume_bs.buy_quote,
                volume_bs__sell_quote: self.volume_bs.sell_quote,
            })
        }
    }

    #[allow(non_snake_case)]
    #[derive(clickhouse::Row, serde::Serialize)]
    pub struct KlineRow {
        pair: String,
        time_frame: String,
        o: f64,
        h: f64,
        l: f64,
        c: f64,
        #[serde(with = "clickhouse::serde::time::datetime")]
        utc_begin: time::OffsetDateTime,
        volume_bs__buy_base: f64,
        volume_bs__sell_base: f64,
        volume_bs__buy_quote: f64,
        volume_bs__sell_quote: f64,
    }

    pub enum ChannelWs {
        Trade,
    }

    pub enum EventWs {
        Subscribe,
        Trade(RecentTrade),
    }

    pub trait ClientPublic {
        fn new() -> Self;

        async fn listen_ws_channel_v2<OnMessage, Fut>(
            &self,
            channel: ChannelWs,
            symbols: &[String],
            on_message: OnMessage,
        ) -> Result<(), Box<dyn Error + Send + Sync>>
        where
            OnMessage: FnMut(EventWs) -> Fut,
            Fut: std::future::Future<Output = Result<(), Box<dyn Error>>>;

        fn fetch_insert_klines(
            &self,
            client_clickhouse: &clickhouse::Client,
            start_date_millis: i64,
            symbol: &String,
            timeframe: &KlineTimeframe,
        ) -> impl std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>;
    }

    pub fn now_millis() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    pub fn millis_to_hr_str(m: i64) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(chrono::DateTime::from_timestamp_millis(m)
            .ok_or("bad-ts-in-to-hr")?
            .to_string())
    }

    #[cfg(test)]
    mod test {
        use std::error::Error;

        fn create_recent_trade(timestamp: i64) -> super::RecentTrade {
            super::RecentTrade {
                tid: "1".into(),
                pair: "BTC_USDT".into(),
                price: "10000".into(),
                amount: "1".into(),
                side: "buy".into(),
                timestamp,
            }
        }

        #[test]
        fn test_handle_kline_expired() -> Result<(), Box<dyn Error>> {
            let th = 1739645880000;
            let t1 = create_recent_trade(th);
            let t2 = create_recent_trade(th + 1);
            let t3 = create_recent_trade(th + 59_999);
            let t4 = create_recent_trade(th + 60_000);
            let k1 = super::Kline::new_from_trade(super::KlineTimeframe::Minute(1), &t1)?;
            assert_eq!(k1.expired(&t2), false);
            assert_eq!(k1.expired(&t3), false);
            assert_eq!(k1.expired(&t4), true);
            let k2 = super::Kline::new_from_trade(super::KlineTimeframe::Minute(1), &t2)?;
            assert_eq!(k2.expired(&t3), false);
            assert_eq!(k2.expired(&t4), true);
            Ok(())
        }
    }
}
