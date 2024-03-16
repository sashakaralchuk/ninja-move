use crate::Candle;
use postgres::{Client, NoTls};
use std::env;

fn connect_to_postgres() -> Client {
    let conn_str = env::var("POSTGRES_DRIVER_STR").unwrap();
    match Client::connect(&conn_str.as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => panic!("postgres connection error: {}", error),
    }
}

pub struct CandlesPort {
    client: Client,
}

impl CandlesPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS candles_bybit_btcusdt_1m (
                open_time TIMESTAMP NOT NULL,
                open real NOT NULL,
                close real NOT NULL,
                high real NOT NULL,
                low real NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert_candle(&mut self, x: &Candle) -> Result<(), String> {
        let open_time = x.open_time / 1000;
        let query = format!(
            "INSERT INTO public.candles_bybit_btcusdt_1m
                (open_time, open, close, low, high)
            VALUES {};",
            format!(
                "(to_timestamp({})::timestamp, {}, {}, {}, {})",
                open_time, x.open, x.close, x.low, x.high,
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }

    pub fn fetch_last_candles(&mut self, limit: i32) -> Vec<Candle> {
        let mut out = vec![];
        let query = format!(
            "
            select open_time, high, low, open, close
            from public.candles_bybit_btcusdt_1m
            order by open_time desc
            limit {};
        ",
            limit
        );
        for row in self.client.query(query.as_str(), &[]).unwrap() {
            // XXX: parse numeric here
            out.push(Candle {
                open_time: row
                    .get::<_, std::time::SystemTime>(0)
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                high: row.get::<_, f32>(1) as f64,
                low: row.get::<_, f32>(2) as f64,
                open: row.get::<_, f32>(3) as f64,
                close: row.get::<_, f32>(4) as f64,
            });
        }
        out
    }
}

pub struct HistoryRow {
    cause: String,
    created_at: u128,
    commit_hash: String,
    trailing_threshold: f64,
    open_price: f64,
    profit_abs: f64,
    profit_rel: f64,
    last_price: f64,
}

impl HistoryRow {
    pub fn new(
        cause: &str,
        open_price: f64,
        trailing_threshold: f64,
        profit_abs: f64,
        profit_rel: f64,
        last_price: f64,
    ) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        Self {
            cause: cause.to_string(),
            created_at,
            commit_hash,
            trailing_threshold,
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
            CREATE TABLE IF NOT EXISTS history_bybit_btcusdt_1m (
                created_at TIMESTAMP NOT NULL,
                commit_hash varchar NOT NULL,
                cause varchar NOT NULL,
                open_price real NOT NULL,
                trailing_threshold real NOT NULL,
                profit_abs real NOT NULL,
                profit_rel real NOT NULL,
                last_price real NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert_hist(&mut self, x: &HistoryRow) -> Result<(), String> {
        let query = format!(
            "INSERT INTO public.history_bybit_btcusdt_1m
                (created_at, commit_hash, cause, open_price, trailing_threshold, profit_abs, \
                    profit_rel, last_price)
            VALUES {};",
            format!(
                "(to_timestamp({})::timestamp, '{}', '{}', {}, {}, {}, {}, {})",
                x.created_at / 1000,
                x.commit_hash,
                x.cause,
                x.open_price,
                x.trailing_threshold,
                x.profit_abs,
                x.profit_rel,
                x.last_price,
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }
}
