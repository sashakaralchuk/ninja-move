use crate::Candle;
use postgres::{Client, NoTls};
use std::env;

fn connect_to_postgres() -> Client {
    match Client::configure()
        .host(env::var("POSTGRES_HOST").unwrap().as_str())
        .port(env::var("POSTGRES_PORT").unwrap().parse::<u16>().unwrap())
        .user(env::var("POSTGRES_USER").unwrap().as_str())
        .password(env::var("POSTGRES_PASSWORD").unwrap().as_str())
        .dbname(env::var("POSTGRES_DBNAME").unwrap().as_str())
        .connect(NoTls)
    {
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
            CREATE TABLE IF NOT EXISTS candles_1m (
                open_time TIMESTAMP NOT NULL,
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
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
            "INSERT INTO public.candles_1m
                (open_time, exchange, symbol, open, close, low, high)
            VALUES {};",
            format!(
                "(to_timestamp({})::timestamp, '{}', '{}', {}, {}, {}, {})",
                open_time, x.exchange, x.symbol, x.open, x.close, x.low, x.high,
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }

    pub fn fetch_last_candles(&mut self, limit: i32, exchange: &str, symbol: &str) -> Vec<Candle> {
        let mut out = vec![];
        let query = format!(
            "
            select open_time, exchange, symbol, high, low, open, close
            from public.candles_1m
            where exchange = '{exchange}' and symbol = '{symbol}'
            order by open_time desc
            limit {limit};
        "
        );
        for row in self.client.query(query.as_str(), &[]).unwrap() {
            // XXX: parse numeric here
            out.push(Candle {
                open_time: row
                    .get::<_, std::time::SystemTime>(0)
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                exchange: row.get::<_, String>(1),
                symbol: row.get::<_, String>(2),
                high: row.get::<_, f32>(3) as f64,
                low: row.get::<_, f32>(4) as f64,
                open: row.get::<_, f32>(5) as f64,
                close: row.get::<_, f32>(6) as f64,
            });
        }
        out
    }
}

pub struct HistoryRow {
    strategy: String,
    cause: String,
    exchange: String,
    symbol: String,
    created_at: u128,
    commit_hash: String,
    trailing_threshold: f64,
    spread: f64,
    open_price: f64,
    profit_abs: f64,
    profit_rel: f64,
    last_price: f64,
}

impl HistoryRow {
    pub fn new(
        strategy: &str,
        cause: &str,
        exchange: &String,
        symbol: &String,
        open_price: f64,
        trailing_threshold: f64,
        spread: f64,
        profit_abs: f64,
        profit_rel: f64,
        last_price: f64,
    ) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let commit_hash = env::var("COMMIT_HASH_STR").unwrap();
        Self {
            strategy: strategy.to_string(),
            cause: cause.to_string(),
            exchange: exchange.clone(),
            symbol: symbol.clone(),
            created_at,
            commit_hash,
            trailing_threshold,
            spread,
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
            CREATE TABLE IF NOT EXISTS history_trades (
                created_at TIMESTAMP NOT NULL,
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
                commit_hash varchar NOT NULL,
                strategy varchar NOT NULL,
                cause varchar NOT NULL,
                open_price real NOT NULL,
                trailing_threshold real NOT NULL,
                profit_abs real NOT NULL,
                profit_rel real NOT NULL,
                last_price real NOT NULL,
                spread real NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert_hist(&mut self, x: &HistoryRow) -> Result<(), String> {
        let query = format!(
            "INSERT INTO public.history_trades
                (created_at, exchange, symbol, commit_hash, strategy, cause, open_price, \
                    trailing_threshold, profit_abs, profit_rel, last_price, spread)
            VALUES {};",
            format!(
                "(to_timestamp({})::timestamp, '{}','{}','{}','{}','{}',{},{},{},{},{},{})",
                x.created_at / 1000,
                x.exchange,
                x.symbol,
                x.commit_hash,
                x.strategy,
                x.cause,
                x.open_price,
                x.trailing_threshold,
                x.profit_abs,
                x.profit_rel,
                x.last_price,
                x.spread,
            ),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }
}

pub struct SpreadRow {
    exchange: String,
    symbol: String,
    value_abs: f64,
}

impl SpreadRow {
    pub fn new(exchange: &String, symbol: &String, value_abs: f64) -> Self {
        Self {
            exchange: exchange.clone(),
            symbol: symbol.clone(),
            value_abs,
        }
    }
}

pub struct SpreadPort {
    client: Client,
}

impl SpreadPort {
    pub fn new_and_connect() -> Self {
        Self {
            client: connect_to_postgres(),
        }
    }

    pub fn create_table(&mut self) {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS public.spreads (
                exchange varchar NOT NULL,
                symbol varchar NOT NULL,
                value_abs real NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        ",
            )
            .unwrap();
    }

    pub fn insert_spread(&mut self, x: &SpreadRow) -> Result<(), String> {
        let query = format!(
            "INSERT INTO public.spreads
                (created_at, exchange, symbol, value_abs)
            VALUES {};",
            format!("(now(), '{}', '{}', {})", x.exchange, x.symbol, x.value_abs),
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => return Ok(v),
            Err(error) => return Err(format!("error: {}", error)),
        }
    }
}
