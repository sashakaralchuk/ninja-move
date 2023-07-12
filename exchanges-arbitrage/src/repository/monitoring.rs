use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use postgres::{Client, NoTls, Row, Error as PostgresError};
use chrono::NaiveDateTime;

fn wrap_result<T>(query: Result<T, PostgresError>) -> Result<T, String> {
    match query {
        Ok(v) => return Ok(v),
        Err(error) => return Err(format!("error: {}", error))
    }
}

#[derive(Debug)]
pub struct InsertParam {
    pub symbol: String,
    pub exchange: String,
    pub last_bid: u64,
    pub last_ask: u64,
}

pub struct Repository {
    client: Client,
}

impl Repository {
    pub fn new_and_connect() -> Self {
        let conn_str = env::var("POSTGRES_DRIVER_STR").unwrap();
        match Client::connect(&conn_str.as_str(), NoTls) {
            Ok(client) => Self{client},
            Err(error) => panic!("postgres connect error: {}", error)
        }
    }

    pub fn create_table(&mut self) {
        // XXX: store order book as differences, not as a snapshot
        self.client.batch_execute("
            CREATE TABLE IF NOT EXISTS monitoring_spread (
                symbol VARCHAR (50) NOT NULL,
                exchange VARCHAR (50) NOT NULL,
                last_bid integer NOT null,
                last_ask integer NOT null,
                timestamp TIMESTAMP NOT NULL
            );
        ").unwrap();
    }

    pub fn insert_batch(
        &mut self, list: &Vec<InsertParam>,
    ) -> Result<(), String> {
        let now_str = chrono::prelude::Utc::now().to_string();
        let values = list.iter()
            .map(|x| format!(
                "('{}', '{}', {}, {}, '{}')",
                x.symbol, x.exchange, x.last_bid, x.last_ask, now_str,
            ))
            .collect::<Vec<String>>();
        let query = format!(
            "INSERT INTO monitoring_spread
                (symbol, exchange, last_bid, last_ask, timestamp)
            VALUES {};",
            values.join(","),
        );
        wrap_result::<()>(self.client.batch_execute(query.as_str()))
    }

    pub fn spread_mat_view(
        &mut self,
        start: &NaiveDateTime,
        end: &NaiveDateTime,
    ) {
        let name = format!(
            "spread_{}_{}_{}",
            start.format("%Y_%m_%d").to_string(),
            start.format("%H").to_string(),
            end.format("%H").to_string(),
        );
        let query_create = format!("
            create materialized view {}
            as (
                with extended as (
                    select
                        symbol,
                        cast(last_bid AS float) / cast(10000 as float) as last_bid,
                        cast(last_ask AS float) / cast(10000 AS float) as last_ask,
                        cast(last_bid + (last_ask - last_bid) / 2 as float) / cast(10000 AS float) as market_price,
                        cast(last_ask - last_bid as float) / cast(10000 AS float) as spread_absolute,
                        cast(last_ask - last_bid as float) / last_ask as spread_relative,
                        cast(last_ask - last_bid as float) / last_ask * 100 as spread_percents,
                        exchange,
                        timestamp
                    from monitoring_spread
                    where last_ask != 0
                        and last_bid != 0
                        and timestamp >= timestamp '{}'
                        and timestamp < timestamp '{}'
                ), min_spread as (
                    select distinct on (symbol) symbol, exchange, spread_percents, last_bid, last_ask, timestamp
                    from extended
                    where spread_percents != 0
                    order by symbol, exchange, spread_percents asc
                ), max_spread as (
                    select distinct on (symbol) symbol, exchange, spread_percents, last_bid, last_ask, timestamp
                    from extended
                    where spread_percents != 0
                    order by symbol, exchange, spread_percents desc
                )
                select
                    min_spread.symbol,
                    min_spread.exchange,
                    min_spread.spread_percents as spread_percents_min,
                    max_spread.spread_percents as spread_percents_max,
                    min_spread.last_bid as last_bid_min,
                    min_spread.last_ask as last_ask_min,
                    min_spread.timestamp as timestamp_min,
                    max_spread.last_bid as last_bid_max,
                    max_spread.last_ask as last_ask_max,
                    max_spread.timestamp as timestamp_max
                from min_spread inner join max_spread
                on min_spread.symbol=max_spread.symbol
                    and min_spread.exchange=max_spread.exchange
            )
            with data;",
            name,
            start.format("%Y-%m-%d %H:%M:%S").to_string(),
            end.format("%Y-%m-%d %H:%M:%S").to_string(),
        );
        match self.client.batch_execute(&format!("select 1 from {};", name)) {
            Ok(_) => log::info!("materialize view {} already created", name),
            Err(_) => {
                self.client.batch_execute(&query_create).unwrap();
                log::info!("materialize view {} created", name);
            },
        };
    }

    pub fn start_end_dates(&mut self) -> Result<
        (NaiveDateTime, NaiveDateTime), (),
    > {
        for row in self.client.query("
            select min(timestamp), max(timestamp)
            from monitoring_spread
        ", &[]).unwrap() {
            let min = row.get::<usize, SystemTime>(0)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let max = row.get::<usize, SystemTime>(1)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            return Ok((
                NaiveDateTime::from_timestamp_millis(min as i64).unwrap(),
                NaiveDateTime::from_timestamp_millis(max as i64).unwrap(),
            ));
        }
        Err(())
    }

    pub fn symbols_amount(&mut self) -> Result<Vec<Row>, String> {
        wrap_result::<Vec<Row>>(self.client.query("
            SELECT symbol, count(*)
            FROM monitoring_spread
            group by symbol
        ", &[]))
    }

    pub fn symbol_raw(&mut self, symbol: &String) -> Result<Vec<Row>, String> {
        let query = format!("
            SELECT symbol, exchange, last_bid, last_ask, timestamp
            FROM monitoring_spread
            where symbol = '{}'
        ", symbol);
        wrap_result::<Vec<Row>>(self.client.query(query.as_str(), &[]))
    }

    pub fn delete_raw(&mut self, symbol: &String) -> Result<(), String> {
        let query = format!("
            DELETE
            FROM monitoring_spread
            WHERE symbol = '{}';
        ", symbol);
        wrap_result::<()>(self.client.batch_execute(query.as_str()))
    }
}

#[derive(Debug)]
pub struct InsertParamV2 {
    pub content: InsertParam,
    pub timestamp: u64,
}

/// Stores order book as difference with timestamps
/// NOTE: for gateio + binance table weight 200mb per hour
pub struct RepositoryV2 {
    client: Client,
}

impl RepositoryV2 {
    pub fn new_and_connect() -> Self {
        let conn_str = env::var("POSTGRES_DRIVER_STR").unwrap();
        match Client::connect(&conn_str.as_str(), NoTls) {
            Ok(client) => Self{client},
            Err(error) => panic!("postgres connect error: {}", error)
        }
    }

    pub fn create_table(&mut self) {
        self.client.batch_execute("
            CREATE TABLE IF NOT EXISTS monitoring_spread_v2 (
                symbol VARCHAR(50) NOT NULL,
                exchange VARCHAR(50) NOT NULL,
                last_bid bigint NOT null,
                last_ask bigint NOT null,
                timestamp TIMESTAMP NOT NULL
            );
        ").unwrap();
    }

    pub fn insert_batch(&mut self, list: &Vec<InsertParamV2>) -> Result<(), String> {
        let values = list.iter()
            .map(|o| {
                let x = &o.content;
                let timestamp = o.timestamp as f64 / 1000.0; // need to be in secs
                format!(
                    "('{}', '{}', {}, {}, to_timestamp({})::timestamp)",
                    x.symbol, x.exchange, x.last_bid, x.last_ask, timestamp,
                )
            })
            .collect::<Vec<String>>();
        let query = format!(
            "INSERT INTO monitoring_spread_v2
                (symbol, exchange, last_bid, last_ask, timestamp)
            VALUES {};",
            values.join(","),
        );
        wrap_result::<()>(self.client.batch_execute(query.as_str()))
    }
}

#[derive(Debug)]
pub struct InsertParamV2Ticker {
    pub symbol: String,
    pub exchange: String,
    pub close_price: u64,
    pub high_price: u64,
    pub low_price: u64,
    pub timestamp: u64,
}

/// Stores tickers
pub struct RepositoryV2Ticker {
    client: Client,
}

impl RepositoryV2Ticker {
    pub fn new_and_connect() -> Self {
        let conn_str = env::var("POSTGRES_DRIVER_STR").unwrap();
        match Client::connect(&conn_str.as_str(), NoTls) {
            Ok(client) => Self{client},
            Err(error) => panic!("postgres connect error: {}", error)
        }
    }

    pub fn create_table(&mut self) {
        self.client.batch_execute("
            CREATE TABLE IF NOT EXISTS monitoring_spread_v2_tickers (
                symbol VARCHAR(50) NOT NULL,
                exchange VARCHAR(50) NOT NULL,
                close_price bigint NOT null,
                high_price bigint NOT null,
                low_price bigint NOT null,
                timestamp TIMESTAMP NOT NULL
            );
        ").unwrap();
    }

    pub fn insert_batch(&mut self, list: &Vec<InsertParamV2Ticker>) -> Result<(), String> {
        let values = list.iter()
            .map(|x| {
                let timestamp = x.timestamp as f64 / 1000.0; // need to be in secs
                format!(
                    "('{}', '{}', {}, {}, {}, to_timestamp({})::timestamp)",
                    x.symbol, x.exchange, x.close_price,
                    x.high_price, x.low_price, timestamp,
                )
            })
            .collect::<Vec<String>>();
        let query = format!(
            "INSERT INTO monitoring_spread_v2_tickers
                (symbol, exchange, close_price, high_price, low_price, timestamp)
            VALUES {};",
            values.join(","),
        );
        wrap_result::<()>(self.client.batch_execute(query.as_str()))
    }
}
