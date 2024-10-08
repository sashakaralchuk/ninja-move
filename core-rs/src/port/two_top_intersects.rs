use crate::{connect_to_postgres, Candle, CandleTimeframe};
use postgres::Client;

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
            VALUES (to_timestamp({})::timestamp, '{}', '{}', {}, {}, {}, {});",
            open_time, x.exchange, x.symbol, x.open, x.close, x.low, x.high,
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => Ok(v),
            Err(error) => Err(format!("error: {}", error)),
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
                    .as_millis() as i64,
                exchange: row.get::<_, String>(1),
                symbol: row.get::<_, String>(2),
                high: row.get::<_, f32>(3) as f64,
                low: row.get::<_, f32>(4) as f64,
                open: row.get::<_, f32>(5) as f64,
                close: row.get::<_, f32>(6) as f64,
                timeframe: CandleTimeframe::Hours(1),
            });
        }
        out
    }
}

pub struct SpreadRow {
    exchange: String,
    symbol: String,
    value_abs: f64,
}

impl SpreadRow {
    pub fn new(exchange: &str, symbol: &str, value_abs: f64) -> Self {
        Self {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
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
            VALUES (now(),'{}','{}',{});",
            x.exchange, x.symbol, x.value_abs,
        );
        match self.client.batch_execute(query.as_str()) {
            Ok(v) => Ok(v),
            Err(error) => Err(format!("error: {}", error)),
        }
    }
}
