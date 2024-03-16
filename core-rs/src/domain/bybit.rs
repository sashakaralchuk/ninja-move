use tungstenite::{connect, Message};
use url::Url;

use crate::FlatTicker;

const URL_WS: &'static str = "wss://stream.bybit.com/v5/public/spot";

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct RawTickerData {
    symbol: String,
    lastPrice: String,
}

#[derive(serde::Deserialize)]
struct RawTicker {
    ts: u128,
    data: RawTickerData,
}

impl RawTicker {
    fn to_flat(&self) -> FlatTicker {
        FlatTicker {
            ts: self.ts,
            data_symbol: self.data.symbol.clone(),
            data_last_price: self.data.lastPrice.parse::<f64>().unwrap(),
        }
    }
}

pub struct TradingWs {}

impl TradingWs {
    pub fn new() -> Self {
        Self {}
    }

    pub fn listen_tickers(on_message: impl Fn(FlatTicker)) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        let symbol = "BTCUSDT".to_string();
        let subscribe_text = format!(
            "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
            symbol,
        );
        socket.write_message(Message::Text(subscribe_text)).unwrap();
        log::info!("start busy loop for {}", symbol);
        loop {
            let msg = socket.read_message().unwrap();
            let msg_str = msg.to_text().unwrap();
            match serde_json::from_str::<RawTicker>(msg_str) {
                Ok(o) => on_message(o.to_flat()),
                Err(_) => {}
            }
        }
    }
}

pub struct Order {
    pub order_id: String,
    pub open_price: f64,
}

pub struct TradingHttpDebug {}

impl TradingHttpDebug {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_market_order(&self, ticker: &FlatTicker) -> Order {
        let open_price = ticker.data_last_price;
        let order_id = uuid::Uuid::new_v4().to_string();
        log::debug!(
            "create_market_order {} created on price {}",
            order_id,
            open_price
        );
        Order {
            open_price,
            order_id,
        }
    }

    pub fn close_order(&self, order_id: &String) {
        log::debug!("close_order {}", order_id);
    }

    pub fn amend_order(&self, order_id: &String, trailing_threshlod: f64) {
        log::debug!("amend_order {} to price {}", order_id, trailing_threshlod);
    }
}
