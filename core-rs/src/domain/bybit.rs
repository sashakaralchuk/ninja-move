use hmac::{Hmac, Mac};
use sha2::Sha256;
use tungstenite::{connect, Message};
use url::Url;

use crate::{FlatDepth, FlatTicker};

const URL_WS: &str = "wss://stream.bybit.com/v5/public/spot";

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct RawTickerData {
    symbol: String,
    lastPrice: String,
}

#[derive(serde::Deserialize)]
struct RawTicker {
    ts: i64,
    data: RawTickerData,
}

impl RawTicker {
    fn to_flat(&self) -> FlatTicker {
        FlatTicker {
            ts_millis: self.ts,
            data_symbol: self.data.symbol.clone(),
            data_last_price: self.data.lastPrice.parse::<f64>().unwrap(),
            exchange: "bybit".to_string(),
        }
    }
}

#[derive(serde::Deserialize)]
pub struct RawDepthData {
    pub u: u64,
    pub a: Vec<[String; 2]>,
    pub b: Vec<[String; 2]>,
}

#[derive(serde::Deserialize)]
pub struct RawDepth {
    pub data: RawDepthData,
}

impl RawDepth {
    fn to_flat(&self) -> FlatDepth {
        FlatDepth {
            update_id: self.data.u,
            asks: self.data.a.clone(),
            bids: self.data.b.clone(),
        }
    }
}

pub enum EventWs {
    Depth(FlatDepth),
    Ticker(FlatTicker),
}

pub struct ConfigWs {
    symbol: String,
    listen_depth: bool,
    listen_tickers: bool,
}

impl ConfigWs {
    pub fn new(symbol: String, listen_depth: bool, listen_tickers: bool) -> Self {
        Self {
            symbol,
            listen_depth,
            listen_tickers,
        }
    }
}

#[derive(Default)]
pub struct TradingWs {}

impl TradingWs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn listen_tickers(on_message: impl Fn(FlatTicker), symbol: &String) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        let subscribe_text = format!(
            "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
            symbol,
        );
        socket.write_message(Message::Text(subscribe_text)).unwrap();
        log::info!("start busy loop for {}", symbol);
        loop {
            let msg = socket.read_message().unwrap();
            let msg_str = msg.to_text().unwrap();
            if let Ok(o) = serde_json::from_str::<RawTicker>(msg_str) {
                on_message(o.to_flat());
            }
        }
    }

    pub fn listen_depth(on_message: impl Fn(RawDepth), symbol: &String) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        let subscribe_text =
            format!("{{\"op\": \"subscribe\", \"args\": [\"orderbook.200.{symbol}\"]}}",);
        socket.write_message(Message::Text(subscribe_text)).unwrap();
        log::info!("start busy loop for {}", symbol);
        loop {
            let msg = socket.read_message().unwrap();
            let msg_str = msg.to_text().unwrap();
            if let Ok(o) = serde_json::from_str::<RawDepth>(msg_str) {
                on_message(o);
            }
        }
    }

    pub fn listen_ws(config: &ConfigWs, on_message: impl FnMut(EventWs)) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        if config.listen_depth {
            log::info!("subscribe to depth stream");
            let subscribe_text = format!(
                "{{\"op\": \"subscribe\", \"args\": [\"orderbook.200.{}\"]}}",
                config.symbol,
            );
            socket.write_message(Message::Text(subscribe_text)).unwrap();
        }
        if config.listen_tickers {
            log::info!("subscribe to tickers stream");
            let subscribe_text = format!(
                "{{\"op\": \"subscribe\", \"args\": [\"tickers.{}\"]}}",
                config.symbol
            );
            socket.write_message(Message::Text(subscribe_text)).unwrap();
        }
        log::info!("start busy loop for {}", config.symbol);
        let mut f = on_message;
        loop {
            let msg = socket.read_message().unwrap();
            let msg_str = msg.to_text().unwrap();
            if let Ok(o) = serde_json::from_str::<RawDepth>(msg_str) {
                f(EventWs::Depth(o.to_flat()));
            }
            if let Ok(o) = serde_json::from_str::<RawTicker>(msg_str) {
                f(EventWs::Ticker(o.to_flat()));
            }
        }
    }
}

pub struct BybitOrder {
    pub avg_fill_price: f64,
}

pub trait TradingHttpTrait {
    fn new_from_envs() -> Self;
    fn fetch_balance(&self) -> Result<BybitBalance, &str>;
    fn place_market_order<'a>(
        &self,
        qty_quote: f64,
        symbol: &str,
        side: &str,
    ) -> Result<BybitOrder, &'a str>;
}

pub struct TradingHttpDebug {}

impl TradingHttpTrait for TradingHttpDebug {
    fn new_from_envs() -> Self {
        Self {}
    }

    fn fetch_balance(&self) -> Result<BybitBalance, &str> {
        Ok(BybitBalance {
            btc: 0.0,
            usdc: 0.0,
        })
    }

    fn place_market_order<'a>(
        &self,
        qty_quote: f64,
        symbol: &str,
        side: &str,
    ) -> Result<BybitOrder, &'a str> {
        log::debug!("place_market_order ({},{},{})", symbol, side, qty_quote);
        Ok(BybitOrder {
            avg_fill_price: 0.0,
        })
    }
}

enum HttpMethod {
    Get,
    Post,
}

#[derive(serde::Deserialize, Debug)]
struct ResBalanceResultItem {
    coin: String,
    free: String,
}

#[derive(serde::Deserialize, Debug)]
struct ResBalanceResult {
    balances: Vec<ResBalanceResultItem>,
}

#[derive(serde::Deserialize, Debug)]
struct ResBalance {
    result: ResBalanceResult,
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize, Debug)]
struct ResOrdersHistoryResultItem {
    orderId: String,
    avgPrice: String,
}

#[derive(serde::Deserialize, Debug)]
struct ResOrdersHistoryResult {
    list: Vec<ResOrdersHistoryResultItem>,
}

#[derive(serde::Deserialize, Debug)]
struct ResOrdersHistory {
    result: ResOrdersHistoryResult,
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize, Debug)]
struct ResPlaceOrderResult {
    orderId: String,
}

#[derive(serde::Deserialize, Debug)]
struct ResPlaceOrder {
    result: ResPlaceOrderResult,
}

#[derive(Debug)]
pub struct BybitBalance {
    pub btc: f64,
    pub usdc: f64,
}

pub struct TradingHttp {
    api_key: String,
    api_secret: String,
    recv_window: String,
}

impl TradingHttp {
    fn gen_request(
        &self,
        method: HttpMethod,
        path: &str,
        signature: String,
        timestamp: String,
        body: Option<String>,
    ) -> reqwest::blocking::RequestBuilder {
        let url_base = "https://api.bybit.com";
        let url = format!("{}{}", url_base, path);
        let client = reqwest::blocking::Client::new();
        let mut req = match method {
            HttpMethod::Get => client.get(url),
            HttpMethod::Post => client.post(url),
        };
        req = match body {
            Some(v) => req.body(v),
            None => req,
        };
        req.header("X-BAPI-API-KEY", self.api_key.as_str())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-SIGN-TYPE", "2")
            .header("X-BAPI-TIMESTAMP", timestamp.as_str())
            .header("X-BAPI-RECV-WINDOW", self.recv_window.as_str())
            .header("Content-Type", "application/json")
    }

    fn gen_signature(&self, timestamp: &String, payload_str: &String) -> String {
        let param_str = format!(
            "{}{}{}{}",
            timestamp, self.api_key, self.recv_window, payload_str
        );
        let secret = self.api_secret.as_bytes();
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(param_str.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn fetch_order(&self, order_id: &str) -> Result<BybitOrder, &str> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let signature = self.gen_signature(&timestamp, &String::new());
        let res = self
            .gen_request(
                HttpMethod::Get,
                "/spot/v3/private/history-orders",
                signature,
                timestamp,
                None,
            )
            .send();
        match res {
            Ok(v) => {
                let status_code = v.status().as_u16();
                if status_code != 200 {
                    return Err("wrong get order status");
                }
                let body = match serde_json::from_str::<ResOrdersHistory>(&v.text().unwrap()) {
                    Ok(v) => v,
                    Err(_) => return Err("couldnt parse balances body"),
                };
                for order_raw in body.result.list.iter() {
                    if order_raw.orderId == order_id {
                        let o = BybitOrder {
                            avg_fill_price: order_raw.avgPrice.parse::<f64>().unwrap(),
                        };
                        return Ok(o);
                    }
                }
                Err("order have not found")
            }
            Err(_) => Err("order get error"),
        }
    }
}

impl TradingHttpTrait for TradingHttp {
    fn new_from_envs() -> Self {
        let api_key = std::env::var("BYBIT_API_KEY").unwrap();
        let api_secret = std::env::var("BYBIT_API_SECRET").unwrap();
        let recv_window = String::from("5000");
        Self {
            api_key,
            api_secret,
            recv_window,
        }
    }

    fn fetch_balance(&self) -> Result<BybitBalance, &str> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let signature = self.gen_signature(&timestamp, &String::from(""));
        let res = self
            .gen_request(
                HttpMethod::Get,
                "/spot/v3/private/account",
                signature,
                timestamp,
                None,
            )
            .send();
        match res {
            Ok(v) => {
                let status_code = v.status().as_u16();
                if status_code != 200 {
                    return Err("wrong fetch balance status");
                }
                let body_raw = v.text().unwrap();
                let body = match serde_json::from_str::<ResBalance>(&body_raw) {
                    Ok(v) => v,
                    Err(_) => return Err("couldnt parse balances body"),
                };
                let mut balance = BybitBalance {
                    btc: 0.0,
                    usdc: 0.0,
                };
                for b in body.result.balances.iter() {
                    match b.coin.as_str() {
                        "BTC" => {
                            balance.btc = b.free.parse::<f64>().unwrap();
                        }
                        "USDC" => {
                            balance.usdc = b.free.parse::<f64>().unwrap();
                        }
                        _ => {}
                    }
                }
                Ok(balance)
            }
            Err(_) => Err("response error"),
        }
    }

    fn place_market_order<'a>(
        &self,
        qty_quote: f64,
        symbol: &str,
        side: &str,
    ) -> Result<BybitOrder, &'a str> {
        let payload = serde_json::json!({
            "symbol": symbol,
            "orderType": "Market",
            "side": side,
            "orderQty": qty_quote.to_string(),
        })
        .to_string();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let signature = self.gen_signature(&timestamp, &payload);
        let res = self
            .gen_request(
                HttpMethod::Post,
                "/spot/v3/private/order",
                signature,
                timestamp,
                Some(payload),
            )
            .send();
        match res {
            Ok(v) => {
                let status_code = v.status().as_u16();
                if status_code != 200 {
                    return Err("wrong place market order status");
                }
                let body_raw = v.text().unwrap();
                let body = match serde_json::from_str::<ResPlaceOrder>(&body_raw) {
                    Ok(v) => v,
                    Err(_) => return Err("couldnt parse balances body"),
                };
                log::debug!("backoff waiting for order");
                for _ in 0..5 {
                    match self.fetch_order(body.result.orderId.as_str()) {
                        Ok(v) => return Ok(v),
                        Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
                    };
                }
                Err("backoff order havent successed")
            }
            Err(_) => Err("order place response error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    #[test]
    fn test_bybit_fetch_balances_signature() {
        let api_key = "lala44";
        let api_secret = "lala77";
        let recv_window = "5000";
        let timestamp = "1710588837615";
        let payload = "";
        let param_str = format!("{}{}{}{}", timestamp, api_key, recv_window, payload);
        assert_eq!(param_str, "1710588837615lala445000");
        let signature = {
            let secret = api_secret.as_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
            mac.update(param_str.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(
            signature,
            "20dc4146a949cff021690fa177630ed713b72b030f397b04a56a204b03c9dd18"
        );
    }

    #[test]
    fn test_bybit_place_order_signature() {
        let qty = 100.44;
        let api_key = "lala44";
        let api_secret = "lala77";
        let recv_window = "5000";
        let timestamp = "1710588837615";
        let payload_str = format!(
            "{{\"symbol\": \"BTCUSDC\", \"orderType\": \"Market\", \"side\": \"Buy\", \"orderQty\": \"{}\"}}",
            qty,
        );
        let param_str = format!("{}{}{}{}", timestamp, api_key, recv_window, payload_str);
        assert_eq!(
            param_str.split("{").collect::<Vec<&str>>()[0],
            "1710588837615lala445000"
        );
        assert_eq!(
            param_str,
            "1710588837615lala445000{\"symbol\": \"BTCUSDC\", \"orderType\": \"Market\", \"side\": \"Buy\", \"orderQty\": \"100.44\"}",
        );
        let signature = {
            let secret = api_secret.as_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
            mac.update(param_str.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(
            signature,
            "82f73dd07206485458276de5cca15991d08f3514c8b03de658ede21ba1a93fa7"
        );
    }
}
