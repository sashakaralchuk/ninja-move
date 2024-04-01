use hmac::{Hmac, Mac};
use sha2::Sha256;
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
            exchange: "bybit".to_string(),
        }
    }
}

pub struct TradingWs {}

impl TradingWs {
    pub fn new() -> Self {
        Self {}
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
            match serde_json::from_str::<RawTicker>(msg_str) {
                Ok(o) => on_message(o.to_flat()),
                Err(_) => {}
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
        let signature = self.gen_signature(&timestamp, &format!(""));
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
                return Err("order have not found");
            }
            Err(_) => return Err("order get error"),
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
                return Ok(balance);
            }
            Err(_) => return Err("response error"),
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
                return Err("backoff order havent successed");
            }
            Err(_) => return Err("order place response error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    #[test]
    fn test_bybit_fetch_balances_signature() {
        let api_key = std::env::var("BYBIT_API_KEY").unwrap();
        let api_secret = std::env::var("BYBIT_API_SECRET").unwrap();
        let recv_window = "5000";
        let timestamp = "1710588837615";
        let payload = "";
        let param_str = format!("{}{}{}{}", timestamp, api_key, recv_window, payload);
        assert_eq!(param_str, "1710588837615HlXHFplHqUg0JAFmtt5000");
        let signature = {
            let secret = api_secret.as_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
            mac.update(param_str.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(
            signature,
            "bc4a26c8f131e16d7988a3d34cdf91b99180d049a8d46dca6aa548ea47dca97e"
        );
    }

    #[test]
    fn test_bybit_place_order_signature() {
        let qty = 100.44;
        let api_key = std::env::var("BYBIT_API_KEY").unwrap();
        let api_secret = std::env::var("BYBIT_API_SECRET").unwrap();
        let recv_window = "5000";
        let timestamp = "1710588837615";
        let payload_str = format!(
            "{{\"symbol\": \"BTCUSDC\", \"orderType\": \"Market\", \"side\": \"Buy\", \"orderQty\": \"{}\"}}",
            qty,
        );
        let param_str = format!("{}{}{}{}", timestamp, api_key, recv_window, payload_str);
        assert_eq!(
            param_str.split("{").collect::<Vec<&str>>()[0],
            "1710588837615HlXHFplHqUg0JAFmtt5000"
        );
        assert_eq!(
            param_str,
            "1710588837615HlXHFplHqUg0JAFmtt5000{\"symbol\": \"BTCUSDC\", \"orderType\": \"Market\", \"side\": \"Buy\", \"orderQty\": \"100.44\"}",
        );
        let signature = {
            let secret = api_secret.as_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
            mac.update(param_str.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(
            signature,
            "8cb3c4b951382ef131a3d715ac7d909bad265ad4a06baf65e892e1faa6197a16"
        );
    }
}
