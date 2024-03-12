use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::collections::HashMap;

use async_tungstenite::tokio::connect_async;
use futures::prelude::*;
use tungstenite::{connect, Message};
use url::Url;
use sha2::{Sha512, Digest};
use hmac::{Hmac, Mac};

use crate::timestamp_secs;

const URL_WS: &'static str = "wss://api.gateio.ws/ws/v4/";
const URL_HTTP: &'static str = "https://api.gateio.ws";

#[derive(serde::Deserialize, Debug)]
pub struct DepthResult {
    pub u: u64,
    pub t: u64,
    pub s: String,
    pub a: Vec<[String; 2]>,
    pub b: Vec<[String; 2]>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Depth {
    pub result: DepthResult,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct TickerResult {
    pub currency_pair: String,
    pub last: String,
    pub lowest_ask: String,
    pub highest_bid: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Ticker {
    pub time_ms: u64,
    pub result: TickerResult,
}

#[derive(serde::Deserialize, Debug)]
pub struct OrderResult {
    pub id: String,
    pub event: String,
    pub finish_as: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct Order {
    pub result: Vec<OrderResult>,
}

#[derive(Debug, Clone)]
struct TradingError {
    message: String,
    status_code: u16,
}

impl std::fmt::Display for TradingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TradingError, message: {}, status_code: {}",
            self.message,
            self.status_code,
        )
    }
}

impl std::error::Error for TradingError {}

#[derive(Clone)]
pub struct Signer {
    pub key: String,
    secret: String,
}

impl Signer {
    pub fn new(key: String, secret: String) -> Self {
        Self { key, secret }
    }

    pub fn new_from_envs() -> Self {
        let key = std::env::var("GATEIO_API_KEY").unwrap();
        let secret = std::env::var("GATEIO_API_SECRET").unwrap();
        Self::new(key, secret)
    }

    pub fn sign_http<'b>(
        &self,
        method: &'static str,
        path: &'b str,
        query: &String,
        body: &String,
    ) -> (String, f64) {
        let timestamp = timestamp_secs();
        let hashed_payload = {
            let mut hasher_2 = Sha512::new();
            hasher_2.update(body.as_bytes());
            format!("{:x}", hasher_2.finalize())
        };
        let payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, path, query, hashed_payload, timestamp,
        );
        let signature = {
            let secret = self.secret.as_bytes();
            let mut mac = Hmac::<Sha512>::new_from_slice(secret).unwrap();
            mac.update(payload.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        (signature, timestamp)
    }

    pub fn sign_ws(
        &self,
        channel: &'static str,
        event: &'static str,
        timestamp: &u64,
    ) -> (String, String) {
        let mut mac = Hmac::<Sha512>::new_from_slice(self.secret.as_bytes())
            .unwrap();
        let payload = format!(
            "channel={}&event={}&time={}",
            channel,
            event,
            timestamp,
        );
        mac.update(payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        (self.key.clone(), signature)
    }
}

// XXX: traits for exchanges trading structs
pub struct TradingHttp {
    signer: Signer,
}

impl TradingHttp {
    pub fn new(signer: Signer) -> Self {
        Self { signer }
    }

    pub fn create_limit_order<'a>(
        &self,
        symbol: &'a str,
        amount: f64,
        price: f64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let path = "/api/v4/spot/orders";
        let body = serde_json::json!({
            "currency_pair": symbol,
            "side": "buy",
            "amount": amount.to_string(),
            "type": "limit",
            "account": "spot",
            "price": price.to_string(),
        }).to_string();
        let (signature, timestamp) = self.signer.sign_http(
            "POST", path, &"".to_string(), &body,
        );

        let request_start = timestamp_secs();
        let response = reqwest::blocking::Client::new()
            .post(&format!("{}{}", URL_HTTP, path))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("KEY", &self.signer.key)
            .header("Timestamp", &timestamp.to_string())
            .header("SIGN", &signature)
            .body(body)
            .send();

        match response {
            Ok(v) => {
                let request_end = timestamp_secs();
                let status_code = v.status().as_u16();
                let response_body: serde_json::Value = v.json().unwrap();
                if status_code != 201 {
                    let message = format!(
                        "wrong status_code, error: {:?}",
                        response_body,
                    );
                    return Err(Box::new(TradingError{message, status_code}))
                }
                let order_id = response_body["id"].as_str().unwrap().to_string();
                log::info!(
                    "limit order made, order_id: {}, \
                    status_code: {}, duration(s): {:.2}",
                    order_id,
                    status_code,
                    request_end - request_start,
                );
                return Ok(order_id)
            }
            Err(error) => return Err(Box::new(error))
        }
    }

    pub fn amend_order<'a>(
        &self,
        symbol: &'a str,
        order_id: &String,
        price: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = format!("/api/v4/spot/orders/{}", order_id);
        let body = serde_json::json!({"price": price.to_string()}).to_string();
        let query_str = format!("currency_pair={}", symbol);
        let (signature, timestamp) = self.signer.sign_http(
            "PATCH", path.as_str(), &query_str, &body,
        );

        let request_start = timestamp_secs();
        let response = reqwest::blocking::Client::new()
            .patch(&format!("{}{}?{}", URL_HTTP, path, query_str))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("KEY", &self.signer.key)
            .header("Timestamp", &timestamp.to_string())
            .header("SIGN", &signature)
            .body(body)
            .send();

        match response {
            Ok(v) => {
                let request_end = timestamp_secs();
                let status_code = v.status().as_u16();
                let response_body: serde_json::Value = v.json().unwrap();
                let order_id = response_body["id"].to_string();
                if status_code != 200 {
                    let message = format!(
                        "wrong status_code, error: {:?}",
                        response_body,
                    );
                    return Err(Box::new(TradingError{message, status_code}))
                }
                log::info!(
                    "amend of order made, order_id: {}, \
                    status_code: {}, duration(s): {:.2}",
                    order_id,
                    status_code,
                    request_end - request_start,
                );
                return Ok(())
            }
            Err(error) => return Err(Box::new(error))
        }
    }

    pub fn assert_balance(&self, params: &Vec<(&'static str, f64)>) {
        let accounts = {
            let path = "/api/v4/spot/accounts";
            let (signature, timestamp) = self.signer.sign_http(
                "GET", path, &"".to_string(), &"".to_string(),
            );
            let response = reqwest::blocking::Client::new()
                .get(&format!("{}{}", URL_HTTP, path))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("KEY", &self.signer.key)
                .header("Timestamp", &timestamp.to_string())
                .header("SIGN", &signature)
                .send();
            response.unwrap()
                .json::<serde_json::Value>()
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|x| (
                    x["currency"].as_str().unwrap()
                        .to_string(),
                    x["available"].as_str().unwrap()
                        .parse::<f64>()
                        .unwrap(),
                ))
                .collect::<HashMap<String, f64>>()
        };
        let tokens_to_check = params.iter()
            .map(|(token, amount)| (token.to_string(), amount.clone()))
            .collect::<HashMap<String, f64>>();
        for token in tokens_to_check.keys() {
            assert!(tokens_to_check[token] < accounts[token])
        }
    }

    pub fn ticker_price<'a>(&self, symbol: &'a str) -> f64 {
        let path = "/api/v4/spot/tickers";
        let query_str = format!("currency_pair={}", symbol);
        let response = reqwest::blocking::Client::new()
            .get(&format!("{}{}?{}", URL_HTTP, path, query_str))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .send();
        let response_body = {
            let body = response.unwrap().text().unwrap();
            serde_json::from_str::<serde_json::Value>(body.as_str()).unwrap()
        };
        response_body[0]["last"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
    }
}

#[derive(serde::Deserialize, Debug)]
struct WsSubscribeResponseResult {
    status: String,
}

#[derive(serde::Deserialize, Debug)]
struct WsSubscribeResponse {
    result: WsSubscribeResponseResult,
}

pub struct TradingWs {
    signer: Signer,
}

impl TradingWs {
    pub fn new(signer: Signer) -> Self {
        Self { signer }
    }

    pub fn listen_orders<'a>(&self, symbol: &'a str, on_message: impl Fn(String)) {
        let channel = "spot.orders";
        let event = "subscribe";
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (key, signature) = self.signer.sign_ws(
            channel, event, &timestamp,
        );
        let subscribe_text = serde_json::json!({
            "time": timestamp,
            "channel": channel,
            "event": event,
            "payload": [symbol],
            "auth": {
                "method": "api_key",
                "KEY": key,
                "SIGN": &signature,
            },
        }).to_string();

        let listen = async {
            let (stream, _) = connect_async(URL_WS).await.unwrap();
            let (mut write, mut read) = stream.split();

            write.send(Message::text(subscribe_text)).await.unwrap();

            let response = serde_json::from_str::<WsSubscribeResponse>(
                read.next().await.unwrap().unwrap().to_string().as_str(),
            ).unwrap();
            if response.result.status.as_str() != "success" {
                log::error!("subscribe has failed, response: {:?}", response);
                return;
            }

            let loop_listen = read.for_each(|message| async {
                let msg_str = message.unwrap().to_string();
                on_message(msg_str);
            });

            let loop_pong = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    write.send(Message::Pong(vec![])).await.unwrap();
                }
            };

            tokio::join!(loop_listen, loop_pong);
        };

        log::info!("start busy loop listening account orders");
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(listen);
    }

    pub fn listen_depth(symbols: &Vec<String>, on_message: impl Fn(Depth)) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        for symbol in symbols.iter() {
            let subscribe_text = serde_json::json!({
                "time": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                "channel": "spot.order_book_update",
                "event": "subscribe",
                "payload": [symbol.as_str(), "100ms"],
            }).to_string();
            socket.write_message(Message::Text(subscribe_text)).unwrap();
        }
        log::info!("start busy loop for {} symbols", symbols.len());
        loop {
            let msg = socket.read_message().unwrap();
            let msg_text = msg.to_text().unwrap();
            match serde_json::from_str::<Depth>(msg_text) {
                Ok(msg_obj) => on_message(msg_obj),
                Err(_) => {},
            }
        }
    }

    pub fn listen_mini_tickers(symbols: &Vec<&str>, on_message: impl Fn(Ticker)) {
        let url_obj = Url::parse(URL_WS).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        let subscribe_text = serde_json::json!({
            "time": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": symbols,
        }).to_string();
        socket.write_message(Message::Text(subscribe_text)).unwrap();
        log::info!("start busy loop for {} tickers", symbols.len());
        loop {
            let msg = socket.read_message().unwrap();
            let msg_text = msg.to_text().unwrap();
            match serde_json::from_str::<Ticker>(msg_text) {
                Ok(msg_obj) => on_message(msg_obj),
                Err(_) => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Sha512, Digest};
    use hmac::{Hmac, Mac};

    #[test]
    fn hash_hello_world() {
        // hashlib.sha512('Hello world!'.encode('utf-8')).hexdigest()
        let hashed_payload_1 = {
            let mut hasher_1 = Sha512::new();
            hasher_1.update("Hello world!".as_bytes());
            format!("{:x}", hasher_1.finalize())
        };
        assert_eq!(
            hashed_payload_1,
            "f6cde2a0f819314cdde55fc227d8d7dae3d28cc556222a0a8ad66d91ccad4aad6094f517a2182360c9aacf6a3dc323162cb6fd8cdffedb0fe038f55e85ffb5b6",
        );
    }

    #[test]
    fn hash_body() {
        let body = "{\"currency_pair\": \"ARB_USDT\", \"side\": \"buy\", \"amount\": \"2\", \"type\": \"limit\", \"account\": \"spot\", \"price\": \"1.17\"}";
        let hashed_payload_2 = {
            let mut hasher_2 = Sha512::new();
            hasher_2.update(body.as_bytes());
            format!("{:x}", hasher_2.finalize())
        };
        assert_eq!(
            hashed_payload_2,
            "1df28880c9ce99e91e1111073aec77527acc2bb2ba495a606e27787f4e5936c48b1e63591c3cf0b7277274cb6064d5d9555a713ebe358ad604b358e17bac74d7",
        );
    }

    #[test]
    fn hex_to_str_and_vise_versa() {
        let s = "6668ed2f7d016c5f12d7808fc4f2d1dc4851622d7f15616de947a823b3ee67d761b953f09560da301f832902020dd1c64f496df37eb7ac4fd2feeeb67d77ba9b";
        let s_bytes_expected: [u8; 64] = [102, 104, 237, 47, 125, 1, 108, 95, 18, 215, 128, 143, 196, 242, 209, 220, 72, 81, 98, 45, 127, 21, 97, 109, 233, 71, 168, 35, 179, 238, 103, 215, 97, 185, 83, 240, 149, 96, 218, 48, 31, 131, 41, 2, 2, 13, 209, 198, 79, 73, 109, 243, 126, 183, 172, 79, 210, 254, 238, 182, 125, 119, 186, 155];

        assert_eq!(hex::decode(s).unwrap(), s_bytes_expected[..]);
        assert_eq!(hex::encode(s_bytes_expected), s);
    }

    #[test]
    fn hash_hmac_sha512() {
        // hmac.new('hello'.encode('utf-8'), 'world'.encode('utf-8'), hashlib.sha512).hexdigest()
        let key = "hello";
        let data = "world";
        let signature = {
            let mut mac = Hmac::<Sha512>::new_from_slice(key.as_bytes()).unwrap();
            mac.update(data.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(signature, "6668ed2f7d016c5f12d7808fc4f2d1dc4851622d7f15616de947a823b3ee67d761b953f09560da301f832902020dd1c64f496df37eb7ac4fd2feeeb67d77ba9b");
    }

    #[test]
    fn hash_gateio_request() {
        // NOTE: below is demo secret
        let secret = "150a14ed5bea6cc731cf86c41566ac427a8db48ef1b9fd626664b3bfbb99071fa4c922f33dde38719b8c8354e2b7ab9d77e0e67fc12843920a712e73d558e197";
        let method = "POST";
        let path = "/api/v4/spot/orders";
        let body = "{\"currency_pair\": \"ARB_USDT\", \"side\": \"buy\", \"amount\": \"2\", \"type\": \"limit\", \"account\": \"spot\", \"price\": \"1.17\"}";
        let timestamp = 1690286727.962889;
        // hashed_payload = hashlib.sha512(body.encode('utf-8')).hexdigest()
        let hashed_payload = {
            let mut hasher_2 = Sha512::new();
            hasher_2.update(body.as_bytes());
            format!("{:x}", hasher_2.finalize())
        };
        let payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, path, "", hashed_payload, timestamp,
        );
        let signature = {
            let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes()).unwrap();
            mac.update(payload.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(timestamp, 1690286727.962889);
        assert_eq!(hashed_payload, "1df28880c9ce99e91e1111073aec77527acc2bb2ba495a606e27787f4e5936c48b1e63591c3cf0b7277274cb6064d5d9555a713ebe358ad604b358e17bac74d7");
        assert_eq!(payload, "POST\n/api/v4/spot/orders\n\n1df28880c9ce99e91e1111073aec77527acc2bb2ba495a606e27787f4e5936c48b1e63591c3cf0b7277274cb6064d5d9555a713ebe358ad604b358e17bac74d7\n1690286727.962889");
        assert_eq!(signature, "e55ca5711f664a78f30f7c51a4f5b2725d3c2378d2ef6d3e6f9b85cbf8a385a5ed7fe5e07eda9cd923ed8ed09cced91e5cb79303044e2a30897bd7e8dac41a52");
    }

    #[test]
    fn hash_gateio_ws() {
        let secret = "150a14ed5bea6cc731cf86c41566ac427a8db48ef1b9fd626664b3bfbb99071fa4c922f33dde38719b8c8354e2b7ab9d77e0e67fc12843920a712e73d558e197";
        let channel = "";
        let event = "";
        let timestamp = 1690384535;
        let signature = {
            let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes()).unwrap();
            let payload = format!("channel={}&event={}&time={}", channel, event, timestamp);
            mac.update(payload.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(signature, "8f077fca7ced0a8ece85f9f6508497f33c490d06f35096d494f2d36b7d3305b7c750d3cdf9f5e53c2fda08f49bd599a9242f2eec4d125ced377ed05bddcc3db8");
    }
}
