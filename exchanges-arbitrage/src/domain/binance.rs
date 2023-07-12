use tungstenite::connect;
use url::Url;

const URL_WS: &'static str = "wss://stream.binance.com:9443";

#[allow(non_snake_case)]
#[derive(serde::Deserialize, Debug)]
pub struct DepthData {
    pub b: Vec<[String; 2]>,
    pub a: Vec<[String; 2]>,
    pub u: u64,
    pub E: u64,
}

#[derive(serde::Deserialize, Debug)]
pub struct Depth {
    pub data: DepthData,
    pub stream: String,
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize, Debug, Clone)]
pub struct MiniTicker {
    pub E: u64,
    pub s: String,
    pub c: String,
    pub h: String,
    pub l: String,
}

pub struct TradingHttp {}

impl TradingHttp {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_market_order(&self) {
        log::info!("NOTE: create_market_order will be implemented later")
    }

    pub fn assert_balance(&self, _params: &Vec<(&'static str, f64)>) {
        log::info!("NOTE: assert_balance will be implemented later")
    }
}

pub struct TradingWs {}

impl TradingWs {
    pub fn listen_depth(streams: &Vec<String>, on_message: impl Fn(Depth)) {
        let url_str = format!("{}/stream?streams={}", URL_WS, streams.join("/"));
        let url_obj = Url::parse(&url_str).unwrap();
        let (mut socket, _response) = connect(url_obj).unwrap();
        log::info!("start busy loop for {} symbols", streams.len());
        loop {
            let msg = socket.read_message().unwrap();
            let msg_text = msg.to_text().unwrap().to_string();
            match serde_json::from_str::<Depth>(&msg_text) {
                Ok(msg_obj) => on_message(msg_obj),
                Err(error) => log::warn!("listen_depth error: {}", error),
            };
        }
    }

    /// All Market Mini Tickers Stream
    pub fn listen_mini_tickers_arr(on_message: impl Fn(Vec<MiniTicker>)) {
        let url_str = format!("{}/ws/!miniTicker@arr", URL_WS);
        let url_obj = Url::parse(&url_str).unwrap();
        let (mut socket, _) = connect(url_obj).unwrap();
        log::info!("start busy loop for all mini tickers");
        loop {
            let msg = socket.read_message().unwrap();
            let msg_text = msg.to_text().unwrap();
            match serde_json::from_str::<Vec<MiniTicker>>(msg_text) {
                Ok(msg_obj) => on_message(msg_obj),
                Err(e) => log::warn!("listen_mini_tickers_arr error: {}", e),
            }
        }
    }
}
