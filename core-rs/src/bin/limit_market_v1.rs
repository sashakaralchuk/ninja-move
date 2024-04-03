/// Scenario
/// 1. Keep binance price up to date
/// 2. On every price update remove previous
///    limit order on gateio and create new one
/// 3. if order is filled do market price trade
/// Requirements
/// 1. 20 USDT on gateio
/// 2. equal amount of token on binance
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use exchanges_arbitrage::domain::{binance, gateio};
use exchanges_arbitrage::{pool, OrderBookCache, TelegramBotPort};

#[derive(serde::Serialize, serde::Deserialize)]
struct BinancePriceUpdate {
    market_price: f64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GateioOrderFilled {}

#[derive(serde::Serialize, serde::Deserialize)]
enum MessageKind {
    BinancePriceUpdate,
    GateioOrderFilled,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ChannelMessage {
    kind: MessageKind,
    content: String,
}

fn perform() {
    let symbol_binance = std::env::var("SYMBOL_BINANCE").unwrap();
    let symbol_gateio = std::env::var("SYMBOL_GATEIO").unwrap();
    let spread_percent = std::env::var("SPREAD_PERCENT")
        .unwrap()
        .parse::<f64>()
        .unwrap();
    let asset_amount = std::env::var("ASSET_AMOUNT")
        .unwrap()
        .parse::<f64>()
        .unwrap();

    fn gen_signer() -> gateio::Signer {
        gateio::Signer::new_from_envs()
    }

    let trading_http_gateio = gateio::TradingHttp::new(gen_signer());
    let trading_ws_gateio = gateio::TradingWs::new(gen_signer());
    let trading_binance = binance::TradingHttp::new();

    let ticker_price_gateio = trading_http_gateio.ticker_price(symbol_gateio.as_str());

    trading_http_gateio.assert_balance(&vec![("USDT", ticker_price_gateio * asset_amount)]);
    trading_binance.assert_balance(&vec![("ARB", asset_amount)]);
    let order_id_gateio = trading_http_gateio
        .create_limit_order(
            symbol_gateio.as_str(),
            asset_amount,
            ticker_price_gateio * 0.75,
        )
        .unwrap();
    let order_id_gateio_observe = order_id_gateio.clone();
    let symbol_gateio_trading = symbol_gateio.clone();

    let (tx, rx_trading) = mpsc::channel();
    let tx_binance = tx.clone();
    let tx_gateio = tx.clone();

    let t_binance = thread::spawn(move || {
        let stream = format!("{}@depth@100ms", symbol_binance.to_lowercase());
        let order_book_arc = Arc::new(Mutex::new(OrderBookCache::new()));
        let last_market_price = Arc::new(Mutex::new(0.0));
        let on_message = |m: binance::Depth| {
            let mut order_book = order_book_arc.lock().unwrap();
            order_book.apply_orders(m.data.u, &m.data.b, &m.data.a);

            if let Some(market_price) = order_book.calc_market_price() {
                let mut last_market_price = last_market_price.lock().unwrap();
                if *last_market_price == market_price {
                    return;
                }
                *last_market_price = market_price;
                let message_content =
                    serde_json::json!(BinancePriceUpdate { market_price }).to_string();
                let message = serde_json::json!(ChannelMessage {
                    kind: MessageKind::BinancePriceUpdate,
                    content: message_content,
                })
                .to_string();
                match tx_binance.send(message) {
                    Ok(_) => {}
                    Err(error) => log::error!("[t_binance] tx_binance send error: {}", error,),
                }
            }
        };
        binance::TradingWs::listen_depth(&vec![stream], on_message);
    });

    let t_gateio = thread::spawn(move || {
        let on_message = |msg_str: String| {
            let message = serde_json::from_str::<gateio::Order>(&msg_str).unwrap();
            let event = &message.result[0];
            let send = event.id == order_id_gateio_observe
                && event.event == "finish"
                && event.finish_as == "filled";
            if !send {
                return;
            }
            let message_to_send = serde_json::json!(ChannelMessage {
                kind: MessageKind::GateioOrderFilled,
                content: String::from(""),
            })
            .to_string();
            match tx_gateio.send(message_to_send) {
                Ok(_) => log::info!("[t_gateio] filled order sent"),
                Err(error) => log::error!("[t_gateio] tx_gateio send error: {}", error,),
            }
        };
        trading_ws_gateio.listen_orders(symbol_gateio.as_str(), on_message);
    });

    let t_trading = thread::spawn(move || loop {
        // HACK: http call is blocking, means that on every price
        //       update only last price must be applied
        //       hack exists to handle occcasion when http request
        //       happens and n price updates appear in channel
        let messages = vec![rx_trading.recv().unwrap()]
            .into_iter()
            .chain(rx_trading.try_iter())
            .collect::<Vec<String>>();
        let mut last_binance_update = None;
        for msg_str in messages {
            log::debug!("[t_trading] msg_str: {}", msg_str);
            let msg_obj = serde_json::from_str::<ChannelMessage>(&msg_str).unwrap();
            match msg_obj.kind {
                MessageKind::BinancePriceUpdate => {
                    last_binance_update = Some(msg_obj.content);
                }
                MessageKind::GateioOrderFilled => {
                    trading_binance.create_market_order();
                    let message = "limit trade on gateio filled";
                    TelegramBotPort::new_from_envs()
                        .notify_pretty("limit-market-v1".to_string(), message.to_string());
                    return;
                }
            }
        }
        let price_update =
            serde_json::from_str::<BinancePriceUpdate>(&last_binance_update.unwrap()).unwrap();
        let next_limit_price = price_update.market_price * (1.0 - spread_percent);
        log::info!(
            "[t_trading] price update, \
            next price: {}, next_limit_price: {}",
            price_update.market_price,
            next_limit_price,
        );
        let amend = trading_http_gateio.amend_order(
            symbol_gateio_trading.as_str(),
            &order_id_gateio,
            next_limit_price,
        );
        match amend {
            Ok(_) => {}
            Err(error) => log::warn!("[t_trading] amend error: {}", error,),
        }
    });

    pool(&vec![t_binance, t_gateio, t_trading]);
}

fn pool_env() {
    let t = thread::spawn(move || perform());
    match t.join() {
        Ok(_) => {}
        Err(_) => {}
    }
    TelegramBotPort::new_from_envs()
        .notify_pretty("limit-market-v1".to_string(), "finished".to_string());
}

fn main() {
    env_logger::init();
    pool_env();
}
