use exchanges_arbitrage::domain::bybit::{TradingHttp, TradingHttpTrait};

fn main() {
    let trading = TradingHttp::new_from_envs();
    let mut balance = trading.fetch_balance().unwrap();
    trading
        .place_market_order(balance.usdc, "BTCUSDC", "Buy")
        .unwrap();
    balance = trading.fetch_balance().unwrap();
    trading
        .place_market_order(balance.btc, "BTCUSDC", "Sell")
        .unwrap();
}
