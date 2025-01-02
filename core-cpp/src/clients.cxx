#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include "clients.hpp"

#include <curl/curl.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <spdlog/spdlog.h>
#include <zlib.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>

std::string gen_precision_str(int precision) {
    if (precision == 0) {
        return "1";
    }
    if (precision < 0) {
        throw std::runtime_error(
            fmt::format("unexpected precision={}", precision));
    }
    return fmt::format("0.{}1", std::string(precision - 1, '0'));
}

std::string conv_to_dec_str_v2(double price, std::string tick_size) {
    int ticks_amount = (int)(price / stod(tick_size));
    double price2 = (double)ticks_amount * stod(tick_size);
    if (tick_size.find(".") == std::string::npos) {
        return std::to_string((int)price2);
    } else {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(tick_size.length() - 2)
            << price2;
        return oss.str();
    }
}

static size_t execute_http_req_write_cb(void* contents, size_t size,
                                        size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

nlohmann::json execute_http_get_req(std::string& url) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(readBuffer);
    curl_easy_cleanup(curl);
    return res_obj;
}

nlohmann::json execute_http_get_req(std::string& url, curl_slist* headers) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    res = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(readBuffer);
    curl_easy_cleanup(curl);
    return res_obj;
}

nlohmann::json execute_http_post_req(std::string url, curl_slist* headers,
                                     std::string body) {
    std::string res_buf;
    CURL* curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    CURLcode res_code = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(res_buf);
    curl_easy_cleanup(curl);
    return res_obj;
}

nlohmann::json execute_http_put_req(std::string url, curl_slist* headers,
                                    std::string body) {
    std::string res_buf;
    CURL* curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    CURLcode res_code = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(res_buf);
    curl_easy_cleanup(curl);
    return res_obj;
}

std::string gen_hmac_sha256(const std::string& secret,
                            const std::string& param_str) {
    unsigned char hmac_result[EVP_MAX_MD_SIZE];
    unsigned int hmac_length = 0;
    HMAC(EVP_sha256(), secret.c_str(), secret.size(),
         reinterpret_cast<const unsigned char*>(param_str.c_str()),
         param_str.size(), hmac_result, &hmac_length);
    std::ostringstream oss;
    for (unsigned int i = 0; i < hmac_length; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(hmac_result[i]);
    }
    return oss.str();
}

std::string gen_hmac_sha512(const std::string& secret,
                            const std::string& param_str) {
    unsigned char hmac_result[EVP_MAX_MD_SIZE];
    unsigned int hmac_length = 0;
    HMAC(EVP_sha512(), secret.c_str(), secret.size(),
         reinterpret_cast<const unsigned char*>(param_str.c_str()),
         param_str.size(), hmac_result, &hmac_length);
    std::ostringstream oss;
    for (unsigned int i = 0; i < hmac_length; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(hmac_result[i]);
    }
    return oss.str();
}

bool str_starts_with(std::string s1, std::string s2) {
    if (s1.length() < s2.length()) {
        return false;
    }
    return s1.substr(0, s2.length()).compare(s2) == 0;
}

bool str_ends_with(std::string s1, std::string s2) {
    if (s1.length() < s2.length()) {
        return false;
    }
    return s1.substr(s1.length() - s2.length(), s2.length()).compare(s2) == 0;
}

ClientPublic::ClientPublic(std::string ex_, std::string kind_,
                           hv::EventLoopPtr loop)
    : WebSocketClient(loop) {
    ex = ex_;
    kind = kind_;
}

ClientPublic::~ClientPublic() {}

void ClientPublic::init_idle_(std::string& url) {
    ws_onopen_received = false;
    ws_onclose_received = false;
    onopen = [&]() {
        SPDLOG_INFO("{} {} public onopen", ex, kind);
        ws_onopen_received = true;
    };
    onclose = [&]() {
        SPDLOG_INFO("{} {} public onclose", ex, kind);
        ws_onclose_received = true;
    };
    onmessage = [=](const std::string& msg) { handle_onmessage(msg); };
    setPingInterval(10000);
    reconn_setting_t reconn;
    reconn_setting_init(&reconn);
    reconn.min_delay = 100;
    reconn.max_delay = 1000;
    reconn.delay_policy = 2;
    setReconnect(&reconn);
    http_headers headers;
    open(url.c_str(), headers);
}

ClientPrivate::ClientPrivate(std::string ex_, std::string kind_,
                             hv::EventLoopPtr loop)
    : WebSocketClient(loop) {
    is_subscribed_to_private_channels = false;
    ex = ex_;
    kind = kind_;
    fut_exchange_info = {};
    spot_exchange_info = {};
}
ClientPrivate::~ClientPrivate() {}

void ClientPrivate::init_idle_(std::string& url) {
    ws_onopen_received = false;
    ws_onclose_received = false;
    onopen = [&]() {
        SPDLOG_INFO("{} {} private onopen", ex, kind);
        ws_onopen_received = true;
    };
    onclose = [&]() {
        SPDLOG_INFO("{} {} private onclose", ex, kind);
        ws_onclose_received = true;
    };
    onmessage = [=](const std::string& msg) { handle_onmessage(msg); };
    setPingInterval(10000);
    reconn_setting_t reconn;
    reconn_setting_init(&reconn);
    reconn.min_delay = 100;
    reconn.max_delay = 1000;
    reconn.delay_policy = 2;
    setReconnect(&reconn);
    http_headers headers;
    open(url.c_str(), headers);
}

int ClientPrivate::get_orders_len() { return orders.size(); }

Order ClientPrivate::get_last_order() {
    if (orders.size() == 0) {
        throw std::runtime_error("no orders");
    }
    return orders[orders.size() - 1];
}

void ClientPrivate::clear_orders() {
    orders.clear();
    spdlog::info("orders cleared");
}

std::string rstrip_zeros(std::string s) {
    int j = s.length() - 1;
    while (j >= 0 && s[j] == '0') {
        j--;
    }
    return s.substr(0, j + 1);
}

long now_millis() {
    return std::chrono::system_clock::now().time_since_epoch().count() / 1000;
}

std::string time_point_to_str(std::chrono::system_clock::time_point tp,
                              std::string format_str) {
    std::time_t now_raw = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream now_oss;
    now_oss << std::put_time(std::gmtime(&now_raw), format_str.c_str());
    return now_oss.str();
}

std::string replace_all(std::string s, std::string from, std::string to) {
    size_t start_pos = 0;
    while ((start_pos = s.find(from, start_pos)) != std::string::npos) {
        s.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
    return s;
}

///
/// Copy of core-rs/trade_contango_arbitrage.SpreadsMap.conv_to_symbol_int_1_v2.
/// Function is needed to make conversion: ("1000000PEPEUSDT", 100.0) ->
/// ("PEPEUSDT", 0.00010)
///
std::tuple<std::string, double> conv_symbol_price_to_atomic_v1(
    std::string symbol, double price) {
    std::string s_int = replace_all(replace_all(symbol, "_", ""), "-", "");
    if (s_int[0] != '1') {
        return std::make_tuple(s_int, price);
    }
    int zeros_amount = 0;
    for (int i = 1; i < s_int.length(); i++) {
        if (s_int[i] == '0') {
            zeros_amount += 1;
        }
    }
    if (zeros_amount == 0) {
        return std::make_tuple(s_int, price);
    }
    return std::make_tuple(s_int.substr(zeros_amount + 1, s_int.length()),
                           price / std::pow(10.0, zeros_amount));
}

std::string ClientPrivate::conv_size_to_str(double size) {
    if (size < 0.0) {
        throw std::runtime_error(fmt::format("unexpected size=", size));
    }
    if (size - (int)size > 0) {
        return rstrip_zeros(std::to_string(size));
    } else {
        return std::to_string((int)size);
    }
}

ClientPublicGateio::ClientPublicGateio(std::string kind)
    : ClientPublic("gateio", kind) {}

void ClientPublicGateio::init_idle() {
    if (kind == "fut") {
        std::string url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        init_idle_(url);
    } else if (kind == "spot") {
        std::string url = "wss://api.gateio.ws/ws/v4/";
        init_idle_(url);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicGateio::subscribe_to_trades(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "time" : %d,
                "channel" : "futures.trades",
                "event": "subscribe",
                "payload" : ["%s"]
            })";
        int ts_secs =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000 /
            1000;
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        int ts_secs =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000 /
            1000;
        std::string t_template = R"({
                "time": %d,
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": ["%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicGateio::subscribe_to_depth(std::string& symbol) {
    int ts_secs = std::chrono::system_clock::now().time_since_epoch().count() /
                  1000 / 1000;
    if (kind == "fut") {
        send(fmt::format(
            R"({{
                "time": {},
                "channel" : "futures.order_book_update",
                "event": "subscribe",
                "payload" : ["{}", "100ms", "100"]
            }})",
            ts_secs, symbol));
    } else if (kind == "spot") {
        send(fmt::format(
            R"({{
                "time": {},
                "channel": "spot.order_book_update",
                "event": "subscribe",
                "payload": ["{}", "100ms"]
            }})",
            ts_secs, symbol));
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicGateio::unsubscribe_from_depth(std::string& symbol) {
    int ts_secs = std::chrono::system_clock::now().time_since_epoch().count() /
                  1000 / 1000;
    if (kind == "fut") {
        std::string t_template = R"({
                "time": %d,
                "channel" : "futures.order_book_update",
                "event": "unsubscribe",
                "payload" : ["%s", "100ms", "100"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        // TODO: format such strings like in spdlog
        std::string t_template = R"({
                "time": %d,
                "channel": "spot.order_book_update",
                "event": "unsubscribe",
                "payload": ["%s", "100ms"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicGateio::ping() {}

Ticker ClientPublicGateio::fetch_ticker(std::string& symbol) {
    long start_millis = now_millis();
    if (kind == "fut") {
        std::string url_str = fmt::format(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={}",
            symbol);
        nlohmann::json res_obj = execute_http_get_req(url_str);
        if (res_obj.size() != 1) {
            throw std::runtime_error(
                fmt::format("{} {} res_obj.size()", ex, kind));
        }
        SPDLOG_DEBUG("{} {} fetch_ticker dur={} res_obj={}", ex, kind,
                     now_millis() - start_millis, res_obj.dump());
        return Ticker{
            .s = res_obj[0]["contract"],
            .bid = stod((std::string)res_obj[0]["highest_bid"]),
            .ask = stod((std::string)res_obj[0]["lowest_ask"]),
        };
    } else if (kind == "spot") {
        std::string url_str = fmt::format(
            "https://api.gateio.ws/api/v4/spot/tickers?currency_pair={}",
            symbol);
        nlohmann::json res_obj = execute_http_get_req(url_str);
        if (res_obj.size() != 1) {
            throw std::runtime_error(
                fmt::format("{} {} res_obj.size()", ex, kind));
        }
        SPDLOG_DEBUG("{} {} fetch_ticker dur={} res_obj={}", ex, kind,
                     now_millis() - start_millis, res_obj.dump());
        return Ticker{
            .s = res_obj[0]["currency_pair"],
            .bid = stod((std::string)res_obj[0]["highest_bid"]),
            .ask = stod((std::string)res_obj[0]["lowest_ask"]),
        };
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

std::vector<Depth> ClientPublicGateio::fetch_depth_snapshot(
    std::string& symbol) {
    if (kind == "fut") {
        std::string url =
            "https://api.gateio.ws/api/v4/futures/usdt/"
            "order_book?limit=100&with_id=true&contract=" +
            symbol;
        nlohmann::json obj = execute_http_get_req(url);
        std::vector<std::tuple<std::string, double>> bids;
        for (auto& b : obj["bids"]) {
            bids.push_back({b["p"], b["s"]});
        }
        std::vector<std::tuple<std::string, double>> asks;
        for (auto& a : obj["asks"]) {
            asks.push_back({a["p"], a["s"]});
        }
        return std::vector<Depth>{
            Depth{.u = obj["id"], .asks = asks, .bids = bids}};
    } else if (kind == "spot") {
        std::string url =
            "https://api.gateio.ws/api/v4/spot/"
            "order_book?limit=100&with_id=true&currency_pair=" +
            symbol;
        nlohmann::json obj = execute_http_get_req(url);
        std::vector<std::tuple<std::string, double>> bids;
        for (auto& b : obj["bids"]) {
            bids.push_back({b[0], stod((std::string)b[1])});
        }
        std::vector<std::tuple<std::string, double>> asks;
        for (auto& a : obj["asks"]) {
            asks.push_back({a[0], stod((std::string)a[1])});
        }
        return std::vector<Depth>{
            Depth{.u = obj["id"], .asks = asks, .bids = bids}};
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicGateio::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    SPDLOG_DEBUG("{} {} msg_obj={}", ex, kind, msg_obj.dump());
    std::string event = msg_obj["event"];
    if (event == "subscribe" && msg_obj["result"]["status"] == "success") {
        spdlog::info("gateio {} subscribed successfully", kind);
        return;
    }
    if (event == "unsubscribe" && msg_obj["result"]["status"] == "success") {
        spdlog::info(
            "gateio {} unsubscribed successfully -> clean order_book_cache",
            kind);
        order_book_cache.clear();
        return;
    }
    if (kind == "fut") {
        if (event != "update") {
            throw std::runtime_error("unexpected event=" + event);
        }
        std::string channel = msg_obj["channel"];
        if (channel == "futures.order_book_update") {
            std::vector<std::tuple<std::string, double>> bids;
            for (auto& a : msg_obj["result"]["b"]) {
                bids.push_back({a["p"], a["s"]});
            }
            std::vector<std::tuple<std::string, double>> asks;
            for (auto& a : msg_obj["result"]["a"]) {
                asks.push_back({a["p"], a["s"]});
            }
            if (order_book_cache.get_last_update_id() == 0) {
                std::string symbol = msg_obj["result"]["s"];
                Depth d2 = fetch_depth_snapshot(symbol)[0];
                order_book_cache.apply_orders(d2.u, d2.asks, d2.bids);
            }
            Depth d = Depth{.u = (long)msg_obj["result"]["u"] + 1,
                            .ex_ts_millis = msg_obj["time_ms"],
                            .ex = "gateio",
                            .k = kind,
                            .s = msg_obj["result"]["s"],
                            .asks = asks,
                            .bids = bids};
            if (d.u > order_book_cache.get_last_update_id()) {
                SPDLOG_WARN(
                    "{} {} order book contains non-incremental depth => apply "
                    "last received asks/bids",
                    ex, kind);
                // TODO: workout why bids ain't proper, does it mean that i have
                // to apply incrementally or clean and apply just last? P.s.
                // DHXUSDT is ignored for now because of this reason
                order_book_cache.apply_orders_force(d.u, d.asks, d.bids);
            }
            onmessage_depth(d);
        } else {
            // TODO: add channel for trades
            // TODO: add ex throw in the end on no event
            for (auto trade_raw : msg_obj["result"]) {
                TradeInt trade =
                    TradeInt::new_("gateio", trade_raw["contract"], kind,
                                   trade_raw["create_time_ms"],
                                   std::stod((std::string)trade_raw["price"]),
                                   trade_raw["size"]);
                onmessage_trade(trade);
            }
        }
    } else if (kind == "spot") {
        if (event != "update") {
            throw std::runtime_error("unexpected event=" + event);
        }
        std::string channel = msg_obj["channel"];
        if (channel == "spot.order_book_update") {
            std::vector<std::tuple<std::string, double>> bids;
            for (auto& b : msg_obj["result"]["b"]) {
                bids.push_back({b[0], stod((std::string)b[1])});
            }
            std::vector<std::tuple<std::string, double>> asks;
            for (auto& a : msg_obj["result"]["a"]) {
                asks.push_back({a[0], stod((std::string)a[1])});
            }
            if (order_book_cache.get_last_update_id() == 0) {
                std::string symbol = msg_obj["result"]["s"];
                Depth d2 = fetch_depth_snapshot(symbol)[0];
                order_book_cache.apply_orders(d2.u, d2.asks, d2.bids);
            }
            Depth d = Depth{.u = (long)msg_obj["result"]["u"] + 1,
                            .asks = asks,
                            .bids = bids};
            if (d.u > order_book_cache.get_last_update_id()) {
                spdlog::warn(
                    "[gateio] order book contains non-incremental depth");
                order_book_cache.apply_orders_force(d.u, d.asks, d.bids);
            }
            onmessage_depth(d);
        } else {
            // TODO: handle(check for) channel trade
            auto res = msg_obj["result"];
            TradeInt trade = TradeInt::new_(
                "gateio", res["currency_pair"], kind,
                (long)std::stod((std::string)res["create_time_ms"]),
                std::stod((std::string)res["price"]),
                std::stod((std::string)res["amount"]));
            onmessage_trade(trade);
        }
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

ClientPrivateGateio::ClientPrivateGateio(std::string kind)
    : ClientPrivate("gateio", kind) {
    api_key = std::getenv("GATEIO_API_KEY");
    api_secret = std::getenv("GATEIO_API_SECRET");
}

void ClientPrivateGateio::init_idle() {
    if (kind == "fut") {
        std::string url_str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
        init_idle_(url_str);
    } else if (kind == "spot") {
        std::string url_str = "wss://api.gateio.ws/ws/v4/";
        init_idle_(url_str);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
    init_exchange_info();
}

void ClientPrivateGateio::init_exchange_info() {
    if (kind == "fut") {
        std::string url_str =
            "https://api.gateio.ws/api/v4/futures/usdt/contracts";
        fut_exchange_info = execute_http_get_req(url_str);
    } else if (kind == "spot") {
        std::string url_str =
            "https://api.gateio.ws/api/v4/spot/currency_pairs";
        spot_exchange_info = execute_http_get_req(url_str);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

void ClientPrivateGateio::subscribe_to_private_events() {
    std::string channel = "";
    if (kind == "fut") {
        channel = "futures.orders";
    } else if (kind == "spot") {
        channel = "spot.orders";
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
    int timestamp = int(
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000);
    std::string sign1 =
        fmt::format("channel={}&event=subscribe&time={}", channel, timestamp);
    std::string sign2 = gen_hmac_sha512(api_secret, sign1);
    std::string body_str = fmt::format(
        R"({{
        "time": {},
        "channel": "{}",
        "event": "subscribe",
        "payload": ["!all"],
        "auth": {{
            "method": "api_key",
            "KEY": "{}",
            "SIGN": "{}"
        }}
        }})",
        timestamp, channel, api_key, sign2);
    send(body_str);
}

///
/// For fut: calculates which contracts amount i have to sent to api if i wanna
/// buy `quantity` tokens of symbol `symbol` for price `price`
/// Logic behind:
/// usdt_to_use=54.0
/// price=0.0000009
/// => tokens_to_buy=60,000,000 (quantity)
/// 1 contract represents 10,000,000 tokens (quanto_multiplier)
/// => buy 6 contracts for 54 USDT
///
std::tuple<std::string, std::string> ClientPrivateGateio::adjust_price_quantity(
    std::string symbol, double price, double quantity) {
    if (kind == "fut") {
        std::string order_price_round = "";
        std::string quanto_multiplier = "";
        for (auto& obj : fut_exchange_info.value()) {
            if (obj["name"] == symbol) {
                order_price_round = obj["order_price_round"];
                quanto_multiplier = obj["quanto_multiplier"];
            }
        }
        if (order_price_round == "" || quanto_multiplier == "") {
            throw std::runtime_error(
                fmt::format("order_price_round or quanto_multiplier is not set "
                            "for symbol={}",
                            symbol));
        }
        return {conv_to_dec_str_v2(price, order_price_round),
                std::to_string((int)(quantity / stod(quanto_multiplier)))};
    } else if (kind == "spot") {
        std::optional<int> precision = {};
        std::optional<int> amount_precision = {};
        for (auto& obj : spot_exchange_info.value()) {
            if (obj["id"] == symbol) {
                precision = obj["precision"];
                amount_precision = obj["amount_precision"];
            }
        }
        if (!precision.has_value() || !amount_precision.has_value()) {
            throw std::runtime_error(fmt::format(
                "precision or amount_precision is not set for symbol={}",
                symbol));
        }
        std::string price_prec = gen_precision_str(precision.value());
        std::string quantity_prec = gen_precision_str(amount_precision.value());
        return {conv_to_dec_str_v2(price, price_prec),
                conv_to_dec_str_v2(quantity, quantity_prec)};
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

Order ClientPrivateGateio::place_fut_limit_order(std::string symbol,
                                                 std::string side,
                                                 std::string price,
                                                 std::string quantity) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("{} unexpected kind=", ex, kind));
    }
    double timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
    std::string timestamp_str = std::to_string(timestamp);
    std::string url = "https://api.gateio.ws";
    std::string path = "/api/v4/futures/usdt/orders";
    // NOTE: json serializon fields ordering is important
    std::string size = "";
    if (side == "buy") {
        size = quantity;
    } else if (side == "sell") {
        size = "-" + quantity;
    } else {
        throw std::runtime_error("unexpected side=" + side);
    }
    std::string body_str = fmt::format(
        R"({{"contract":"{}","size":{},"iceberg":0,"price":"{}","tif":"gtc","text":"t-my-custom-id","stp_act":"-"}})",
        symbol, size, price);
    SPDLOG_INFO("{} {} place order side={} body_str={}", ex, kind, side,
                body_str);
    std::string sign = sign_str("POST", timestamp_str, path, "", body_str);
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
    headers =
        curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
    headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
    nlohmann::json res_obj =
        execute_http_post_req(url + path, headers, body_str);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = std::to_string((long)res_obj["id"]),
        .open_ts = (long)((double)res_obj["create_time"]) * 1000,
        .s = res_obj["contract"],
        .p = res_obj["price"],
        .v = (double)res_obj["size"],
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

void ClientPrivateGateio::set_leverage_to_1(std::string symbol) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("unexpected kind=", kind));
    }
    double timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
    std::string timestamp_str = std::to_string(timestamp);
    std::string url = "https://api.gateio.ws";
    std::string path =
        fmt::format("/api/v4/futures/usdt/positions/{}/leverage", symbol);
    std::string query_param = "leverage=1";
    SPDLOG_INFO("set selerage query_param={}", query_param);
    std::string sign = sign_str("POST", timestamp_str, path, query_param, "");
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
    headers =
        curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
    headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
    nlohmann::json res_obj =
        execute_http_post_req(url + path + "?" + query_param, headers, "");
    SPDLOG_DEBUG("res_obj={}", res_obj.dump());
}

Order ClientPrivateGateio::amend_fut_order(Order& order, std::string price) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("{} unexpected kind=", ex, kind));
    }
    std::string timestamp_str = std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000);
    std::string url = "https://api.gateio.ws";
    std::string path = fmt::format("/api/v4/futures/usdt/orders/{}", order.id);
    std::string body_str = fmt::format(R"({{"price":"{}"}})", price);
    SPDLOG_INFO("{} {} amend order order.id={} body_str={}", ex, kind, order.id,
                body_str);
    std::string sign = sign_str("PUT", timestamp_str, path, "", body_str);
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
    headers =
        curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
    headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
    nlohmann::json res_obj =
        execute_http_put_req(url + path, headers, body_str);
    SPDLOG_DEBUG("{} {} amend order res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = std::to_string((long)res_obj["id"]),
        .open_ts = (long)((double)res_obj["create_time"]) * 1000,
        .s = res_obj["contract"],
        .p = res_obj["price"],
        .v = (double)res_obj["size"],
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

Order ClientPrivateGateio::place_spot_limit_order(std::string symbol,
                                                  std::string side,
                                                  std::string price,
                                                  std::string quantity) {
    if (kind != "spot") {
        throw std::runtime_error("unexpected kind=" + kind);
    }
    if (side != "buy" && side != "sell") {
        throw std::runtime_error("unexpected side=" + side);
    }
    double timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
    std::string timestamp_str = std::to_string(timestamp);
    std::string url = "https://api.gateio.ws";
    std::string path = "/api/v4/spot/orders";
    std::string body_str = fmt::format(
        R"({{"text":"t-123","currency_pair":"{}","type":"limit","account":"spot","side":"{}","amount":"{}","price":"{}","time_in_force":"gtc","iceberg":"0"}})",
        symbol, side, quantity, price);
    SPDLOG_INFO("{} {} place order side={} body_str={}", ex, kind, side,
                body_str);
    std::string sign = sign_str("POST", timestamp_str, path, "", body_str);
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
    headers =
        curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
    headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
    nlohmann::json res_obj =
        execute_http_post_req(url + path, headers, body_str);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = res_obj["id"],
        .open_ts = res_obj["create_time_ms"],
        .s = res_obj["currency_pair"],
        .p = res_obj["price"],
        .v = stod((std::string)res_obj["amount"]),
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

Order ClientPrivateGateio::fetch_order(Order& order) {
    double timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
    std::string timestamp_str = std::to_string(timestamp);
    if (kind == "fut") {
        std::string url = "https://api.gateio.ws";
        std::string path =
            fmt::format("/api/v4/futures/usdt/orders/{}", order.id);
        std::string sign = sign_str("GET", timestamp_str, path, "", "");
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
        headers =
            curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
        headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
        std::string url_path = url + path;
        nlohmann::json res_obj = execute_http_get_req(url_path, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        Order order = Order{
            .ex = ex,
            .k = kind,
            .id = std::to_string((long)res_obj["id"]),
            .open_ts = (long)((double)res_obj["create_time"]) * 1000,
            .s = res_obj["contract"],
            .p = res_obj["price"],
            .v = (double)res_obj["size"],
            .filled_amount = .0,
            .st = "PLACED_TO_ORDER_BOOK",
        };
        if (res_obj["status"] == "finished") {
            order.p_avg_fill = stod((std::string)res_obj["fill_price"]);
            order.filled_amount = fabs((double)res_obj["size"]);
            order.st = "FILLED";
        }
        return order;
    } else if (kind == "spot") {
        std::string url = "https://api.gateio.ws";
        std::string path = fmt::format("/api/v4/spot/orders/{}", order.id);
        std::string query_param = fmt::format("currency_pair={}", order.s);
        SPDLOG_DEBUG("{} {} path={}", ex, kind, path);
        std::string sign =
            sign_str("GET", timestamp_str, path, query_param, "");
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
        headers =
            curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
        headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
        std::string url_path = fmt::format("{}{}?{}", url, path, query_param);
        nlohmann::json res_obj = execute_http_get_req(url_path, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        Order order = Order{
            .ex = ex,
            .k = kind,
            .id = res_obj["id"],
            .open_ts = res_obj["create_time_ms"],
            .s = res_obj["currency_pair"],
            .p = res_obj["price"],
            .v = stod((std::string)res_obj["amount"]),
            .filled_amount = .0,
            .st = "PLACED_TO_ORDER_BOOK",
        };
        if (res_obj.contains("status") && res_obj["status"] == "closed") {
            order.p_avg_fill = stod((std::string)res_obj["avg_deal_price"]);
            order.filled_amount =
                fabs(stod((std::string)res_obj["filled_amount"])) -
                stod((std::string)res_obj["fee"]);
            order.st = "FILLED";
            order.fee_usdt = stod((std::string)res_obj["fee"]);
        }
        return order;
    } else {
        throw std::runtime_error(
            fmt::format("{} unexpected kind={}", ex, kind));
    }
}

double ClientPrivateGateio::fetch_order_fee_usdt(Order& order) {
    if (kind == "fut") {
        double timestamp =
            std::chrono::system_clock::now().time_since_epoch().count() /
            1000000;
        std::string timestamp_str = std::to_string(timestamp);
        std::string url = "https://api.gateio.ws";
        std::string path = "/api/v4/futures/usdt/my_trades";
        std::string query_params = fmt::format("order={}", order.id);
        std::string sign =
            sign_str("GET", timestamp_str, path, query_params, "");
        SPDLOG_DEBUG("{} {} fetch trades order_id={}", ex, kind, order.id);
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
        headers =
            curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
        headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
        std::string url_path = fmt::format("{}{}?{}", url, path, query_params);
        nlohmann::json res_obj = execute_http_get_req(url_path, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        double fee_out = 0.0;
        for (auto& obj : res_obj) {
            fee_out += stod((std::string)obj["fee"]);
        }
        return fee_out;
    } else if (kind == "spot") {
        SPDLOG_INFO("{} fee_usdt is already fetched in fetch_order", ex);
        return order.fee_usdt;
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

std::map<std::string, double> ClientPrivateGateio::fetch_balances() {
    if (kind == "fut") {
        std::string timestamp_str = std::to_string(now_millis() / 1000);
        std::string url = "https://api.gateio.ws";
        std::string path = "/api/v4/futures/usdt/accounts";
        std::string sign = sign_str("GET", timestamp_str, path, "", "");
        SPDLOG_INFO("{} {} fetch balances", ex, kind);
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
        headers =
            curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
        headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
        std::string url_path = fmt::format("{}{}", url, path);
        nlohmann::json res_obj = execute_http_get_req(url_path, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        return {{"USDT", stod((std::string)res_obj["available"])}};
    } else if (kind == "spot") {
        std::string timestamp_str = std::to_string(now_millis() / 1000);
        std::string url = "https://api.gateio.ws";
        std::string path = "/api/v4/spot/accounts";
        std::string sign = sign_str("GET", timestamp_str, path, "", "");
        SPDLOG_INFO("{} {} fetch balances", ex, kind);
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("KEY: " + api_key).c_str());
        headers =
            curl_slist_append(headers, ("Timestamp: " + timestamp_str).c_str());
        headers = curl_slist_append(headers, ("SIGN: " + sign).c_str());
        std::string url_path = fmt::format("{}{}", url, path);
        nlohmann::json res_obj = execute_http_get_req(url_path, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        std::map<std::string, double> balances;
        for (auto& obj : res_obj) {
            balances[obj["currency"]] = stod((std::string)obj["available"]);
        }
        return balances;
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

void ClientPrivateGateio::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    SPDLOG_DEBUG("ex={} kind={} msg_obj={}", ex, kind, msg_obj.dump());
    if (kind == "fut") {
        if (msg_obj["event"] == "subscribe" &&
            msg_obj["channel"] == "futures.orders" &&
            msg_obj["result"]["status"] == "success") {
            spdlog::info("gateio fut subscribed successfully");
            is_subscribed_to_private_channels = true;
            return;
        }
        if (msg_obj["event"] == "update" &&
            msg_obj["channel"] == "futures.orders") {
            nlohmann::json order_obj = msg_obj["result"][0];
            std::string finish_as = order_obj["finish_as"];
            if (finish_as == "_new") {
                spdlog::info("gateio fut order created");
                Order order = Order{
                    .ex = ex,
                    .k = kind,
                    .id = order_obj["id_string"],
                    .open_ts = order_obj["create_time_ms"],
                    .s = order_obj["contract"],
                    .p = order_obj["price"],
                    .v = (double)order_obj["size"],
                    .filled_amount = 0.0,
                    .st = "PLACED_TO_ORDER_BOOK",
                };
                orders.push_back(order);
            } else if (finish_as == "filled") {
                std::string order_id = order_obj["id_string"];
                spdlog::info("gateio fut order filled order_id={}", order_id);
                for (int i = 0; i < orders.size(); i++) {
                    if (orders[i].id == order_id) {
                        orders[i].filled_amount = orders[i].v;
                        orders[i].st = "FILLED";
                        spdlog::info(
                            "gateio order amount updated for order_id={}",
                            order_id);
                        return;
                    }
                }
                spdlog::info(
                    "gateio fut order havent found order_id={} => create it",
                    order_id);
                Order order = Order{
                    .ex = ex,
                    .k = kind,
                    .id = order_obj["id_string"],
                    .open_ts = order_obj["create_time_ms"],
                    .s = order_obj["contract"],
                    .p = std::to_string((double)order_obj["price"]),
                    .v = (double)order_obj["size"],
                    .filled_amount = fabs((double)order_obj["size"]),
                    .st = "FILLED",
                };
                orders.push_back(order);
            } else {
                throw std::runtime_error(
                    fmt::format("unexpected finish_as=", finish_as));
            }
        }
        return;
    } else if (kind == "spot") {
        if (msg_obj["event"] == "subscribe" &&
            msg_obj["channel"] == "spot.orders" &&
            msg_obj["result"]["status"] == "success") {
            spdlog::info("gateio spot subscribed successfully");
            is_subscribed_to_private_channels = true;
            return;
        }
        if (msg_obj["event"] == "update" &&
            msg_obj["channel"] == "spot.orders") {
            nlohmann::json order_obj = msg_obj["result"][0];
            std::string finish_as = order_obj["finish_as"];
            if (finish_as == "open") {
                spdlog::info("gateio spot order created");
                Order order = Order{
                    .ex = ex,
                    .k = kind,
                    .id = order_obj["id"],
                    .open_ts = stol((std::string)order_obj["create_time_ms"]),
                    .s = order_obj["currency_pair"],
                    .p = order_obj["price"],
                    .v = stod((std::string)order_obj["amount"]),
                    .filled_amount = 0.0,
                    .st = "PLACED_TO_ORDER_BOOK",
                };
                orders.push_back(order);
            } else if (finish_as == "filled") {
                std::string order_id = order_obj["id"];
                double filled_amount = stod((std::string)order_obj["amount"]) -
                                       stod((std::string)order_obj["fee"]);
                spdlog::info("gateio spot order filled order_id={}", order_id);
                for (int i = 0; i < orders.size(); i++) {
                    if (orders[i].id == order_id) {
                        orders[i].filled_amount = filled_amount;
                        orders[i].st = "FILLED";
                        spdlog::info(
                            "gateio order amount updated for order_id={}",
                            order_id);
                        return;
                    }
                }
                spdlog::info(
                    "gateio fut order havent found order_id={} => create it",
                    order_id);
                Order order = Order{
                    .ex = ex,
                    .k = kind,
                    .id = order_obj["id"],
                    .open_ts = stol((std::string)order_obj["create_time_ms"]),
                    .s = order_obj["currency_pair"],
                    .p = order_obj["price"],
                    .v = stod((std::string)order_obj["amount"]),
                    .filled_amount = filled_amount,
                    .st = "FILLED",
                };
                orders.push_back(order);
            } else {
                throw std::runtime_error("unexpected finish_as=" + finish_as);
            }
            return;
        }
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
    spdlog::warn("missing {} {} message msg_obj={}", ex, kind, msg_obj.dump());
}

std::string ClientPrivateGateio::sign_str(std::string method, std::string t,
                                          std::string url,
                                          std::string query_str,
                                          std::string payload_str) {
    unsigned char hashed_payload[SHA512_DIGEST_LENGTH];
    SHA512(reinterpret_cast<const unsigned char*>(payload_str.c_str()),
           payload_str.size(), hashed_payload);
    std::ostringstream oss_hashed_payload;
    for (size_t i = 0; i < SHA512_DIGEST_LENGTH; ++i) {
        oss_hashed_payload << std::hex << std::setw(2) << std::setfill('0')
                           << static_cast<int>(hashed_payload[i]);
    }
    std::string hashed_payload_hex = oss_hashed_payload.str();
    std::ostringstream oss;
    oss << method << "\n"
        << url << "\n"
        << (query_str.empty() ? "" : query_str) << "\n"
        << hashed_payload_hex << "\n"
        << t;
    std::string s = oss.str();
    return gen_hmac_sha512(api_secret, s);
}

ClientPublicMexc::ClientPublicMexc(std::string kind)
    : ClientPublic("mexc", kind) {}

void ClientPublicMexc::init_idle() {
    if (kind == "fut") {
        std::string url = "wss://contract.mexc.com:443/edge";
        init_idle_(url);
    } else if (kind == "spot") {
        std::string url = "wss://wbs.mexc.com/ws";
        init_idle_(url);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicMexc::subscribe_to_trades(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "method": "sub.deal",
                "param": {"symbol": "%s"}
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "method": "SUBSCRIPTION",
                "params": ["spot@public.deals.v3.api@%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicMexc::subscribe_to_depth(std::string& symbol) {
    subscribed_to_depth = true;
    if (kind == "fut") {
        send(fmt::format(
            R"({{
                "method":"sub.depth",
                "param":{"symbol":"{}"}
            }})",
            symbol));
    } else if (kind == "spot") {
        send(fmt::format(
            R"({{
                "method": "SUBSCRIPTION",
                "params": ["spot@public.increase.depth.v3.api@{}"]
            }})",
            symbol));
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

void ClientPublicMexc::unsubscribe_from_depth(std::string& symbol) {
    spdlog::info("mexc {} send unsubscribing from depth", kind);
    subscribed_to_depth = false;
    order_book_cache.clear();
    if (kind == "fut") {
        std::string t_template = R"({
                "method":"unsub.depth",
                "param":{"symbol":"%s"}
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "method": "UNSUBSCRIPTION",
                "params": ["spot@public.increase.depth.v3.api@%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicMexc::ping() {
    if (kind == "fut") {
        send(R"({"method": "ping"})");
    }
}

Ticker ClientPublicMexc::fetch_ticker(std::string& symbol) {
    if (kind == "spot") {
        std::string url_str = fmt::format(
            "https://api.mexc.com/api/v3/ticker/bookTicker?symbol={}", symbol);
        nlohmann::json res_obj = execute_http_get_req(url_str);
        SPDLOG_DEBUG("{} {} fetch_ticker res_obj={}", ex, kind, res_obj.dump());
        return Ticker{
            .s = res_obj["symbol"],
            .bid = stod((std::string)res_obj["bidPrice"]),
            .ask = stod((std::string)res_obj["askPrice"]),
        };
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

std::vector<Depth> ClientPublicMexc::fetch_depth_snapshot(std::string& symbol) {
    if (kind == "fut") {
        std::string url =
            "https://contract.mexc.com/api/v1/contract/depth_commits/" +
            symbol + "/100000";
        nlohmann::json obj = execute_http_get_req(url);
        if (!((bool)obj["success"])) {
            throw std::runtime_error("res is not success");
        }
        std::vector<nlohmann::json> data = obj["data"];
        sort(data.begin(), data.end(), [](nlohmann::json a, nlohmann::json b) {
            return a["version"] < b["version"];
        });
        std::vector<Depth> depths;
        for (auto& o : data) {
            std::vector<std::tuple<std::string, double>> bids;
            for (auto& b : o["bids"]) {
                std::string p = b[0].dump();
                bids.push_back({p, b[1]});
            }
            std::vector<std::tuple<std::string, double>> asks;
            for (auto& a : o["asks"]) {
                std::string p = a[0].dump();
                asks.push_back({p, a[1]});
            }
            depths.push_back(
                Depth{.u = o["version"], .asks = asks, .bids = bids});
        }
        return depths;
    } else if (kind == "spot") {
        std::string url = "https://api.mexc.com/api/v3/depth?symbol=" + symbol +
                          "&limit=5000";
        nlohmann::json obj = execute_http_get_req(url);
        std::vector<std::tuple<std::string, double>> bids;
        for (auto& b : obj["bids"]) {
            bids.push_back({b[0], stod((std::string)b[1])});
        }
        std::vector<std::tuple<std::string, double>> asks;
        for (auto& a : obj["asks"]) {
            asks.push_back({a[0], stod((std::string)a[1])});
        }
        return std::vector<Depth>{
            Depth{.u = obj["lastUpdateId"], .asks = asks, .bids = bids}};
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicMexc::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    SPDLOG_DEBUG("{} {} handle_onmessage msg_obj={}", ex, kind, msg_obj.dump());
    if (kind == "fut") {
        std::string channel = msg_obj["channel"];
        if (channel == "rs.sub.depth" && msg_obj["data"] == "success") {
            spdlog::info("mexc fut depth subscribed successfully");
        } else if (channel == "push.depth") {
            if (!subscribed_to_depth) {
                spdlog::warn(
                    "mexc fut depth received but subscribed_to_depth={}",
                    subscribed_to_depth);
                return;
            }
            std::vector<std::tuple<std::string, double>> bids;
            for (auto& b : msg_obj["data"]["bids"]) {
                std::string p = b[0].dump();
                bids.push_back({p, b[1]});
            }
            std::vector<std::tuple<std::string, double>> asks;
            for (auto& a : msg_obj["data"]["asks"]) {
                std::string p = a[0].dump();
                asks.push_back({p, a[1]});
            }
            Depth d = Depth{
                .u = msg_obj["data"]["version"],
                .asks = asks,
                .bids = bids,
            };
            if (order_book_cache.get_last_update_id() == 0) {
                std::string symbol = msg_obj["symbol"];
                auto snapshot_depths = fetch_depth_snapshot(symbol);
                for (auto& d : snapshot_depths) {
                    order_book_cache.apply_orders(d.u, d.asks, d.bids);
                }
            }
            if (d.u > order_book_cache.get_last_update_id()) {
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
            }
            onmessage_depth(d);
        } else if (channel == "rs.sub.deal" && msg_obj["data"] == "success") {
            spdlog::info("mexc fut subscribed successfully");
        } else if (channel == "pong") {
            spdlog::debug("handle pong");
        } else if (channel == "push.deal") {
            TradeInt trade =
                TradeInt::new_("mexc", msg_obj["symbol"], kind, msg_obj["ts"],
                               msg_obj["data"]["p"], msg_obj["data"]["v"]);
            onmessage_trade(trade);
        } else {
            throw std::runtime_error("unexpected channel=" + channel +
                                     ", msg=" + msg);
        }
    } else if (kind == "spot") {
        if (msg_obj.contains("id") && msg_obj["id"] == 0 &&
            msg_obj.contains("code") && msg_obj["code"] == 0) {
            SPDLOG_INFO("mexc spot subscribed successfully msg_obj={}",
                        msg_obj.dump());
            return;
        }
        if (msg_obj.contains("c")) {
            std::string c = msg_obj["c"];
            if (c.rfind("spot@public.increase.depth.v3.api@", 0) == 0) {
                if (!subscribed_to_depth) {
                    spdlog::warn(
                        "mexc spot depth received but "
                        "subscribed_to_depth={} -> ignore",
                        subscribed_to_depth);
                    return;
                }
                std::vector<std::tuple<std::string, double>> bids;
                if (msg_obj["d"].contains("bids")) {
                    for (auto& b : msg_obj["d"]["bids"]) {
                        bids.push_back({b["p"], stod((std::string)b["v"])});
                    }
                }
                std::vector<std::tuple<std::string, double>> asks;
                if (msg_obj["d"].contains("asks")) {
                    for (auto& a : msg_obj["d"]["asks"]) {
                        asks.push_back({a["p"], stod((std::string)a["v"])});
                    }
                }
                Depth d = Depth{
                    .u = stol((std::string)msg_obj["d"]["r"]),
                    .ex_ts_millis = msg_obj["t"],
                    .ex = "mexc",
                    .k = kind,
                    .s = msg_obj["s"],
                    .asks = asks,
                    .bids = bids,
                };
                if (order_book_cache.get_last_update_id() == 0) {
                    for (int i = 0; i < 3; i++) {
                        std::string symbol = msg_obj["s"];
                        auto snapshot_depths = fetch_depth_snapshot(symbol);
                        Depth d2 = snapshot_depths[0];
                        if (d2.u < d.u) {
                            spdlog::info("stale depth snapshot u={}", d.u);
                            continue;
                        }
                        order_book_cache.apply_orders(d2.u, d2.asks, d2.bids);
                        break;
                    }
                    if (order_book_cache.get_last_update_id() == 0) {
                        throw std::runtime_error("stale depth snapshot u");
                    }
                }
                if (d.u > order_book_cache.get_last_update_id()) {
                    order_book_cache.apply_orders(d.u, d.asks, d.bids);
                }
                onmessage_depth(d);
            } else if (c.rfind("spot@public.deals.v3.api@", 0) == 0) {
                for (auto& deal_raw : msg_obj["d"]["deals"]) {
                    TradeInt trade = TradeInt::new_(
                        "mexc", msg_obj["s"], kind, deal_raw["t"],
                        std::stod((std::string)deal_raw["p"]),
                        std::stod((std::string)deal_raw["v"]));
                    onmessage_trade(trade);
                }
            }
        } else {
            throw std::runtime_error(fmt::format("unexpected msg={}", msg));
        }
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

ClientPrivateMexc::ClientPrivateMexc(std::string kind)
    : ClientPrivate("mexc", kind) {
    api_key = std::getenv("MEXC_API_KEY");
    api_secret = std::getenv("MEXC_API_SECRET");
}

void ClientPrivateMexc::init_idle() {
    if (kind == "fut") {
        throw std::runtime_error(
            "not implemented due to impossiblity to place fut order through "
            "api");
    } else if (kind == "spot") {
        std::string url_str = fmt::format("wss://wbs.mexc.com/ws?listenKey={}",
                                          create_listen_key());
        init_idle_(url_str);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
    init_exchange_info();
}

void ClientPrivateMexc::init_exchange_info() {
    if (kind == "spot") {
        std::string url_str = "https://api.mexc.com/api/v3/exchangeInfo";
        spot_exchange_info = execute_http_get_req(url_str);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

void ClientPrivateMexc::subscribe_to_private_events() {
    if (kind == "spot") {
        std::string s = R"({
            "method": "SUBSCRIPTION",
            "params": ["spot@private.orders.v3.api"]
        })";
        send(s);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

std::tuple<std::string, std::string> ClientPrivateMexc::adjust_price_quantity(
    std::string symbol, double price, double quantity) {
    if (kind != "spot") {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
    if (!spot_exchange_info.has_value()) {
        throw std::runtime_error("spot_exchange_info is not set");
    }
    std::optional<int> quoteAssetPrecision = {};
    std::optional<int> baseAssetPrecision = {};
    for (auto& obj : spot_exchange_info.value()["symbols"]) {
        if (obj["symbol"] == symbol) {
            quoteAssetPrecision = obj["quoteAssetPrecision"];
            baseAssetPrecision = obj["baseAssetPrecision"];
            break;
        }
    }
    if (!quoteAssetPrecision.has_value() || !baseAssetPrecision.has_value()) {
        throw std::runtime_error(
            fmt::format("quoteAssetPrecision or baseAssetPrecision is not set "
                        "for symbol={}",
                        symbol));
    }
    std::string price_prec = gen_precision_str(quoteAssetPrecision.value());
    std::string quantity_prec = gen_precision_str(baseAssetPrecision.value());
    return {conv_to_dec_str_v2(price, price_prec),
            conv_to_dec_str_v2(quantity, quantity_prec)};
}

Order ClientPrivateMexc::place_fut_limit_order(std::string symbol,
                                               std::string side,
                                               std::string price,
                                               std::string quantity) {
    // NOTE: placing fut order is "Under maintenance"
    // https://mexcdevelop.github.io/apidocs/contract_v1_en/#order-under-maintenance
    // https://www.mexc.com/support/articles/15149585234969
    throw std::runtime_error("not-implemented");
}

void ClientPrivateMexc::set_leverage_to_1(std::string symbol) {
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateMexc::amend_fut_order(Order& order, std::string price) {
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateMexc::place_spot_limit_order(std::string symbol,
                                                std::string side,
                                                std::string price,
                                                std::string quantity) {
    std::string side_int = "";
    if (side == "buy") {
        side_int = "BUY";
    } else if (side == "sell") {
        side_int = "SELL";
    } else {
        throw std::runtime_error(fmt::format("unexpected side=", side));
    }
    long timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000;
    std::string q1 = fmt::format(
        "symbol={}&side={}&type=LIMIT&price={}&quantity={}&recvWindow=60000&"
        "timestamp={}",
        symbol, side_int, price, quantity, timestamp);
    SPDLOG_INFO("{} {} place order side={} q1={}", ex, kind, side_int, q1);
    std::string q2 = fmt::format("{}&signature={}", q1, sign_str(q1));
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
    nlohmann::json res_obj =
        execute_http_post_req("https://api.mexc.com/api/v3/order", headers, q2);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = res_obj["orderId"],
        .open_ts = res_obj["transactTime"],
        .s = res_obj["symbol"],
        .p = res_obj["price"],
        .v = stod((std::string)res_obj["origQty"]),
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

Order ClientPrivateMexc::fetch_order(Order& order) {
    if (kind == "spot") {
        long timestamp =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string q1 = fmt::format("symbol={}&orderId={}&timestamp={}",
                                     order.s, order.id, timestamp);
        SPDLOG_INFO("fetch order q1={}", q1);
        std::string q2 = fmt::format("{}&signature={}", q1, sign_str(q1));
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers =
            curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
        std::string url =
            fmt::format("https://api.mexc.com/api/v3/order?{}", q2);
        nlohmann::json res_obj = execute_http_get_req(url, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        Order order = Order{
            .ex = ex,
            .k = kind,
            .id = res_obj["orderId"],
            .open_ts = res_obj["updateTime"],
            .s = res_obj["symbol"],
            .p = res_obj["price"],
            .v = stod((std::string)res_obj["origQty"]),
            .filled_amount = .0,
            .st = "PLACED_TO_ORDER_BOOK",
        };
        if (res_obj["status"] == "FILLED") {
            order.p_avg_fill =
                stod((std::string)res_obj["cummulativeQuoteQty"]) /
                stod((std::string)res_obj["executedQty"]);
            order.filled_amount = stod((std::string)res_obj["executedQty"]);
            order.st = "FILLED";
        }
        return order;
    } else {
        throw std::runtime_error(
            fmt::format("{} unexpected kind={}", ex, kind));
    }
}

double ClientPrivateMexc::fetch_order_fee_usdt(Order& order) {
    if (kind == "spot") {
        long timestamp =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string q1 = fmt::format("symbol={}&orderId={}&timestamp={}",
                                     order.s, order.id, timestamp);
        SPDLOG_INFO("fetch order q1={}", q1);
        std::string q2 = fmt::format("{}&signature={}", q1, sign_str(q1));
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers =
            curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
        std::string url =
            fmt::format("https://api.mexc.com/api/v3/myTrades?{}", q2);
        nlohmann::json res_obj = execute_http_get_req(url, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        double fee_out = 0.0;
        for (auto& obj : res_obj) {
            fee_out += stod((std::string)obj["commission"]);
        }
        return fee_out;
    } else {
        throw std::runtime_error(
            fmt::format("{} unexpected kind={}", ex, kind));
    }
}

std::map<std::string, double> ClientPrivateMexc::fetch_balances() {
    if (kind == "spot") {
        long timestamp =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string q1 = fmt::format("timestamp={}", timestamp);
        SPDLOG_INFO("{} {} fetch balances q1={}", ex, kind, q1);
        std::string q2 = fmt::format("{}&signature={}", q1, sign_str(q1));
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers =
            curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
        std::string url =
            fmt::format("https://api.mexc.com/api/v3/account?{}", q2);
        nlohmann::json res_obj = execute_http_get_req(url, headers);
        SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
        std::map<std::string, double> balances;
        for (auto& obj : res_obj["balances"]) {
            balances[obj["asset"]] = stod((std::string)obj["free"]);
        }
        return balances;
    } else {
        throw std::runtime_error(
            fmt::format("{} unexpected kind={}", ex, kind));
    }
}

void ClientPrivateMexc::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    SPDLOG_DEBUG("ex={} kind={} msg_obj={}", ex, kind, msg_obj.dump());
    std::string exp_channels = "spot@private.orders.v3.api";
    if (msg_obj["id"] == 0 && msg_obj["code"] == 0 &&
        msg_obj["msg"] == exp_channels) {
        is_subscribed_to_private_channels = true;
        spdlog::info("mexc {} subscribed successfully", kind);
        return;
    }
    if (msg_obj["c"] == "spot@private.orders.v3.api") {
        int s = msg_obj["d"]["s"];
        if (s == 1) {
            Order order = Order{
                .ex = "mexc",
                .k = "spot",
                .id = msg_obj["d"]["i"],
                .open_ts = msg_obj["d"]["O"],
                .s = msg_obj["s"],
                .p = msg_obj["d"]["p"],
                .v = stod((std::string)msg_obj["d"]["v"]),
                .filled_amount = 0.0,
                .st = "PLACED_TO_ORDER_BOOK",
            };
            orders.push_back(order);
            spdlog::info("mexc {} order created successfully order={}", kind,
                         order.toString());
        } else if (s == 2) {
            std::string order_id = msg_obj["d"]["i"];
            double filled_amount = stod((std::string)msg_obj["d"]["cv"]);
            spdlog::info("mexc order filled order_id={}", order_id);
            for (int i = 0; i < orders.size(); i++) {
                if (orders[i].id == order_id) {
                    orders[i].filled_amount = filled_amount;
                    orders[i].st = "FILLED";
                    spdlog::info("order amount updated for order_id={}",
                                 order_id);
                    return;
                }
            }
            spdlog::info("order not found order_id={} => create it", order_id);
            Order order = Order{
                .ex = "mexc",
                .k = "spot",
                .id = msg_obj["d"]["i"],
                .open_ts = msg_obj["d"]["O"],
                .s = msg_obj["s"],
                .p = msg_obj["d"]["p"],
                .v = stod((std::string)msg_obj["d"]["v"]),
                .filled_amount = filled_amount,
                .st = "FILLED",
            };
            orders.push_back(order);
        } else if (s == 3) {
            spdlog::info("paritally filled received => wait for fully filled");
        } else if (s == 4) {
            std::string order_id = msg_obj["d"]["i"];
            spdlog::info("mexc order cancelled order_id={}", order_id);
            auto pos =
                std::find_if(orders.begin(), orders.end(),
                             [order_id](Order o) { return order_id == o.id; });
            if (pos == orders.end()) {
                throw std::runtime_error(
                    fmt::format("order not found order_id={}", order_id));
            } else {
                orders.erase(pos);
            }
        } else {
            throw std::runtime_error(fmt::format("unexpected mexc s={}", s));
        }
        spdlog::debug("orders.size={}", orders.size());
        return;
    }
    spdlog::warn("missing {} {} message msg_obj={}", ex, kind, msg_obj.dump());
}

std::string ClientPrivateMexc::sign_str(std::string& query) {
    unsigned char signature_digest[EVP_MAX_MD_SIZE];
    unsigned int signature_digest_len = 0;
    HMAC(EVP_sha256(), api_secret.c_str(), api_secret.length(),
         reinterpret_cast<const unsigned char*>(query.c_str()),
         strlen(query.c_str()), signature_digest, &signature_digest_len);
    std::ostringstream signature_ss;
    for (unsigned int i = 0; i < signature_digest_len; ++i) {
        signature_ss << std::hex << std::setw(2) << std::setfill('0')
                     << static_cast<int>(signature_digest[i]);
    }
    return signature_ss.str();
}

std::string ClientPrivateMexc::create_listen_key() {
    long timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000;
    std::string q1 = fmt::format("timestamp={}", timestamp);
    std::string q2 = fmt::format("{}&signature={}", q1, sign_str(q1));
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
    CURL* curl = curl_easy_init();
    nlohmann::json res_obj = execute_http_post_req(
        "https://api.mexc.com/api/v3/userDataStream", headers, q2);
    SPDLOG_DEBUG("res_obj={}", res_obj.dump());
    return res_obj["listenKey"];
}

ClientPublicBybit::ClientPublicBybit(std::string kind)
    : ClientPublic("bybit", kind) {}

void ClientPublicBybit::init_idle() {
    if (kind == "fut") {
        std::string url = "wss://stream.bybit.com/v5/public/linear";
        init_idle_(url);
    } else if (kind == "spot") {
        std::string url = "wss://stream.bybit.com/v5/public/spot";
        init_idle_(url);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicBybit::subscribe_to_trades(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "req_id": "t",
                "op": "subscribe",
                "args": ["publicTrade.%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "req_id": "t",
                "op": "subscribe",
                "args": ["publicTrade.%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicBybit::subscribe_to_depth(std::string& symbol) {
    if (kind == "fut" || kind == "spot") {
        std::string t_template = R"({
                "op": "subscribe",
                "args": ["orderbook.200.%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicBybit::unsubscribe_from_depth(std::string& symbol) {
    if (kind == "fut" || kind == "spot") {
        std::string t_template = R"({
                "op": "unsubscribe",
                "args": ["orderbook.200.%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicBybit::ping() {}

Ticker ClientPublicBybit::fetch_ticker(std::string& symbol) {
    std::string url_str = "";
    if (kind == "fut") {
        url_str = fmt::format(
            "https://api.bybit.com/v5/market/tickers?category=linear&symbol={}",
            symbol);
    } else if (kind == "spot") {
        url_str = fmt::format(
            "https://api.bybit.com/v5/market/tickers?category=spot&symbol={}",
            symbol);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
    nlohmann::json res_obj = execute_http_get_req(url_str);
    if (res_obj["retCode"] != 0 || res_obj["retMsg"] != "OK" ||
        res_obj["result"]["list"].size() != 1) {
        throw std::runtime_error(
            fmt::format("{} {} res is not success", ex, kind));
    }
    nlohmann::json t = res_obj["result"]["list"][0];
    return Ticker{
        .s = t["symbol"],
        .bid = stod((std::string)t["bid1Price"]),
        .ask = stod((std::string)t["ask1Price"]),
    };
}

void ClientPublicBybit::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    if (msg_obj.contains("op")) {
        bool success = msg_obj["success"];
        std::string op = msg_obj["op"];
        if ((bool)msg_obj["success"]) {
            spdlog::info("bybit {} successfully -> clear order-book", op);
            order_book_cache.clear();
            return;
        } else {
            throw std::runtime_error("bybit " + op + " failed");
        }
    }
    std::string msg_type = msg_obj["type"];
    std::string topic = msg_obj["topic"];
    if (kind == "fut") {
        if (str_starts_with(topic, "orderbook.")) {
            if (msg_type == "snapshot") {
                spdlog::info("(re)create bybit fut order-book");
                Depth d = parse_depth(msg_obj);
                order_book_cache.clear();
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
                onmessage_depth(d);
            } else if (msg_type == "delta") {
                Depth d = parse_depth(msg_obj);
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
                onmessage_depth(d);
            } else {
                throw std::runtime_error("bybit fut unknown type=" + msg_type);
            }
        } else {
            // TODO: add topic check for trades
            if (msg_type != "snapshot") {
                throw std::runtime_error("bybit fut unknown type=" + msg_type);
            }
            for (auto& trade_raw : msg_obj["data"]) {
                TradeInt trade = TradeInt::new_(
                    "bybit", trade_raw["s"], kind, trade_raw["T"],
                    std::stod((std::string)trade_raw["p"]),
                    std::stod((std::string)trade_raw["v"]));
                onmessage_trade(trade);
            }
        }
    } else if (kind == "spot") {
        if (str_starts_with(topic, "orderbook.")) {
            if (msg_type == "snapshot") {
                spdlog::info("(re)create bybit spot order-book");
                Depth d = parse_depth(msg_obj);
                order_book_cache.clear();
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
                onmessage_depth(d);
            } else if (msg_type == "delta") {
                Depth d = parse_depth(msg_obj);
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
                onmessage_depth(d);
            } else {
                throw std::runtime_error("bybit fut unknown type=" + msg_type);
            }
        } else {
            // TODO: add topic check
            if (msg_type != "snapshot") {
                throw std::runtime_error("bybit fut unknown type=" + msg_type);
            }
            for (auto& trade_raw : msg_obj["data"]) {
                TradeInt trade = TradeInt::new_(
                    "bybit", trade_raw["s"], kind, trade_raw["T"],
                    std::stod((std::string)trade_raw["p"]),
                    std::stod((std::string)trade_raw["v"]));
                onmessage_trade(trade);
            }
        }
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

Depth ClientPublicBybit::parse_depth(nlohmann::json& msg_obj) {
    std::vector<std::tuple<std::string, double>> bids;
    for (auto& b : msg_obj["data"]["b"]) {
        bids.push_back({b[0], stod((std::string)b[1])});
    }
    std::vector<std::tuple<std::string, double>> asks;
    for (auto& a : msg_obj["data"]["a"]) {
        asks.push_back({a[0], stod((std::string)a[1])});
    }
    return Depth{.u = msg_obj["data"]["u"], .asks = asks, .bids = bids};
}

ClientPrivateBybit::ClientPrivateBybit(std::string kind)
    : ClientPrivate("bybit", kind) {
    api_key = std::getenv("BYBIT_API_KEY");
    api_secret = std::getenv("BYBIT_API_SECRET");
}

void ClientPrivateBybit::init_idle() {
    std::string url_str = "wss://stream.bybit.com/v5/private";
    init_idle_(url_str);
    init_exchange_info();
}

void ClientPrivateBybit::init_exchange_info() {
    std::string fut_ex_info_url_str =
        "https://api.bybit.com/v5/market/instruments-info?category=linear";
    std::string spot_ex_info_url_str =
        "https://api.bybit.com/v5/market/instruments-info?category=spot";
    if (kind == "fut") {
        fut_exchange_info = execute_http_get_req(fut_ex_info_url_str);
    } else if (kind == "spot") {
        spot_exchange_info = execute_http_get_req(spot_ex_info_url_str);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

void ClientPrivateBybit::subscribe_to_private_events() {
    long expires =
        (long)(std::chrono::system_clock::now().time_since_epoch().count() /
               1000) +
        1000;
    std::string param_str = fmt::format("GET/realtime{}", expires);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string auth_message =
        fmt::format(R"({{"op": "auth", "args": ["{}", {}, "{}"]}})", api_key,
                    expires, sign);
    send(auth_message);
    send(R"({
        "op": "subscribe",
        "req_id": "t-req-id",
        "args": ["order"]
    })");
}

std::tuple<std::string, std::string> ClientPrivateBybit::adjust_price_quantity(
    std::string symbol, double price, double quantity) {
    if (kind == "fut") {
        if (!fut_exchange_info.has_value()) {
            throw std::runtime_error("fut_exchange_info is not inited");
        }
        std::string lotSizeFilter_qtyStep = "";
        std::string priceFilter_tickSize = "";
        for (auto& ex_info_obj : fut_exchange_info.value()["result"]["list"]) {
            if (ex_info_obj["symbol"] == symbol) {
                lotSizeFilter_qtyStep = ex_info_obj["lotSizeFilter"]["qtyStep"];
                priceFilter_tickSize = ex_info_obj["priceFilter"]["tickSize"];
            }
        }
        if (lotSizeFilter_qtyStep.empty() || priceFilter_tickSize.empty()) {
            throw std::runtime_error(fmt::format(
                "symbol={}, lotSizeFilter_qtyStep={} or "
                "priceFilter_tickSize={} is empty",
                symbol, lotSizeFilter_qtyStep, priceFilter_tickSize));
        }
        std::string price2 = conv_to_dec_str_v2(price, priceFilter_tickSize);
        std::string quantity2 =
            conv_to_dec_str_v2(quantity, lotSizeFilter_qtyStep);
        return std::make_tuple(price2, quantity2);
    } else if (kind == "spot") {
        if (!spot_exchange_info.has_value()) {
            throw std::runtime_error("spot_exchange_info is not inited");
        }
        std::string basePrecision = "";
        std::string priceFilter_tickSize = "";
        for (auto& ex_info_obj : spot_exchange_info.value()["result"]["list"]) {
            if (ex_info_obj["symbol"] == symbol) {
                basePrecision = ex_info_obj["lotSizeFilter"]["basePrecision"];
                priceFilter_tickSize = ex_info_obj["priceFilter"]["tickSize"];
            }
        }
        if (basePrecision.empty()) {
            throw std::runtime_error("basePrecision is empty");
        }
        if (priceFilter_tickSize.empty()) {
            throw std::runtime_error("priceFilter_tickSize is empty");
        }
        std::string price2 = conv_to_dec_str_v2(price, priceFilter_tickSize);
        std::string quantity2 = conv_to_dec_str_v2(quantity, basePrecision);
        return std::make_tuple(price2, quantity2);
    } else {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
}

Order ClientPrivateBybit::place_fut_limit_order(std::string symbol,
                                                std::string side,
                                                std::string price,
                                                std::string quantity) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("unexpected kind={}", kind));
    }
    std::string side_int = "";
    if (side == "buy") {
        side_int = "Buy";
    } else if (side == "sell") {
        side_int = "Sell";
    } else {
        throw std::runtime_error(fmt::format("unproper side={}", side));
    }
    std::string timestamp_str = std::to_string(
        (long)(std::chrono::system_clock::now().time_since_epoch().count() /
               1000));
    std::string recw_window = "5000";
    std::string body_str = fmt::format(
        R"({{"category": "linear", "symbol": "{}", "side": "{}", "orderType": "Limit", "qty": "{}", "price": "{}", "timeInForce": "GTC", "isLeverage": 1}})",
        symbol, side_int, quantity, price);
    SPDLOG_INFO("{} {} place order side={} body_str={}", ex, kind, side_int,
                body_str);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, body_str);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/order/create";
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    nlohmann::json res_obj = execute_http_post_req(url, headers, body_str);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    if (res_obj["retCode"] != 0 || res_obj["retMsg"] != "OK") {
        throw std::runtime_error(
            fmt::format("{} {} order creation failed", ex, kind));
    }
    return Order{
        .ex = ex,
        .k = kind,
        .id = res_obj["result"]["orderId"],
        .open_ts = res_obj["time"],
        .s = symbol,
        .p = "0.0",
        .v = .0,
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

void ClientPrivateBybit::set_leverage_to_1(std::string symbol) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("unexpected kind=", kind));
    }
    std::string timestamp_str = std::to_string(
        (long)(std::chrono::system_clock::now().time_since_epoch().count() /
               1000));
    std::string recw_window = "5000";
    std::string body_str = fmt::format(
        R"({{"category": "linear", "symbol": "{}", "buyLeverage": "1", "sellLeverage": "1"}})",
        symbol);
    SPDLOG_INFO("set leverage body_str={}", body_str);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, body_str);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/position/set-leverage";
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    nlohmann::json res_obj = execute_http_post_req(url, headers, body_str);
    SPDLOG_DEBUG("res_obj={}", res_obj.dump());
}

Order ClientPrivateBybit::amend_fut_order(Order& order, std::string price) {
    if (kind != "fut") {
        throw std::runtime_error(fmt::format("unexpected kind=", kind));
    }
    std::string timestamp_str = std::to_string(
        (long)(std::chrono::system_clock::now().time_since_epoch().count() /
               1000));
    std::string recw_window = "5000";
    std::string body_str = fmt::format(
        R"({{"category": "linear", "symbol": "{}", "orderId": "{}", "price": "{}"}})",
        order.s, order.id, price);
    SPDLOG_INFO("{} {} amend order body_str={}", ex, kind, body_str);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, body_str);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/order/amend";
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    nlohmann::json res_obj = execute_http_post_req(url, headers, body_str);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = res_obj["result"]["orderId"],
        .open_ts = res_obj["time"],
        .s = "",
        .p = "0.0",
        .v = .0,
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

Order ClientPrivateBybit::place_spot_limit_order(std::string symbol,
                                                 std::string side,
                                                 std::string price,
                                                 std::string quantity) {
    if (kind != "spot") {
        throw std::runtime_error("unexpected kind=" + kind);
    }
    std::string side_int = "";
    if (side == "buy") {
        side_int = "Buy";
    } else if (side == "sell") {
        side_int = "Sell";
    } else {
        throw std::runtime_error(fmt::format("unexpected side=", side));
    }
    std::string timestamp_str = std::to_string(
        (long)(std::chrono::system_clock::now().time_since_epoch().count() /
               1000));
    std::string recw_window = "5000";
    std::string body_str = fmt::format(
        R"({{"category": "spot", "symbol": "{}", "side": "{}", "orderType": "Limit", "qty": "{}", "price": "{}"}})",
        symbol, side_int, quantity, price);
    SPDLOG_INFO("{} {} place order side={} body_str={}", ex, kind, side_int,
                body_str);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, body_str);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/order/create";
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    nlohmann::json res_obj = execute_http_post_req(url, headers, body_str);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    return Order{
        .ex = ex,
        .k = kind,
        .id = res_obj["result"]["orderId"],
        .open_ts = res_obj["time"],
        .s = symbol,
        .p = "0.0",
        .v = .0,
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
}

Order ClientPrivateBybit::fetch_order(Order& order) {
    std::string query_params = "";
    if (kind == "fut") {
        query_params = fmt::format("category=linear&orderId={}", order.id);
    } else if (kind == "spot") {
        query_params = fmt::format("category=spot&orderId={}", order.id);
    } else {
        throw std::runtime_error(
            fmt::format("{} unexpected kind={}", ex, kind));
    }
    std::string timestamp_str = std::to_string((long)(now_millis()));
    std::string recw_window = "5000";
    SPDLOG_INFO("fetch order query_params={}", query_params);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, query_params);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/order/realtime";
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    std::string url_path = fmt::format("{}?{}", url, query_params);
    nlohmann::json res_obj = execute_http_get_req(url_path, headers);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    if (res_obj["retCode"] != 0 || res_obj["retMsg"] != "OK" ||
        res_obj["result"]["list"].size() != 1) {
        throw std::runtime_error(
            fmt::format("{} {} order fetch failed", ex, kind));
    }
    nlohmann::json t = res_obj["result"]["list"][0];
    Order order_out = Order{
        .ex = ex,
        .k = kind,
        .id = t["orderId"],
        .open_ts = stol((std::string)t["createdTime"]),
        .s = t["symbol"],
        .p = t["price"],
        .v = stod((std::string)t["qty"]),
        .filled_amount = .0,
        .st = "PLACED_TO_ORDER_BOOK",
    };
    if (t["orderStatus"] == "Filled") {
        order_out.p_avg_fill = stod((std::string)t["avgPrice"]);
        order_out.st = "FILLED";
        order_out.fee_usdt = stod((std::string)t["cumExecFee"]);
        if (kind == "fut") {
            order_out.filled_amount = stod((std::string)t["cumExecQty"]);
        } else if (kind == "spot") {
            order_out.filled_amount = stod((std::string)t["cumExecQty"]) -
                                      stod((std::string)t["cumExecFee"]);
        } else {
            throw std::runtime_error(fmt::format("unexpected kind=", kind));
        }
    }
    return order_out;
}

double ClientPrivateBybit::fetch_order_fee_usdt(Order& order) {
    SPDLOG_INFO("{} fee_usdt is already fetched from /order/realtime", ex);
    return order.fee_usdt;
}

std::map<std::string, double> ClientPrivateBybit::fetch_balances() {
    std::string query_params = "accountType=UNIFIED";
    std::string timestamp_str = std::to_string(now_millis());
    std::string recw_window = "5000";
    SPDLOG_INFO("fetch wallet balances query_params={}", query_params);
    std::string param_str = fmt::format(R"({}{}{}{})", timestamp_str, api_key,
                                        recw_window, query_params);
    std::string sign = gen_hmac_sha256(api_secret, param_str);
    std::string url = "https://api.bybit.com/v5/account/wallet-balance";
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers =
        curl_slist_append(headers, ("X-BAPI-API-KEY: " + api_key).c_str());
    headers = curl_slist_append(headers,
                                ("X-BAPI-RECV-WINDOW: " + recw_window).c_str());
    headers = curl_slist_append(headers, ("X-BAPI-SIGN: " + sign).c_str());
    headers = curl_slist_append(headers, "X-BAPI-SIGN-TYPE: 2");
    headers = curl_slist_append(headers,
                                ("X-BAPI-TIMESTAMP: " + timestamp_str).c_str());
    std::string url_path = fmt::format("{}?{}", url, query_params);
    nlohmann::json res_obj = execute_http_get_req(url_path, headers);
    SPDLOG_DEBUG("{} {} res_obj={}", ex, kind, res_obj.dump());
    std::map<std::string, double> balances;
    for (auto& obj : res_obj["result"]["list"][0]["coin"]) {
        balances[obj["coin"]] = stod((std::string)obj["walletBalance"]);
    }
    return balances;
}

void ClientPrivateBybit::handle_onmessage(const std::string& msg) {
    nlohmann::json msg_obj = nlohmann::json::parse(msg);
    SPDLOG_DEBUG("ex={} kind={} msg_obj={}", ex, kind, msg_obj.dump());
    if (msg_obj["op"] == "auth" && msg_obj["success"] == true) {
        spdlog::info("bybit auth success");
        return;
    }
    if (msg_obj["op"] == "subscribe" && msg_obj["success"] == true) {
        spdlog::info("bybit subscribe success");
        is_subscribed_to_private_channels = true;
        return;
    }
    if (msg_obj["topic"] == "order") {
        nlohmann::json order_obj = msg_obj["data"][0];
        std::string order_id = order_obj["orderId"];
        SPDLOG_INFO("{} {} order appeared order_id={}", ex, kind, order_id);
        Order order = Order{
            .ex = ex,
            .k = kind,
            .id = order_id,
            .open_ts = stol((std::string)order_obj["createdTime"]),
            .s = order_obj["symbol"],
            .p = order_obj["price"],
            .v = stod((std::string)order_obj["qty"]),
            .filled_amount = 0.0,
            .st = "",
        };
        std::string order_status = order_obj["orderStatus"];
        if (order_status == "New") {
            order.st = "PLACED_TO_ORDER_BOOK";
        } else if (order_status == "Filled") {
            order.st = "FILLED";
            if (kind == "fut") {
                order.filled_amount =
                    stod((std::string)order_obj["cumExecQty"]);
            } else if (kind == "spot") {
                order.filled_amount =
                    stod((std::string)order_obj["cumExecQty"]) -
                    stod((std::string)order_obj["cumExecFee"]);
            } else {
                throw std::runtime_error(fmt::format("unexpected kind=", kind));
            }
        } else {
            throw std::runtime_error(
                fmt::format("unexpected order_status=", order_status));
        }
        orders.push_back(order);
        return;
    }
    SPDLOG_WARN("missing {} {} message msg_obj={}", ex, kind, msg_obj.dump());
}

std::string ClientPrivateBybit::sign_str(std::string qs0,
                                         std::string payload0) {
    throw std::runtime_error("not-implemented");
}

ClientPublicHtx::ClientPublicHtx(std::string kind)
    : ClientPublic("htx", kind) {}

void ClientPublicHtx::init_idle() {
    if (kind == "fut") {
        std::string url = "wss://api.hbdm.com/linear-swap-ws";
        init_idle_(url);
    } else if (kind == "spot") {
        std::string url = "wss://api.huobi.pro/ws";
        init_idle_(url);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicHtx::subscribe_to_trades(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "sub":"market.%s.trade.detail",
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "sub":"market.%s.trade.detail",
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicHtx::subscribe_to_depth(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "sub":"market.%s.depth.step0",
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "sub":["market.%s.depth.step0"],
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicHtx::unsubscribe_from_depth(std::string& symbol) {
    if (kind == "fut") {
        std::string t_template = R"({
                "unsub":"market.%s.depth.step0",
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "unsub":["market.%s.depth.step0"],
                "id":"t"
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicHtx::ping() {}

Ticker ClientPublicHtx::fetch_ticker(std::string& symbol) {
    throw std::runtime_error("not-implemented");
}

std::string ClientPublicHtx::parse_symbol_from_trade_ch(std::string& ch) {
    std::regex r("^market\\.(.*)\\.trade\\.detail$");
    std::smatch s_m;
    return std::regex_search(ch, s_m, r) ? s_m[1] : (std::string) "";
}

std::string ClientPublicHtx::parse_symbol_from_depth_ch(std::string& ch) {
    std::regex r("^market\\.(.*)\\.depth\\.step0$");
    std::smatch s_m;
    return std::regex_search(ch, s_m, r) ? s_m[1] : (std::string) "";
}

// https://github.com/HuobiRDCenter/huobi_Cpp/blob/master/include/gzDecompress.h#L5
int ClientPublicHtx::gzDecompress(const char* src, int srcLen, const char* dst,
                                  int dstLen) {
    z_stream strm;
    strm.zalloc = NULL;
    strm.zfree = NULL;
    strm.opaque = NULL;

    strm.avail_in = srcLen;
    strm.avail_out = dstLen;
    strm.next_in = (Bytef*)src;
    strm.next_out = (Bytef*)dst;

    int err = -1, ret = -1;
    err = inflateInit2(&strm, MAX_WBITS + 16);
    if (err == Z_OK) {
        err = inflate(&strm, Z_FINISH);
        if (err == Z_STREAM_END) {
            ret = strm.total_out;
        } else {
            inflateEnd(&strm);
            return err;
        }
    } else {
        inflateEnd(&strm);
        return err;
    }
    inflateEnd(&strm);
    return err;
}

std::vector<Depth> ClientPublicHtx::fetch_depth_snapshot(std::string& symbol) {
    if (kind == "fut") {
        std::string url =
            "https://api.hbdm.com/linear-swap-ex/market/"
            "depth?contract_code=" +
            symbol + "&type=step0";
        return parse_depth_from_res(execute_http_get_req(url));
    } else if (kind == "spot") {
        std::string url =
            "https://api.huobi.pro/market/depth?symbol=" + symbol +
            "&depth=20&type=step0";
        return parse_depth_from_res(execute_http_get_req(url));
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPublicHtx::handle_onmessage(const std::string& msg) {
    // NOTE: in huobi_Cpp guy made 40960+1024, but longer messages appear
    //       https://github.com/HuobiRDCenter/huobi_Cpp/blob/master/include/define.h#L13
    int BUFF = pow(2, 17);
    if (msg.size() > BUFF) {
        spdlog::warn("too long {} msg for kind={} -> miss it", ex, kind);
        return;
    }
    char buf[BUFF];
    ClientPublicHtx::gzDecompress(msg.c_str(), msg.size(), buf, BUFF);
    nlohmann::json msg_obj = nlohmann::json::parse((std::string)buf);
    memset(&buf[0], 0, sizeof(buf));
    if (msg_obj.contains("ping")) {
        spdlog::debug("{} {} ping -> send pong", kind, ex);
        long ping_val = msg_obj["ping"];
        send("{\"pong\":" + std::to_string(ping_val) + "}");
        return;
    } else if (msg_obj.contains("subbed") && msg_obj["status"] == "ok") {
        spdlog::info("{} {} subscribed successfully", ex, kind);
        return;
    } else if (msg_obj.contains("unsubbed") && msg_obj["status"] == "ok") {
        spdlog::debug("{} {} unsubbed -> clear order-book", kind, ex);
        order_book_cache.clear();
        return;
    }
    if (kind == "fut") {
        if (msg_obj.contains("ch")) {
            std::string ch = msg_obj["ch"];
            if (str_ends_with(ch, ".depth.step0")) {
                Depth d = parse_depth_from_res(msg_obj.dump())[0];
                if (order_book_cache.get_last_update_id() == 0) {
                    std::string symbol =
                        ClientPublicHtx::parse_symbol_from_depth_ch(ch);
                    auto snapshot_depths = fetch_depth_snapshot(symbol);
                    Depth d2 = snapshot_depths[0];
                    order_book_cache.apply_orders(d2.u, d2.asks, d2.bids);
                }
                long ver = order_book_cache.get_last_update_id();
                if (d.u == ver + 1) {
                    order_book_cache.apply_orders(d.u, d.asks, d.bids);
                } else if (d.u - ver >= 2) {
                    spdlog::warn(
                        "htx fut depth is broken u={} ver={} -> reload", d.u,
                        ver);
                    order_book_cache.clear();
                    order_book_cache.apply_orders(d.u, d.asks, d.bids);
                }
                onmessage_depth(d);
            } else {
                // TODO: add condition for detecting a message based on ch
                std::string ch = msg_obj["ch"];
                std::string symbol =
                    ClientPublicHtx::parse_symbol_from_trade_ch(ch);
                for (auto& trade_raw : msg_obj["tick"]["data"]) {
                    TradeInt trade = TradeInt::new_(
                        "htx", symbol, kind, trade_raw["ts"],
                        trade_raw["price"], trade_raw["quantity"]);
                    onmessage_trade(trade);
                }
            }
        } else {
            throw std::runtime_error("unexpected msg=" + msg);
        }
    } else if (kind == "spot") {
        if (msg_obj.contains("ch")) {
            std::string ch = msg_obj["ch"];
            if (str_ends_with(ch, ".depth.step0")) {
                Depth d = parse_depth_from_res(msg_obj.dump())[0];
                spdlog::warn(
                    "order book contains just last snapshot (because of "
                    "fancy htx implementation where vecrsions are random)");
                order_book_cache.clear();
                order_book_cache.apply_orders(d.u, d.asks, d.bids);
                onmessage_depth(d);
            } else {
                // TODO: add condition for detecting a message based on ch
                std::string symbol =
                    ClientPublicHtx::parse_symbol_from_trade_ch(ch);
                for (auto& trade_raw : msg_obj["tick"]["data"]) {
                    TradeInt trade =
                        TradeInt::new_("htx", symbol, kind, trade_raw["ts"],
                                       trade_raw["price"], trade_raw["amount"]);
                    onmessage_trade(trade);
                }
            }
        } else {
            throw std::runtime_error("unexpected msg=" + msg);
        }
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

std::vector<Depth> ClientPublicHtx::parse_depth_from_res(nlohmann::json obj) {
    std::vector<std::tuple<std::string, double>> bids;
    for (auto& b : obj["tick"]["bids"]) {
        std::string p = b[0].dump();
        bids.push_back({p, b[1]});
    }
    std::vector<std::tuple<std::string, double>> asks;
    for (auto& b : obj["tick"]["asks"]) {
        std::string p = b[0].dump();
        asks.push_back({p, b[1]});
    }
    return std::vector<Depth>{Depth{
        .u = obj["tick"]["version"],
        .asks = asks,
        .bids = bids,
    }};
}

ClientPrivateHtx::ClientPrivateHtx(std::string kind)
    : ClientPrivate("htx", kind) {
    api_key = std::getenv("HTX_API_ACCESS_KEY");
    api_secret = std::getenv("HTX_API_SECRET_KEY");
}

void ClientPrivateHtx::init_idle() {
    throw std::runtime_error("not-implemented");
}

void ClientPrivateHtx::init_exchange_info() {
    throw std::runtime_error("not-implemented");
}

void ClientPrivateHtx::subscribe_to_private_events() {
    throw std::runtime_error("not-implemented");
}

std::string htx_base64_encode(const unsigned char* input, int length) {
    BIO* bmem = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);  // Do not add newlines
    bmem = BIO_push(b64, bmem);
    BIO_write(bmem, input, length);
    BIO_flush(bmem);
    BUF_MEM* bptr;
    BIO_get_mem_ptr(bmem, &bptr);
    std::string output(bptr->data, bptr->length);
    BIO_free_all(bmem);
    return output;
}

std::string url_encode(const std::string& value) {
    std::ostringstream encoded;
    for (const char c : value) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' ||
            c == '_' || c == '.' || c == '~') {
            encoded << c;
        } else {
            encoded << '%' << std::uppercase << std::hex << std::setw(2)
                    << std::setfill('0')
                    << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return encoded.str();
}

std::tuple<std::string, std::string> ClientPrivateHtx::adjust_price_quantity(
    std::string symbol, double price, double quantity) {
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateHtx::place_fut_limit_order(std::string symbol,
                                              std::string side,
                                              std::string price,
                                              std::string quantity) {
    // TODO: adjust side
    // POST /v1/order/orders/place
    // https://api.huobi.pro
    // https://api.hbdm.com/linear-swap-api/v1/swap_order
    std::string qs0 = fmt::format(
        "AccessKeyId={}&SignatureVersion=2&SignatureMethod=HmacSHA256&"
        "Timestamp={}",
        api_key,
        url_encode(
            time_point_to_str(std::chrono::system_clock::now(), "%FT%T")));
    // std::string qs0 =
    //     "AccessKeyId=api-key-44&SignatureMethod=HmacSHA256&SignatureVersion=2&"
    //     "Timestamp=2024-12-08T17%3A42%3A27";
    std::string payload0 = fmt::format(
        "POST\napi.hbdm.com\n/linear-swap-api/v1/swap_order\n{}", qs0);
    std::string sign = sign_str(qs0, payload0);
    std::string url = fmt::format(
        "https://api.hbdm.com/linear-swap-api/v1/swap_order?{}&Signature={}",
        qs0, url_encode(sign));
    std::string body = R"({{
        "contract_code":"eth-usdt",
        "price":"3900",
        "volume":0.001,
        "direction":"buy",
        "lever_rate":1,
        "order_price_type":"limit"
    }})";
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    CURL* curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    CURLcode res_code = curl_easy_perform(curl);
    std::cout << "res_buf=" << res_buf << std::endl;
    nlohmann::json res_obj = nlohmann::json::parse(res_buf);
    curl_easy_cleanup(curl);
    throw std::runtime_error("implement return Order");
}

void ClientPrivateHtx::set_leverage_to_1(std::string symbol) {
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateHtx::amend_fut_order(Order& order, std::string price) {
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateHtx::place_spot_limit_order(std::string symbol,
                                               std::string side,
                                               std::string price,
                                               std::string quantity) {
    // TODO: adjust side
    throw std::runtime_error("not-implemented");
}

Order ClientPrivateHtx::fetch_order(Order& order) {
    throw std::runtime_error("not-implemented");
}

double ClientPrivateHtx::fetch_order_fee_usdt(Order& order) {
    throw std::runtime_error("not-implemented");
}

std::map<std::string, double> ClientPrivateHtx::fetch_balances() {
    throw std::runtime_error("not-implemented");
}

void ClientPrivateHtx::handle_onmessage(const std::string& msg) {
    throw std::runtime_error("not-implemented");
}

std::string ClientPrivateHtx::sign_str(std::string qs0, std::string payload0) {
    unsigned char* hmac_result;
    unsigned int hmac_length = 0;
    hmac_result = HMAC(EVP_sha256(), api_secret.c_str(), api_secret.length(),
                       reinterpret_cast<const unsigned char*>(payload0.c_str()),
                       payload0.length(), nullptr, &hmac_length);
    BIO* bmem = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bmem = BIO_push(b64, bmem);
    BIO_write(bmem, hmac_result, hmac_length);
    BIO_flush(bmem);
    BUF_MEM* bptr;
    BIO_get_mem_ptr(bmem, &bptr);
    std::string sign(bptr->data, bptr->length);
    BIO_free_all(bmem);
    return sign;
}

const std::string TELEGRAM_NOTIFY_PRETTY_TEMPLATE = R"({{
    "message": "{}",
    "action": "{}",
    "now": "{}"
}})";

TelegramBotPort::TelegramBotPort(std::string token_, std::string chat_id_) {
    token = token_;
    chat_id = chat_id_;
};

TelegramBotPort TelegramBotPort::new_from_envs() {
    return TelegramBotPort(std::getenv("TELEGRAM_BOT_API_KEY"),
                           std::getenv("TELEGRAM_BOT_CHAT_ID"));
}

void TelegramBotPort::notify_pretty(std::string message, std::string action) {
    std::string m_raw =
        fmt::format(TELEGRAM_NOTIFY_PRETTY_TEMPLATE, message, action,
                    time_point_to_str(std::chrono::system_clock::now()));
    std::string m_encoded = fmt::format("```%0A{}```", url_encode(m_raw));
    std::string url_str = fmt::format(
        "https://api.telegram.org/bot{}/"
        "sendMessage?chat_id={}&text={}&parse_mode=Markdown",
        token, chat_id, m_encoded);
    execute_http_get_req(url_str);
}
