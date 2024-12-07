#include "clients.hpp"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <spdlog/spdlog.h>
#include <zlib.h>

#include <iomanip>
#include <regex>

static size_t execute_http_req_write_cb(void* contents, size_t size,
                                        size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

std::string execute_http_req(std::string& url) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    return readBuffer;
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
        ws_onopen_received = true;
        spdlog::info("{} onopen kind={}", ex, kind);
    };
    onclose = [&]() {
        spdlog::info("{} onclose kind={}", ex, kind);
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
        std::string t_template = R"({
                "time": %d,
                "channel" : "futures.order_book_update",
                "event": "subscribe",
                "payload" : ["%s", "100ms", "100"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "time": %d,
                "channel": "spot.order_book_update",
                "event": "subscribe",
                "payload": ["%s", "100ms"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
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

std::vector<Depth> ClientPublicGateio::fetch_depth_snapshot(
    std::string& symbol) {
    if (kind == "fut") {
        std::string url =
            "https://api.gateio.ws/api/v4/futures/usdt/"
            "order_book?limit=100&with_id=true&contract=" +
            symbol;
        nlohmann::json obj = nlohmann::json::parse(execute_http_req(url));
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
        nlohmann::json obj = nlohmann::json::parse(execute_http_req(url));
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
                            .asks = asks,
                            .bids = bids};
            if (d.u > order_book_cache.get_last_update_id()) {
                spdlog::warn(
                    "[gateio] order book contains non-incremental depth");
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
        std::string t_template = R"({
                "method":"sub.depth",
                "param":{"symbol":"%s"}
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else if (kind == "spot") {
        std::string t_template = R"({
                "method": "SUBSCRIPTION",
                "params": ["spot@public.increase.depth.v3.api@%s"]
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
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

std::vector<Depth> ClientPublicMexc::fetch_depth_snapshot(std::string& symbol) {
    if (kind == "fut") {
        std::string url =
            "https://contract.mexc.com/api/v1/contract/depth_commits/" +
            symbol + "/100000";
        nlohmann::json obj = nlohmann::json::parse(execute_http_req(url));
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
        nlohmann::json obj = nlohmann::json::parse(execute_http_req(url));
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
            spdlog::info("mexc spot subscribed successfully");
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
            throw std::runtime_error("unexpected msg=" + msg);
        }
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

ClientPrivateMexc::ClientPrivateMexc(std::string kind)
    : ClientPublic("mexc", kind) {
    api_key = std::getenv("MEXC_API_KEY");
    api_secret = std::getenv("MEXC_API_SECRET");
}

void ClientPrivateMexc::init_idle_private(std::string& listen_key) {
    if (kind == "spot") {
        std::string url = "wss://wbs.mexc.com/ws?listenKey=" + listen_key;
        init_idle_(url);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPrivateMexc::subscribe_to_private_events() {
    if (kind == "spot") {
        std::string s = R"({
                "method": "SUBSCRIPTION",
                "params": [
                   "spot@private.account.v3.api",
                   "spot@private.deals.v3.api",
                   "spot@private.orders.v3.api"
                ]
            })";
        send(s);
    } else {
        throw std::runtime_error("unexpected kind=" + kind);
    }
}

void ClientPrivateMexc::place_spot_limit_order() {
    long timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000;
    std::string query =
        "symbol=DNXUSDT&side=BUY&type=LIMIT&price=0.2800&"
        "quantity=60.0&recvWindow=60000&timestamp=" +
        std::to_string(timestamp);
    query += "&signature=" + sign_str(query);
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
    CURL* curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, "https://api.mexc.com/api/v3/order");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query.c_str());
    CURLcode res_code = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(res_buf);
    curl_easy_cleanup(curl);
}

void ClientPrivateMexc::place_fut_limit_order() {
    // NOTE: placing fut order is "Under maintenance"
    // https://mexcdevelop.github.io/apidocs/contract_v1_en/#order-under-maintenance
    // https://www.mexc.com/support/articles/15149585234969
    throw std::runtime_error("not-implemented");
}

std::string ClientPrivateMexc::create_listen_key() {
    long timestamp =
        std::chrono::system_clock::now().time_since_epoch().count() / 1000;
    std::string query = "timestamp=" + std::to_string(timestamp);
    query += "&signature=" + sign_str(query);
    std::string res_buf;
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
    CURL* curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL,
                     "https://api.mexc.com/api/v3/userDataStream");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, execute_http_req_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query.c_str());
    CURLcode res_code = curl_easy_perform(curl);
    nlohmann::json res_obj = nlohmann::json::parse(res_buf);
    curl_easy_cleanup(curl);
    return res_obj["listenKey"];
}

std::string ClientPrivateMexc::sign_str(std::string& query) {
    unsigned char* signature_digest;
    unsigned int signature_digest_len;
    signature_digest =
        HMAC(EVP_sha256(), api_secret.c_str(), api_secret.length(),
             reinterpret_cast<const unsigned char*>(query.c_str()),
             strlen(query.c_str()), nullptr, &signature_digest_len);
    std::ostringstream signature_ss;
    for (unsigned int i = 0; i < signature_digest_len; ++i) {
        signature_ss << std::hex << std::setw(2) << std::setfill('0')
                     << static_cast<int>(signature_digest[i]);
    }
    return signature_ss.str();
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
        return parse_depth_from_res(execute_http_req(url));
    } else if (kind == "spot") {
        std::string url =
            "https://api.huobi.pro/market/depth?symbol=" + symbol +
            "&depth=20&type=step0";
        return parse_depth_from_res(execute_http_req(url));
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

std::vector<Depth> ClientPublicHtx::parse_depth_from_res(std::string s) {
    nlohmann::json obj = nlohmann::json::parse(s);
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
