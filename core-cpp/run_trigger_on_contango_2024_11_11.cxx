#include <WebSocketClient.h>
#include <clickhouse/client.h>
#include <curl/curl.h>
#include <gmpxx.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <openssl/hmac.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>
#include <zlib.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <regex>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "grpcpp/grpcpp.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "src/models.hpp"
#include "trade_contango.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using trade_contango::FireTradeReq;
using trade_contango::FireTradeRes;
using trade_contango::QTickerReq;
using trade_contango::SpreadsReq;
using trade_contango::TradeContango;

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

void configure_logger();
void listen_gateio_tickers_debug();
void listen_gateio_tickers_v1();
void listen_gateio_tickers_v2(int argc, char** argv);

int main(int argc, char** argv) {
    configure_logger();
    std::string v = std::getenv("TICKERS_VERSION");
    if (v == "debug") {
        listen_gateio_tickers_debug();
    } else if (v == "v1") {
        listen_gateio_tickers_v1();
    } else if (v == "v2") {
        listen_gateio_tickers_v2(argc, argv);
    } else {
        throw std::runtime_error("unexpected TICKERS_VERSION=" + v);
    }
    return 0;
}

std::ostream& operator<<(std::ostream& os, TradeInt const& o) {
    os << o.toString();
    return os;
}

std::string format_as(TradeInt const& o) {
    std::ostringstream ss;
    ss << o.toString();
    return std::move(ss).str();
}

std::ostream& operator<<(std::ostream& os, QTickerReq const& o) {
    os << "QTickerReq{ex=" << o.ex() << ",s=" << o.s() << ",st=" << o.st()
       << ",k=" << o.k() << ",ts=" << o.ts() << ",p=" << o.p() << ",v=" << o.v()
       << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, SpreadsReq const& o) {
    os << "SpreadsReq{p_spot=" << o.p_spot() << ",p_fut=" << o.p_fut()
       << ",diff_rel=" << o.diff_rel() << ",t_spot=" << o.t_spot()
       << ",t_fut=" << o.t_fut() << "}";
    return os;
}

std::string format_as(SpreadsReq const& o) {
    std::ostringstream ss;
    ss << o;
    return std::move(ss).str();
}

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

struct Depth {
    long u;
    std::vector<std::tuple<std::string, double>> asks;
    std::vector<std::tuple<std::string, double>> bids;
};

std::ostream& operator<<(std::ostream& os, Depth const& d) {
    os << "u=" << d.u << ", asks=[";
    for (int i = 0; i < d.asks.size(); i++) {
        os << "(" << std::get<0>(d.asks[i]) << "," << std::get<1>(d.asks[i])
           << ")";
        if (i < d.asks.size() - 1) {
            os << ",";
        }
    }
    os << "], bids=[";
    for (int i = 0; i < d.bids.size(); i++) {
        os << "(" << std::get<0>(d.bids[i]) << "," << std::get<1>(d.bids[i])
           << ")";
        if (i < d.bids.size() - 1) {
            os << ",";
        }
    }
    os << "]";
    return os;
}

class ClientPublic : public hv::WebSocketClient {
   public:
    bool ws_onopen_received = false;
    bool ws_onclose_received = false;
    std::string ex;
    std::string kind;
    OrderBookCache order_book_cache;
    std::function<void(const TradeInt trade_int)> onmessage_trade;
    std::function<void(const Depth depth)> onmessage_depth;

    ClientPublic(std::string ex_, std::string kind_,
                 hv::EventLoopPtr loop = NULL)
        : WebSocketClient(loop) {
        ex = ex_;
        kind = kind_;
    }
    ~ClientPublic() {}

    virtual void init_idle() = 0;
    virtual void subscribe_to_trades(std::string& symbol) = 0;
    virtual void subscribe_to_depth(std::string& symbol) = 0;
    virtual void unsubscribe_from_depth(std::string& symbol) = 0;
    virtual void ping() = 0;

   protected:
    virtual void handle_onmessage(const std::string& msg) = 0;
    void init_idle_(std::string& url) {
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
};

class ClientPublicGateio : public ClientPublic {
   public:
    ClientPublicGateio(std::string kind) : ClientPublic("gateio", kind) {}

    void init_idle() {
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

    void subscribe_to_trades(std::string& symbol) {
        if (kind == "fut") {
            std::string t_template = R"({
                "time" : %d,
                "channel" : "futures.trades",
                "event": "subscribe",
                "payload" : ["%s"]
            })";
            int ts_secs =
                std::chrono::system_clock::now().time_since_epoch().count() /
                1000 / 1000;
            char t[256];
            snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
            send(t);
        } else if (kind == "spot") {
            int ts_secs =
                std::chrono::system_clock::now().time_since_epoch().count() /
                1000 / 1000;
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

    void subscribe_to_depth(std::string& symbol) {
        int ts_secs =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000 /
            1000;
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

    void unsubscribe_from_depth(std::string& symbol) {
        int ts_secs =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000 /
            1000;
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

    void ping() {}

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol) {
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

   protected:
    void handle_onmessage(const std::string& msg) {
        nlohmann::json msg_obj = nlohmann::json::parse(msg);
        std::string event = msg_obj["event"];
        if (event == "subscribe" && msg_obj["result"]["status"] == "success") {
            spdlog::info("gateio {} subscribed successfully", kind);
            return;
        }
        if (event == "unsubscribe" &&
            msg_obj["result"]["status"] == "success") {
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
                    TradeInt trade = TradeInt::new_(
                        "gateio", trade_raw["contract"], kind,
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
};

class ClientPublicMexc : public ClientPublic {
   public:
    ClientPublicMexc(std::string kind) : ClientPublic("mexc", kind) {}

    void init_idle() {
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

    void subscribe_to_trades(std::string& symbol) {
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

    void subscribe_to_depth(std::string& symbol) {
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

    void unsubscribe_from_depth(std::string& symbol) {
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

    void ping() {
        if (kind == "fut") {
            send(R"({"method": "ping"})");
        }
    }

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol) {
        if (kind == "fut") {
            std::string url =
                "https://contract.mexc.com/api/v1/contract/depth_commits/" +
                symbol + "/100000";
            nlohmann::json obj = nlohmann::json::parse(execute_http_req(url));
            if (!((bool)obj["success"])) {
                throw std::runtime_error("res is not success");
            }
            std::vector<nlohmann::json> data = obj["data"];
            sort(data.begin(), data.end(),
                 [](nlohmann::json a, nlohmann::json b) {
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
            std::string url =
                "https://api.mexc.com/api/v3/depth?symbol=" + symbol +
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

   protected:
    void handle_onmessage(const std::string& msg) {
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
            } else if (channel == "rs.sub.deal" &&
                       msg_obj["data"] == "success") {
                spdlog::info("mexc fut subscribed successfully");
            } else if (channel == "pong") {
                spdlog::debug("handle pong");
            } else if (channel == "push.deal") {
                TradeInt trade = TradeInt::new_(
                    "mexc", msg_obj["symbol"], kind, msg_obj["ts"],
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
                            order_book_cache.apply_orders(d2.u, d2.asks,
                                                          d2.bids);
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

   private:
    bool subscribed_to_depth = false;
};

class ClientPrivateMexc : public ClientPublic {
   public:
    ClientPrivateMexc(std::string kind) : ClientPublic("mexc", kind) {
        api_key = std::getenv("MEXC_API_KEY");
        api_secret = std::getenv("MEXC_API_SECRET");
    }

    void init_idle_private(std::string& listen_key) {
        if (kind == "spot") {
            std::string url = "wss://wbs.mexc.com/ws?listenKey=" + listen_key;
            init_idle_(url);
        } else {
            throw std::runtime_error("unexpected kind=" + kind);
        }
    }

    void subscribe_to_private_events() {
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

    void place_spot_limit_order() {
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
        headers =
            curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
        CURL* curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_URL,
                         "https://api.mexc.com/api/v3/order");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                         execute_http_req_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query.c_str());
        CURLcode res_code = curl_easy_perform(curl);
        nlohmann::json res_obj = nlohmann::json::parse(res_buf);
        curl_easy_cleanup(curl);
    }

    static void place_fut_limit_order() {
        // NOTE: placing fut order is "Under maintenance"
        // https://mexcdevelop.github.io/apidocs/contract_v1_en/#order-under-maintenance
        // https://www.mexc.com/support/articles/15149585234969
        throw std::runtime_error("not-implemented");
    }

    std::string create_listen_key() {
        long timestamp =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string query = "timestamp=" + std::to_string(timestamp);
        query += "&signature=" + sign_str(query);
        std::string res_buf;
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers =
            curl_slist_append(headers, ("X-MEXC-APIKEY: " + api_key).c_str());
        CURL* curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_URL,
                         "https://api.mexc.com/api/v3/userDataStream");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                         execute_http_req_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res_buf);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query.c_str());
        CURLcode res_code = curl_easy_perform(curl);
        nlohmann::json res_obj = nlohmann::json::parse(res_buf);
        curl_easy_cleanup(curl);
        return res_obj["listenKey"];
    }

   private:
    std::string api_key;
    std::string api_secret;

    std::string sign_str(std::string& query) {
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
};

class ClientPublicBybit : public ClientPublic {
   public:
    ClientPublicBybit(std::string kind) : ClientPublic("bybit", kind) {}

    void init_idle() {
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

    void subscribe_to_trades(std::string& symbol) {
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

    void subscribe_to_depth(std::string& symbol) {
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

    void unsubscribe_from_depth(std::string& symbol) {
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

    void ping() {}

   protected:
    void handle_onmessage(const std::string& msg) {
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
                    throw std::runtime_error("bybit fut unknown type=" +
                                             msg_type);
                }
            } else {
                // TODO: add topic check for trades
                if (msg_type != "snapshot") {
                    throw std::runtime_error("bybit fut unknown type=" +
                                             msg_type);
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
                    throw std::runtime_error("bybit fut unknown type=" +
                                             msg_type);
                }
            } else {
                // TODO: add topic check
                if (msg_type != "snapshot") {
                    throw std::runtime_error("bybit fut unknown type=" +
                                             msg_type);
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

   private:
    Depth parse_depth(nlohmann::json& msg_obj) {
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
};

class ClientPublicHtx : public ClientPublic {
   public:
    ClientPublicHtx(std::string kind) : ClientPublic("htx", kind) {}

    void init_idle() {
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

    void subscribe_to_trades(std::string& symbol) {
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

    void subscribe_to_depth(std::string& symbol) {
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

    void unsubscribe_from_depth(std::string& symbol) {
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

    void ping() {}

    static std::string parse_symbol_from_trade_ch(std::string& ch) {
        std::regex r("^market\\.(.*)\\.trade\\.detail$");
        std::smatch s_m;
        return std::regex_search(ch, s_m, r) ? s_m[1] : (std::string) "";
    }

    static std::string parse_symbol_from_depth_ch(std::string& ch) {
        std::regex r("^market\\.(.*)\\.depth\\.step0$");
        std::smatch s_m;
        return std::regex_search(ch, s_m, r) ? s_m[1] : (std::string) "";
    }

    // https://github.com/HuobiRDCenter/huobi_Cpp/blob/master/include/gzDecompress.h#L5
    static int gzDecompress(const char* src, int srcLen, const char* dst,
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

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol) {
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

   protected:
    void handle_onmessage(const std::string& msg) {
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
                            "htx fut depth is broken u={} ver={} -> reload",
                            d.u, ver);
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
                        TradeInt trade = TradeInt::new_(
                            "htx", symbol, kind, trade_raw["ts"],
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

   private:
    std::vector<Depth> parse_depth_from_res(std::string s) {
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
};

struct OpportunityRow {
    std::string symbol_int_1;
    double fut_price;
    double spot_price;
    std::string fut_ex;
    std::string spot_ex;
    std::string fut_symbol;
    std::string spot_symbol;
    double diff_abs;
    double diff_rel;
    static OpportunityRow newFromClickhouseBlock(const clickhouse::Block& b,
                                                 int i) {
        return OpportunityRow{
            .symbol_int_1 =
                (std::string)b[1]->As<clickhouse::ColumnString>()->At(i),
            .fut_price = b[2]->As<clickhouse::ColumnFloat64>()->At(i),
            .spot_price = b[3]->As<clickhouse::ColumnFloat64>()->At(i),
            .fut_ex = (std::string)b[4]->As<clickhouse::ColumnString>()->At(i),
            .spot_ex = (std::string)b[5]->As<clickhouse::ColumnString>()->At(i),
            .fut_symbol =
                (std::string)b[6]->As<clickhouse::ColumnString>()->At(i),
            .spot_symbol =
                (std::string)b[7]->As<clickhouse::ColumnString>()->At(i),
            .diff_abs = b[8]->As<clickhouse::ColumnFloat64>()->At(i),
            .diff_rel = b[9]->As<clickhouse::ColumnFloat64>()->At(i),
        };
    }
};

std::ostream& operator<<(std::ostream& os, OpportunityRow const& o) {
    os << "{symbol_int_1=" << o.symbol_int_1 << ",fut_price=" << o.fut_price
       << ",spot_price=" << o.spot_price << ",fut_ex=" << o.fut_ex
       << ",spot_ex=" << o.spot_ex << ",fut_symbol=" << o.fut_symbol
       << ",spot_symbol=" << o.spot_symbol << ",diff_abs=" << o.diff_abs
       << ",diff_rel=" << o.diff_rel << "}";
    return os;
}

std::string format_as(OpportunityRow const& o) {
    std::ostringstream ss;
    ss << o;
    return std::move(ss).str();
}

std::string replace_first(const std::string& s_in, std::string const& toReplace,
                          std::string const& replaceWith) {
    std::string s = s_in;
    std::size_t pos = s.find(toReplace);
    if (pos == std::string::npos) {
        return s;
    }
    s.replace(pos, toReplace.length(), replaceWith);
    return s;
}

void configure_logger() {
    // XXX: add file-name, fn-name, line-number
    auto level = spdlog::level::from_str(std::getenv("SPDLOG_LEVEL"));
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(
        std::make_shared<spdlog::sinks::ansicolor_stdout_sink_st>());
    sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_st>(
        ".var/logfile", 0, 0));
    for (auto& s : sinks) {
        s->set_level(level);
    }
    auto l = std::make_shared<spdlog::logger>("default–global", begin(sinks),
                                              end(sinks));
    l->set_level(level);
    spdlog::set_default_logger(l);
}

std::optional<OpportunityRow> is_opportunity_exists(
    clickhouse::Client& client) {
    const std::string QUERY_OPPORTUNITIES = R"(
        WITH t AS (
            SELECT
                exchange,
                kind,
                symbol,
                UPPER(replaceRegexpAll(symbol, '[10*_-]?', '')) symbol_int_1,
                price / COALESCE(toFloat64OrNull(regexpExtract(symbol, '10*', 0)), 1) price_int_1
            FROM default.trade_contango_arbitrage_v1
            FINAL
            WHERE timestamp >= (now() - toIntervalSecond(60))
                AND length(replaceRegexpOne(symbol, '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) = length(symbol)
                AND volume > 0
                AND status = 'TRADING'
                AND symbol_int_1 != 'DEFIUSDT'
        )
        SELECT
            now() ts,
            t1.symbol_int_1,
            t1.price_int_1 AS fut_price,
            t2.price_int_1 AS spot_price,
            t1.exchange AS fut_ex,
            t2.exchange AS spot_ex,
            t1.symbol AS fut_symbol,
            t2.symbol AS spot_symbol,
            round(fut_price - spot_price, 4) AS diff_abs,
            round((fut_price - spot_price) / spot_price * 100, 2) AS diff_rel
        FROM (
            SELECT *
            FROM (
                SELECT
                    symbol,
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'futures'
            )
            WHERE _rownum = 1
        ) t1
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    symbol,
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'spot'
            )
            WHERE _rownum = 1
        ) t2
            ON t1.symbol_int_1 = t2.symbol_int_1
        WHERE diff_rel > %(threshold_rel)s
            AND spot_ex in ('bybit', 'mexc', 'gateio', 'htx')
            AND fut_ex in ('bybit', 'mexc', 'gateio', 'htx')
        ORDER BY diff_rel DESC
    )";
    std::optional<OpportunityRow> opp_row_t = {};
    std::string threshold_rel =
        std::to_string(std::stod(std::getenv("THRESHOLD_REL")));
    client.Select(
        replace_first(QUERY_OPPORTUNITIES, "%(threshold_rel)s", threshold_rel),
        [&](const clickhouse::Block& b) {
            if (b.GetRowCount() == 0) {
                return;
            }
            for (size_t i = 0; i < b.GetRowCount(); ++i) {
                OpportunityRow opp_row =
                    OpportunityRow::newFromClickhouseBlock(b, i);
                if (!opp_row_t.has_value()) {
                    opp_row_t = opp_row;
                }
            }
        });
    return opp_row_t;
}

class SpreadsHouse {
   public:
    SpreadsHouse() {}

    void init_idle() {
        if (ws_clients.size() == 0) {
            ws_clients["bybit-fut"] = new ClientPublicBybit("fut");
            ws_clients["bybit-spot"] = new ClientPublicBybit("spot");
            ws_clients["mexc-fut"] = new ClientPublicMexc("fut");
            ws_clients["mexc-spot"] = new ClientPublicMexc("spot");
            ws_clients["gateio-fut"] = new ClientPublicGateio("fut");
            ws_clients["gateio-spot"] = new ClientPublicGateio("spot");
            ws_clients["htx-fut"] = new ClientPublicHtx("fut");
            ws_clients["htx-spot"] = new ClientPublicHtx("spot");
        }
        for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
            o->second->init_idle();
        }
        auto is_all_ws_connected = [&]() -> bool {
            for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
                if (!o->second->isConnected() ||
                    !o->second->ws_onopen_received) {
                    return false;
                }
            }
            return true;
        };
        for (int i = 0; i < 100; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (is_all_ws_connected()) {
                spdlog::info("connections set up");
                break;
            }
        }
        if (!is_all_ws_connected()) {
            throw std::runtime_error("connections haven't been set up");
        }
    }

    void ping() {
        for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
            o->second->ping();
        }
    }

    void close_all() {
        for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
            o->second->close();
        }
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            spdlog::debug("tick on if all ws clients are closed");
            bool all_closed = true;
            for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
                if (!o->second->ws_onclose_received) {
                    all_closed = false;
                }
            }
            if (all_closed) {
                spdlog::debug("all closed => break");
                break;
            }
        }
        spdlog::debug("clean order-books");
        for (auto o = ws_clients.cbegin(); o != ws_clients.cend(); ++o) {
            o->second->order_book_cache.clear();
        }
    }

    ClientPublic* get_client(std::string ex, std::string kind) {
        std::string k = ex + "-" + kind;
        if (ws_clients.find(k) == ws_clients.end()) {
            throw std::runtime_error("unexpected key=" + k);
        }
        return ws_clients[k];
    }

   private:
    std::map<std::string, ClientPublic*> ws_clients;
};

void listen_gateio_tickers_debug() {
    std::string symbol = "btcusdt";
    ClientPublicHtx client("spot");
    client.init_idle();
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    client.onmessage_depth = [&](const Depth depth) {
        std::cout << "depth=" << depth << std::endl;
    };
    client.subscribe_to_depth(symbol);
    std::thread _([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            client.ping();
        }
    });
    for (int i = 0; i < 20; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        spdlog::info("tick idle 1");
        if (client.order_book_cache.get_last_update_id() > 0) {
            continue;
            double ask = client.order_book_cache.get_bottom_ask().get_d();
            double bid = client.order_book_cache.get_top_bid().get_d();
            std::cout << "\x1B[2J\x1B[H"
                      << "ask=" << ask << std::endl
                      << "bid=" << bid << std::endl;
            client.order_book_cache.print(7);
        }
    }
    client.unsubscribe_from_depth(symbol);
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        spdlog::info("tick idle 2");
    }
}

void listen_gateio_tickers_v1() {
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::map<std::string, long> last_timestamps;
    std::map<std::string, double> last_prices;
    SpreadsHouse sh;
    sh.init_idle();
    std::thread _([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            sh.ping();
        }
    });
    OpportunityRow opp_row;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        std::optional<OpportunityRow> opp_row_opt =
            is_opportunity_exists(clickhouse_client);
        if (opp_row_opt.has_value()) {
            opp_row = opp_row_opt.value();
            spdlog::info("found opp={}", opp_row);
            break;
        } else {
            spdlog::info("there is no opp");
        }
    }
    auto handle_trades = [&](const TradeInt trade_int) {
        std::string key = trade_int.k + "-" + trade_int.ex;
        last_timestamps[key] = trade_int.ts;
        last_prices[key] = trade_int.p;
    };
    ClientPublic* client_fut = sh.get_client(opp_row.fut_ex, "fut");
    client_fut->onmessage_trade = handle_trades;
    client_fut->subscribe_to_trades(opp_row.fut_symbol);
    ClientPublic* client_spot = sh.get_client(opp_row.spot_ex, "spot");
    client_spot->onmessage_trade = handle_trades;
    client_spot->subscribe_to_trades(opp_row.fut_symbol);
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        long now_millis =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string b1;
        for (auto o = last_timestamps.cbegin(); o != last_timestamps.cend();
             ++o) {
            b1 += o->first + ":" + std::to_string(now_millis - o->second) + ",";
        }
        if (!b1.empty()) {
            b1.pop_back();
        }
        std::string b2;
        for (auto o = last_prices.cbegin(); o != last_prices.cend(); ++o) {
            b2 += o->first + ":" + std::to_string(o->second) + ",";
        }
        if (!b2.empty()) {
            b2.pop_back();
        }
        spdlog::info("last_timestamps={} last_prices={}", "{" + b1 + "}",
                     "{" + b2 + "}");
        if (last_prices.size() == 2) {
            spdlog::info("opp_row={}", opp_row);
            break;
        }
    }
}

static int trade_obj_raw_f = 0;
static std::optional<SpreadsReq> spreads_req = {};

class TradeContangoServiceImpl final : public TradeContango::Service {
    Status FireTrade(ServerContext* context, const FireTradeReq* req,
                     FireTradeRes* reply) override {
        reply->set_run_initiated(trade_obj_raw_f == 0 ? 1 : 0);
        spdlog::debug("fire-trade req->list.size={}", req->list().size());
        if (trade_obj_raw_f == 0) {
            spreads_req = req->list()[0];
            for (auto& obj : req->list()) {
                if (obj.diff_rel() > spreads_req.value().diff_rel()) {
                    spdlog::debug("set spreads_req={}", format_as(obj));
                    spreads_req = obj;
                }
            }
            trade_obj_raw_f = 1;
        }
        return Status::OK;
    }
};

void listen_gateio_tickers_v2(int argc, char** argv) {
    SpreadsHouse sh;
    sh.init_idle();
    std::thread _1([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            sh.ping();
        }
    });
    std::thread _2([&]() {
        while (true) {
            while (trade_obj_raw_f == 0) {
                continue;
            }
            SpreadsReq obj = spreads_req.value();
            spdlog::info("obj={}", format_as(obj));
            auto client_fut = sh.get_client(obj.t_fut().ex(), "fut");
            std::string symbol_fut = obj.t_fut().s();
            client_fut->onmessage_depth = [&](const Depth depth) {};
            client_fut->subscribe_to_depth(symbol_fut);
            auto client_spot = sh.get_client(obj.t_spot().ex(), "spot");
            std::string symbol_spot = obj.t_spot().s();
            client_spot->onmessage_depth = [&](const Depth depth) {};
            client_spot->subscribe_to_depth(symbol_spot);
            spdlog::info("wait for order-books to be downloaded");
            while (true) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                bool is_ready_fut =
                    client_fut->order_book_cache.get_last_update_id() != 0;
                bool is_ready_spot =
                    client_spot->order_book_cache.get_last_update_id() != 0;
                spdlog::info("tick is_ready_spot={} is_ready_fut={}",
                             is_ready_spot, is_ready_fut);
                if (is_ready_fut && is_ready_spot) {
                    double bid_fut =
                        client_fut->order_book_cache.get_top_bid().get_d();
                    double bid_spot =
                        client_spot->order_book_cache.get_top_bid().get_d();
                    double diff_rel_2 = (bid_fut - bid_spot) / bid_spot * 100;
                    spdlog::info("bid_spot={} bid_fut={} diff_rel_2={}",
                                 bid_spot, bid_fut, diff_rel_2);
                    spdlog::info("p_spot={} p_fut={} diff_rel={}",
                                 obj.t_spot().p(), obj.t_fut().p(),
                                 obj.diff_rel());
                    spdlog::info("obj={}", format_as(obj));
                    break;
                }
            }
            spdlog::info("start waiting for price converge");
            while (true) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                double bid_fut =
                    client_fut->order_book_cache.get_top_bid().get_d();
                double bid_spot =
                    client_spot->order_book_cache.get_top_bid().get_d();
                double diff_rel = (bid_fut - bid_spot) / bid_spot * 100;
                spdlog::info("bid_spot={:.6f} bid_fut={:.6f} diff_rel={:.2f}",
                             bid_spot, bid_fut, diff_rel);
                if (diff_rel < 0.5) {
                    spdlog::info("price converged => close trades obj={}",
                                 format_as(obj));
                    break;
                }
            }
            // NOTE: order book cleaned up is checked here because sometimes
            // bybit sends events in the next order (snapshot, subscribe, ...)
            // NOTE: connections drop happens because of weird unsubscribe
            // mechanism implemented in exchanges
            spdlog::info("close connection and set-up them again");
            sh.close_all();
            spdlog::debug("all closed");
            sh.init_idle();
            spdlog::debug("all connected again");
            spreads_req = {};
            trade_obj_raw_f = 0;
        }
    });
    std::string server_address = absl::StrFormat("0.0.0.0:%d", 50051);
    TradeContangoServiceImpl service;
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    spdlog::info("Server listening on {}", server_address);
    server->Wait();
}
