#include <WebSocketClient.h>
#include <clickhouse/client.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <nlohmann/json.hpp>

void listen_gateio_tickers();

int main() {
    spdlog::cfg::load_env_levels();
    listen_gateio_tickers();
    return 0;
}

struct TradeInt {
    std::string ex;
    std::string s;
    std::string k;
    long ts;  // TODO: add check for milliseconds
    double p;
    double v;
};

std::ostream& operator<<(std::ostream& os, TradeInt const& o) {
    os << "{ex=" << o.ex << ",s=" << o.s << ",k=" << o.k << ",ts=" << o.ts
       << ",p=" << o.p << ",v=" << o.v << "}";
    return os;
}

class WSClientGateio : public hv::WebSocketClient {
   public:
    WSClientGateio(hv::EventLoopPtr loop = NULL) : WebSocketClient(loop) {}
    ~WSClientGateio() {}

    void init_spot_idle() {
        onopen = []() { spdlog::info("gateio onopen"); };
        onclose = []() { spdlog::info("gateio onclose"); };
        setPingInterval(10000);
        reconn_setting_t reconn;
        reconn_setting_init(&reconn);
        reconn.min_delay = 100;
        reconn.max_delay = 1000;
        reconn.delay_policy = 2;
        setReconnect(&reconn);
        http_headers headers;
        open("wss://api.gateio.ws:443/ws/v4/", headers);
    }

    void subscribe_to_spot_trades(std::string& symbol) {
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
    }
};

class WSClientMexc : public hv::WebSocketClient {
   public:
    std::function<void(const TradeInt trade_int)> onmessage_trade;

    WSClientMexc(hv::EventLoopPtr loop = NULL) : WebSocketClient(loop) {}
    ~WSClientMexc() {}

    void init_fut_idle() {
        std::string url = "wss://contract.mexc.com:443/edge";
        std::string kind = "fut";
        this->init_idle(url, kind);
    }

    void subscribe_to_fut_trades(std::string& symbol) {
        std::string t_template = R"({
            "method": "sub.deal",
            "param": {"symbol": "%s"}
        })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    }

    void init_spot_idle() {
        std::string url = "wss://wbs.mexc.com/ws";
        std::string kind = "spot";
        this->init_idle(url, kind);
    }

    void subscribe_to_spot_trades(std::string& symbol) {
        std::string t_template = R"({
            "method": "SUBSCRIPTION",
            "params": ["spot@public.deals.v3.api@%s"]
        })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    }

    void ping() { send(R"({"method": "ping"})"); }

   private:
    void init_idle(std::string& url, std::string& kind) {
        onopen = []() { spdlog::info("mexc onopen"); };
        onclose = []() { spdlog::info("mexc onclose"); };
        onmessage = [=](const std::string& msg) {
            nlohmann::json msg_obj = nlohmann::json::parse(msg);
            if (kind == "fut") {
                std::string channel = msg_obj["channel"];
                if (channel == "rs.sub.deal" && msg_obj["data"] == "success") {
                    spdlog::info("mexc fut subscribed successfully");
                } else if (channel == "pong") {
                    spdlog::debug("handle pong");
                } else if (channel == "push.deal") {
                    TradeInt trade = TradeInt{
                        .ex = "mexc",
                        .s = msg_obj["symbol"],
                        .k = kind,
                        .ts = msg_obj["ts"],
                        .p = msg_obj["data"]["p"],
                        .v = msg_obj["data"]["v"],
                    };
                    onmessage_trade(trade);
                } else {
                    throw std::runtime_error("unexpected channel=" + channel);
                }
            } else if (kind == "spot") {
                if (msg_obj.contains("id") && msg_obj["id"] == 0 &&
                    msg_obj.contains("code") && msg_obj["code"] == 0) {
                    spdlog::info("mexc spot subscribed successfully");
                } else if (msg_obj.contains("c") &&
                           ((std::string)msg_obj["c"])
                                   .rfind("spot@public.deals.v3.api@", 0) ==
                               0) {
                    std::cout << "handle spot" << msg_obj << std::endl;
                    for (auto& deal_raw : msg_obj["d"]["deals"]) {
                        TradeInt trade = TradeInt{
                            .ex = "mexc",
                            .s = msg_obj["s"],
                            .k = kind,
                            .ts = deal_raw["t"],
                            .p = std::stod((std::string)deal_raw["p"]),
                            .v = std::stod((std::string)deal_raw["v"]),
                        };
                        onmessage_trade(trade);
                    }
                } else {
                    throw std::runtime_error("unexpected msg=" + msg);
                }
            } else {
                throw std::runtime_error("unexpected kind=" + kind);
            }
        };
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

class WSClientBybit : public hv::WebSocketClient {
   public:
    std::function<void(const TradeInt trade_int)> onmessage_trade;

    WSClientBybit(hv::EventLoopPtr loop = NULL) : WebSocketClient(loop) {}
    ~WSClientBybit() {}

    void init_fut_idle() {
        std::string url = "wss://stream.bybit.com/v5/public/linear";
        std::string kind = "fut";
        this->init_idle(url, kind);
    }

    void subscribe_to_fut_trades(std::string& symbol) {
        std::string t_template = R"({
            "req_id": "t",
            "op": "subscribe",
            "args": ["publicTrade.%s"]
        })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    }

    void init_spot_idle() {
        std::string url = "wss://stream.bybit.com/v5/public/spot";
        std::string kind = "spot";
        this->init_idle(url, kind);
    }

    void subscribe_to_spot_trades(std::string& symbol) {
        std::string t_template = R"({
            "req_id": "t",
            "op": "subscribe",
            "args": ["publicTrade.%s"]
        })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    }

   private:
    void init_idle(std::string& url, std::string& kind) {
        onopen = []() { spdlog::info("bybit onopen"); };
        onclose = []() { spdlog::info("bybit onclose"); };
        onmessage = [=](const std::string& msg) {
            nlohmann::json msg_obj = nlohmann::json::parse(msg);
            if (msg_obj.contains("op") && msg_obj["op"] == "subscribe") {
                if ((bool)msg_obj["success"]) {
                    spdlog::info("bybit subscribed successfully");
                    return;
                } else {
                    throw std::runtime_error("bybit subscription failed");
                }
            }
            std::string msg_type = msg_obj["type"];
            if (msg_type != "snapshot") {
                throw std::runtime_error("bybit fut unknown type=" + msg_type);
            }
            if (kind == "fut") {
                for (auto& trade_raw : msg_obj["data"]) {
                    TradeInt trade = TradeInt{
                        .ex = "bybit",
                        .s = trade_raw["s"],
                        .k = "fut",
                        .ts = trade_raw["T"],
                        .p = std::stod((std::string)trade_raw["p"]),
                        .v = std::stod((std::string)trade_raw["v"]),
                    };
                    onmessage_trade(trade);
                }
            } else if (kind == "spot") {
                for (auto& trade_raw : msg_obj["data"]) {
                    TradeInt trade = TradeInt{
                        .ex = "bybit",
                        .s = trade_raw["s"],
                        .k = "spot",
                        .ts = trade_raw["T"],
                        .p = std::stod((std::string)trade_raw["p"]),
                        .v = std::stod((std::string)trade_raw["v"]),
                    };
                    onmessage_trade(trade);
                }
            } else {
                throw std::runtime_error("unexpected kind=" + kind);
            }
        };
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

struct OpportunityRow {
    std::string symbol_int_1;
    double fut_price;
    double spot_price;
    std::string fut_ex;
    std::string spot_ex;
    std::string fut_symbol;
    std::string spot_symbol;
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
        };
    }
};

std::ostream& operator<<(std::ostream& os, OpportunityRow const& o) {
    os << "{symbol_int_1=" << o.symbol_int_1 << ",fut_price=" << o.fut_price
       << ",spot_price=" << o.spot_price << ",fut_ex=" << o.fut_ex
       << ",spot_ex=" << o.spot_ex << ",fut_symbol=" << o.fut_symbol
       << ",spot_symbol=" << o.spot_symbol << "}";
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
            AND spot_ex = 'gateio'
            AND fut_ex = 'mexc'
        ORDER BY diff_rel DESC
    )";
    std::optional<OpportunityRow> opp_row_t = {};
    client.Select(
        replace_first(QUERY_OPPORTUNITIES, "%(threshold_rel)s", "3.0"),
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

void listen_gateio_tickers() {
    std::map<std::string, long> last_timestamps;
    std::map<std::string, double> last_prices;
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    WSClientGateio ws_gateio;
    ws_gateio.init_spot_idle();
    ws_gateio.onmessage = [&](const std::string& msg) {
        nlohmann::json msg_obj = nlohmann::json::parse(msg);
        std::string event = msg_obj["event"];
        if (event == "subscribe") {
            return;
        }
        if (event == "update") {
            last_timestamps["gateio"] = msg_obj["time_ms"];
            last_prices["gateio"] =
                std::stod((std::string)msg_obj["result"]["price"]);
            return;
        }
        throw std::runtime_error("gateio unexpected event=" + event);
    };
    WSClientMexc ws_mexc;
    ws_mexc.init_fut_idle();
    ws_mexc.onmessage = [&](const std::string& msg) {
        nlohmann::json msg_obj = nlohmann::json::parse(msg);
        std::string channel = msg_obj["channel"];
        if (channel == "rs.sub.deal" || channel == "pong") {
            return;
        }
        if (channel == "push.deal") {
            last_timestamps["mexc"] = msg_obj["ts"];
            last_prices["mexc"] = msg_obj["data"]["p"];
            return;
        }
        if (channel == "rs.error") {
            spdlog::error("mexc error: {}", msg);
        }
        throw std::runtime_error("mexc unexpected channel=" + channel);
    };
    for (int i = 0; i < 100; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (ws_gateio.isConnected() && ws_mexc.isConnected()) {
            spdlog::info("connections set up");
            break;
        }
    }
    if (!ws_gateio.isConnected() || !ws_mexc.isConnected()) {
        throw std::runtime_error("connections haven't been set up");
    }
    std::thread _([&ws_mexc]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            ws_mexc.ping();
        }
    });
    OpportunityRow opp_row;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        std::optional<OpportunityRow> opp_row_opt =
            is_opportunity_exists(clickhouse_client);
        if (opp_row_opt.has_value()) {
            opp_row = opp_row_opt.value();
            spdlog::info("found opp for {}", opp_row.symbol_int_1);
            break;
        } else {
            spdlog::info("there is no opp");
        }
    }
    ws_gateio.subscribe_to_spot_trades(opp_row.spot_symbol);
    ws_mexc.subscribe_to_fut_trades(opp_row.fut_symbol);
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
        // TODO: apply this to code here (parsing functions will be needed)
        std::cout << last_prices.size() << std::endl;
        spdlog::info("last_timestamps={} last_prices={} opp_row=({}, {})",
                     "{" + b1 + "}", "{" + b2 + "}", opp_row.spot_price,
                     opp_row.fut_price);
        if (last_prices.size() == 2) {
            break;
        }
    }
}
