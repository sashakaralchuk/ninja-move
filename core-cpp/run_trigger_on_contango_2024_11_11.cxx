#include <clickhouse/client.h>
// #include <fmt/core.h>
#include <gmpxx.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "grpcpp/grpcpp.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "src/clients.hpp"
// #include "src/models.hpp" // TODO: how to do such imports types together
#include <openssl/hmac.h>

#include <cstring>
#include <iomanip>
#include <iostream>

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
void debug_place_order();
void debug_listen_gateio_tickers();
void listen_gateio_tickers_v1();
void listen_gateio_tickers_v2(int argc, char** argv);

int main(int argc, char** argv) {
    configure_logger();
    std::string v = std::getenv("TICKERS_VERSION");
    if (v == "debug-order") {
        debug_place_order();
    } else if (v == "debug-depth") {
        debug_listen_gateio_tickers();
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
    auto level = spdlog::level::from_str(std::getenv("SPDLOG_LEVEL"));
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(
        std::make_shared<spdlog::sinks::ansicolor_stdout_sink_st>());
    sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_st>(
        ".var/logfile", 0, 0));
    for (auto& s : sinks) {
        s->set_level(level);
        s->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [%s %! %#] %v");
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
                price / COALESCE(toFloat64OrNull(regexpExtract(symbol, '10*',
                0)), 1) price_int_1
            FROM default.trade_contango_arbitrage_v1
            FINAL
            WHERE timestamp >= (now() - toIntervalSecond(60))
                AND length(replaceRegexpOne(symbol,
                '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) =
                length(symbol) AND volume > 0 AND status = 'TRADING' AND
                symbol_int_1 != 'DEFIUSDT'
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
            o->second->closesocket();
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

void debug_place_listen_mexc_fut_v2() {
    throw std::runtime_error("there is not opportunity for fut on mexc");
}

void debug_place_listen_mexc_spot_v2() {
    ClientPrivateMexc client = ClientPrivateMexc("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    while (true) {
        if (client.ws_onopen_received) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    while (true) {
        if (client.is_subscribed_to_private_channels) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order("DNXUSDT", "BUY", "0.35", "60.0");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 0) {
            spdlog::info("buy-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("buy-order is filled => finish");
            break;
        }
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order(
        "DNXUSDT", "SELL", "0.25",
        std::to_string(client.get_last_order().filled_amount));
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 1) {
            spdlog::info("sell-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("sell-order is filled => finish");
            break;
        }
    }
    client.clear_orders();
}

void debug_place_listen_gateio_fut_v2() {
    ClientPrivateGateio client = ClientPrivateGateio("fut");
    client.init_idle();
    spdlog::info("wait for onopen event");
    while (true) {
        if (client.ws_onopen_received) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    while (true) {
        if (client.is_subscribed_to_private_channels) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_fut_limit_order("ETH_USDT", "SELL", "3800", "1");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 0) {
            spdlog::info("buy-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("buy-order is filled => finish");
            break;
        }
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_fut_limit_order("ETH_USDT", "BUY", "4000", "1");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 1) {
            spdlog::info("sell-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("sell-order is filled => finish");
            break;
        }
    }
    client.clear_orders();
}

void debug_place_listen_gateio_spot_v2() {
    ClientPrivateGateio client = ClientPrivateGateio("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    while (true) {
        if (client.ws_onopen_received) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    while (true) {
        if (client.is_subscribed_to_private_channels) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order("ETH_USDT", "buy", "4050", "0.01");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 0) {
            spdlog::info("buy-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("buy-order is filled => finish");
            break;
        }
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order(
        "ETH_USDT", "sell", "3950",
        std::to_string(client.get_last_order().filled_amount));
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 1) {
            spdlog::info("sell-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("sell-order is filled => finish");
            break;
        }
    }
    client.clear_orders();
}

void debug_place_listen_bybit_fut_v2() {
    ClientPrivateBybit client = ClientPrivateBybit("fut");
    client.init_idle();
    spdlog::info("wait for onopen event");
    while (true) {
        if (client.ws_onopen_received) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    while (true) {
        if (client.is_subscribed_to_private_channels) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_fut_limit_order("ETHUSDT", "Sell", "3800", "0.01");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 0) {
            spdlog::info("buy-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("buy-order is filled => finish");
            break;
        }
    }
    for (int i = 0; i < 25; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_fut_limit_order("ETHUSDT", "Buy", "4000", "0.01");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 1) {
            spdlog::info("sell-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("sell-order is filled => finish");
            break;
        }
    }
    client.clear_orders();
}

void debug_place_listen_bybit_spot_v2() {
    ClientPrivateBybit client = ClientPrivateBybit("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    while (true) {
        if (client.ws_onopen_received) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    while (true) {
        if (client.is_subscribed_to_private_channels) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order("ETHUSDT", "Buy", "4050", "0.01");
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 0) {
            spdlog::info("buy-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("buy-order is filled => finish");
            break;
        }
    }
    for (int i = 0; i < 5; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    client.place_spot_limit_order(
        "ETHUSDT", "Sell", "3950",
        std::to_string(client.get_last_order().filled_amount));
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_orders_len() > 1) {
            spdlog::info("sell-order appeared");
            break;
        }
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (client.get_last_order().isFilled()) {
            spdlog::info("sell-order is filled => finish");
            break;
        }
    }
    client.clear_orders();
}

void debug_place_order() {
    // TODO: keep in PrivateClient just one order and one position without
    // vector
    // TODO: how to handle situation when you already sent order amend message
    // and than order fills
    // TODO: what's the features? what does it mean when you are buying short
    // and buying long
    // TODO: commit + do this exchange with some bullshit token (not with eth)
    // TODO: write code to ahndle whole strategy
    // TODO: think on do the same with on-chain exchanges + join "миша флипает"
    // TODO: think how to abuse mms algorithms
    // private chat
    // debug_place_listen_mexc_fut_v2();
    // debug_place_listen_mexc_spot_v2();
    // debug_place_listen_gateio_fut_v2();
    debug_place_listen_gateio_spot_v2();
    // debug_place_listen_bybit_fut_v2();
    // debug_place_listen_bybit_spot_v2();
}

void debug_trade_contango_private_client_interface() {
    // ...
    // private_clients.init_idle();
    // ...
    while (true) {
        // ...
        while (true) {
            // ...
            // if (is_ready_fut && is_ready_spot) {
            //     break;
            // }
        }
        spdlog::info("place orders and wait for fills");
        // private_clients.place_fut_order();
        // private_clients.place_spot_order();
        while (true) {
            // if (!fut_order.is_filled && fut_order.price >
            // client_fut.last_bid_price) {
            //     private_clients.amend_fut_order();
            // }
            // if (!spot_order.is_filled && spot_order.price >
            // client_spot.last_bid_price) {
            //     private_clients.amend_spot_order();
            // }
            // if (fut_order.is_filled && spot_order.is_filled) {
            //     break;
            // }
        }
        spdlog::info("orders filled => wait for prices converge");
        while (true) {
            // double diff_rel = (bid_fut - bid_spot) / bid_spot * 100;
            // if (diff_rel < 0.1) {
            //     break;
            // }
        }
        spdlog::info("price converged => place exit orders and wait for fills");
        // private_clients.place_fut_order();
        // private_clients.place_spot_order();
        while (true) {
            // if (!fut_order.is_filled && fut_order.price >
            // client_fut.last_bid_price) {
            //     private_clients.amend_fut_order();
            // }
            // if (!spot_order.is_filled && spot_order.price >
            // client_spot.last_bid_price) {
            //     private_clients.amend_spot_order();
            // }
            // if (fut_order.is_filled && spot_order.is_filled) {
            //     break;
            // }
        }
        spdlog::info(
            "exit orders closed => start looking for another opportunity");
        // client_fut->clear_orders();
        // client_spot->clear_orders();
    }
}

void debug_listen_gateio_tickers() {
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
            if (client_fut->order_book_cache.get_last_update_id() != 0) {
                throw std::runtime_error(
                    "fut order-book is not empty on start");
            }
            auto client_spot = sh.get_client(obj.t_spot().ex(), "spot");
            std::string symbol_spot = obj.t_spot().s();
            client_spot->onmessage_depth = [&](const Depth depth) {};
            client_spot->subscribe_to_depth(symbol_spot);
            if (client_spot->order_book_cache.get_last_update_id() != 0) {
                throw std::runtime_error(
                    "spot order-book is not empty on start");
            }
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
