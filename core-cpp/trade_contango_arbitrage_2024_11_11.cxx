#include <clickhouse/client.h>
#include <gmpxx.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>

#include <backward.hpp>
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

#include "rdkafkacpp.h"
#include "trade_contango.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using trade_contango::FireTradeReq;
using trade_contango::FireTradeReqV2;
using trade_contango::FireTradeRes;
using trade_contango::QTickerReq;
using trade_contango::QTickerReqV2;
using trade_contango::SpreadsReq;
using trade_contango::SpreadsReqV2;
using trade_contango::TradeContango;

#define GET_FN_NAME_TO_FN(fn) {#fn, fn}

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");
backward::SignalHandling sh{};

void configure_logger();
void debug_place_listen_mexc_fut_v2();
void debug_place_listen_mexc_spot_v2();
void debug_place_listen_gateio_fut_v2();
void debug_place_listen_gateio_spot_v2();
void debug_place_listen_bybit_fut_v2();
void debug_place_listen_bybit_spot_v2();
void debug_listen_gateio_tickers();
void execute_v1();
void execute_v2();
void execute_v3();
void debug_fetch_tickers();
void debug_place_fetch_order_gateio_fut();
void debug_place_fetch_order_gateio_spot();
void debug_place_fetch_order_mexc_spot();
void debug_place_fetch_order_bybit_fut();
void debug_place_fetch_order_bybit_spot();
void debug_print_balances(std::ostream& stream = std::cout);
void execute_v4();
void debug_init_obj();
void write_spreads_to_topic();
void wait_for_spread_converge_save_order_book();
void wait_for_spread_converge_show_order_book();
void debug_amend_order_gateio_fut();
void debug_amend_order_bybit_fut();
void debug_buy_one_side_through_limit_order();

std::map<std::string, void (*)()> FNS_MAP{
    GET_FN_NAME_TO_FN(debug_place_listen_mexc_fut_v2),
    GET_FN_NAME_TO_FN(debug_place_listen_mexc_spot_v2),
    GET_FN_NAME_TO_FN(debug_place_listen_gateio_fut_v2),
    GET_FN_NAME_TO_FN(debug_place_listen_gateio_spot_v2),
    GET_FN_NAME_TO_FN(debug_place_listen_bybit_fut_v2),
    GET_FN_NAME_TO_FN(debug_place_listen_bybit_spot_v2),
    GET_FN_NAME_TO_FN(debug_listen_gateio_tickers),
    GET_FN_NAME_TO_FN(execute_v1),
    GET_FN_NAME_TO_FN(execute_v2),
    GET_FN_NAME_TO_FN(execute_v3),
    GET_FN_NAME_TO_FN(debug_fetch_tickers),
    GET_FN_NAME_TO_FN(debug_place_fetch_order_gateio_fut),
    GET_FN_NAME_TO_FN(debug_place_fetch_order_gateio_spot),
    GET_FN_NAME_TO_FN(debug_place_fetch_order_mexc_spot),
    GET_FN_NAME_TO_FN(debug_place_fetch_order_bybit_fut),
    GET_FN_NAME_TO_FN(debug_place_fetch_order_bybit_spot),
    {"debug_print_balances", []() { debug_print_balances(); }},
    GET_FN_NAME_TO_FN(execute_v4),
    GET_FN_NAME_TO_FN(debug_init_obj),
    GET_FN_NAME_TO_FN(write_spreads_to_topic),
    GET_FN_NAME_TO_FN(wait_for_spread_converge_save_order_book),
    GET_FN_NAME_TO_FN(wait_for_spread_converge_show_order_book),
    GET_FN_NAME_TO_FN(debug_amend_order_gateio_fut),
    GET_FN_NAME_TO_FN(debug_amend_order_bybit_fut),
    GET_FN_NAME_TO_FN(debug_buy_one_side_through_limit_order),
};

int main(int argc, char** argv) {
    configure_logger();
    std::string v = std::getenv("TICKERS_VERSION");
    auto fn = FNS_MAP.find(v);
    if (fn == FNS_MAP.end()) {
        throw std::runtime_error(
            fmt::format("unexpected TICKERS_VERSION={}", v));
    }
    fn->second();
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
    os << "u=" << d.u << ", exchange_ts_millis=" << d.ex_ts_millis
       << ", asks=[";
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

std::ostream& operator<<(std::ostream& os, QTickerReqV2 const& o) {
    os << "QTickerReqV2{ex=" << o.ex() << ",s=" << o.s() << ",st=" << o.st()
       << ",k=" << o.k() << ",ts=" << o.ts() << ",p_bid=" << o.p_bid()
       << ",p_ask=" << o.p_ask() << ",v=" << o.v() << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, SpreadsReqV2 const& o) {
    os << "SpreadsReqV2{p_ask_spot=" << o.p_ask_spot()
       << ",p_bid_fut=" << o.p_bid_fut() << ",diff_rel=" << o.diff_rel()
       << ",t_spot=" << o.t_spot() << ",t_fut=" << o.t_fut() << "}";
    return os;
}

std::string format_as(SpreadsReqV2 const& o) {
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
        ".var/logs/logfile", 0, 0));
    for (auto& s : sinks) {
        s->set_level(level);
        s->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [t=%t] [%l] [%s %! %#] %v");
    }
    auto l = std::make_shared<spdlog::logger>("t", begin(sinks), end(sinks));
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

void wait_until(std::function<bool()> callback, int interval_millis = 250) {
    while (true) {
        if (callback()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_millis));
    }
}

void wait_idle(int iters_amount = 5) {
    for (int i = 0; i < iters_amount; i++) {
        spdlog::info("idle tick");
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
}

class ClientsHouse {
   public:
    ClientsHouse() {}

    void init_idle_public() {
        if (ws_clients_public.size() == 0) {
            init_public_clients();
        }
        for (auto o = ws_clients_public.cbegin(); o != ws_clients_public.cend();
             ++o) {
            o->second->init_idle();
        }
        auto is_all_ws_connected = [&]() -> bool {
            for (auto o = ws_clients_public.cbegin();
                 o != ws_clients_public.cend(); ++o) {
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

    void init_public_clients() {
        ws_clients_public["bybit-fut"] = new ClientPublicBybit("fut");
        ws_clients_public["bybit-spot"] = new ClientPublicBybit("spot");
        ws_clients_public["mexc-fut"] = new ClientPublicMexc("fut");
        ws_clients_public["mexc-spot"] = new ClientPublicMexc("spot");
        ws_clients_public["gateio-fut"] = new ClientPublicGateio("fut");
        ws_clients_public["gateio-spot"] = new ClientPublicGateio("spot");
        ws_clients_public["htx-fut"] = new ClientPublicHtx("fut");
        ws_clients_public["htx-spot"] = new ClientPublicHtx("spot");
    }

    void init_idle_private() {
        if (ws_clients_private.size() == 0) {
            init_private_clients();
        }
        auto m = ws_clients_private;
        for (auto o = m.cbegin(); o != m.cend(); ++o) {
            o->second->init_idle();
        }
        SPDLOG_INFO("wait for onopen event");
        wait_until([&]() {
            for (auto o = m.cbegin(); o != m.cend(); ++o) {
                if (!o->second->isConnected() ||
                    !o->second->ws_onopen_received) {
                    return false;
                }
            }
            return true;
        });
        for (auto o = m.cbegin(); o != m.cend(); ++o) {
            o->second->subscribe_to_private_events();
        }
        SPDLOG_INFO("wait for subscribed events");
        wait_until([&]() {
            for (auto o = m.cbegin(); o != m.cend(); ++o) {
                if (!o->second->is_subscribed_to_private_channels) {
                    return false;
                }
            }
            return true;
        });
        SPDLOG_INFO("ws_clients_private been set up");
    }

    void init_private_clients() {
        ws_clients_private["mexc-spot"] = new ClientPrivateMexc("spot");
        ws_clients_private["gateio-fut"] = new ClientPrivateGateio("fut");
        ws_clients_private["gateio-spot"] = new ClientPrivateGateio("spot");
        ws_clients_private["bybit-fut"] = new ClientPrivateBybit("fut");
        ws_clients_private["bybit-spot"] = new ClientPrivateBybit("spot");
        SPDLOG_INFO("ws_clients_private been created => init exchanges infos");
        for (auto o = ws_clients_private.cbegin();
             o != ws_clients_private.cend(); ++o) {
            o->second->init_exchange_info();
        }
    }

    void ping() {
        for (auto o = ws_clients_public.cbegin(); o != ws_clients_public.cend();
             ++o) {
            o->second->ping();
        }
    }

    void close_all() {
        for (auto o = ws_clients_public.cbegin(); o != ws_clients_public.cend();
             ++o) {
            o->second->closesocket();
        }
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            spdlog::debug("tick on if all ws clients are closed");
            bool all_closed = true;
            for (auto o = ws_clients_public.cbegin();
                 o != ws_clients_public.cend(); ++o) {
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
        for (auto o = ws_clients_public.cbegin(); o != ws_clients_public.cend();
             ++o) {
            o->second->order_book_cache.clear();
        }
    }

    ClientPublic* get_client_public(std::string ex, std::string kind) {
        std::string k = ex + "-" + kind;
        if (ws_clients_public.find(k) == ws_clients_public.end()) {
            throw std::runtime_error(fmt::format("unexpected key={}", k));
        }
        return ws_clients_public[k];
    }

    ClientPrivate* get_client_private(std::string ex, std::string kind) {
        std::string k = ex + "-" + kind;
        if (ws_clients_private.find(k) == ws_clients_private.end()) {
            throw std::runtime_error(fmt::format("unexpected key={}", k));
        }
        return ws_clients_private[k];
    }

   private:
    std::map<std::string, ClientPublic*> ws_clients_public;
    std::map<std::string, ClientPrivate*> ws_clients_private;
};

void debug_place_listen_mexc_fut_v2() {
    throw std::runtime_error("there is not opportunity for fut on mexc");
}

void debug_place_listen_mexc_spot_v2() {
    double usdt_to_use = 20.0;
    std::string symbol = "DNXUSDT";
    double price = 0.2859;
    ClientPrivateMexc client = ClientPrivateMexc("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    wait_until([&]() { return client.ws_onopen_received; });
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    wait_until([&]() { return client.is_subscribed_to_private_channels; });
    wait_idle();
    auto [buy_price, buy_quantity] =
        client.adjust_price_quantity(symbol, price * 1.1, usdt_to_use / price);
    client.place_spot_limit_order(symbol, "buy", buy_price, buy_quantity);
    wait_until([&]() { return client.get_orders_len() > 0; });
    spdlog::info("buy-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("buy-order is filled => finish");
    wait_idle();
    auto [sell_price, sell_quantity] = client.adjust_price_quantity(
        symbol, price * 0.9, client.get_last_order().filled_amount);
    client.place_spot_limit_order(symbol, "sell", sell_price, sell_quantity);
    wait_until([&]() { return client.get_orders_len() > 1; });
    spdlog::info("sell-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("sell-order is filled => finish");
    client.clear_orders();
}

void debug_place_listen_gateio_fut_v2() {
    double usdt_to_use = 20.0;
    std::string symbol = "FLT_USDT";
    double price = 0.4126;
    ClientPrivateGateio client = ClientPrivateGateio("fut");
    client.init_idle();
    spdlog::info("wait for onopen event");
    wait_until([&]() { return client.ws_onopen_received; });
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    wait_until([&]() { return client.is_subscribed_to_private_channels; });
    wait_idle();
    auto [sell_price, sell_quantity] =
        client.adjust_price_quantity(symbol, price * 0.9, usdt_to_use / price);
    client.place_fut_limit_order(symbol, "sell", sell_price, sell_quantity);
    wait_until([&]() { return client.get_orders_len() > 0; });
    spdlog::info("buy-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("buy-order is filled => finish");
    client.set_leverage_to_1(symbol);
    spdlog::info("leverage been set to 1");
    wait_idle();
    auto [buy_price, buy_quantity] = client.adjust_price_quantity(
        symbol, price * 1.1, client.get_last_order().filled_amount);
    client.place_fut_limit_order(symbol, "buy", buy_price, buy_quantity);
    wait_until([&]() { return client.get_orders_len() > 1; });
    spdlog::info("sell-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("sell-order is filled => finish");
    client.clear_orders();
}

void debug_place_listen_gateio_spot_v2() {
    double usdt_to_use = 20.0;
    std::string symbol = "FLT_USDT";
    double price = 0.4063;
    ClientPrivateGateio client = ClientPrivateGateio("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    wait_until([&]() { return client.ws_onopen_received; });
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    wait_until([&]() { return client.is_subscribed_to_private_channels; });
    wait_idle();
    auto [buy_price, buy_quantity] =
        client.adjust_price_quantity(symbol, price * 1.1, usdt_to_use / price);
    client.place_spot_limit_order(symbol, "buy", buy_price, buy_quantity);
    wait_until([&]() { return client.get_orders_len() > 0; });
    spdlog::info("buy-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("buy-order is filled => finish");
    wait_idle();
    auto [sell_price, sell_quantity] = client.adjust_price_quantity(
        symbol, price * 0.9, client.get_last_order().filled_amount);
    client.place_spot_limit_order(symbol, "sell", sell_price, sell_quantity);
    wait_until([&]() { return client.get_orders_len() > 1; });
    spdlog::info("sell-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("sell-order is filled => finish");
    client.clear_orders();
}

void debug_place_listen_bybit_fut_v2() {
    double usdt_to_use = 20.0;
    std::string symbol = "BILLYUSDT";
    double price = 0.02876;
    ClientPrivateBybit client = ClientPrivateBybit("fut");
    client.init_idle();
    spdlog::info("wait for onopen event");
    wait_until([&]() { return client.ws_onopen_received; });
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    wait_until([&]() { return client.is_subscribed_to_private_channels; });
    wait_idle();
    auto [sell_price, sell_quantity] =
        client.adjust_price_quantity(symbol, price * 0.96, usdt_to_use / price);
    client.place_fut_limit_order(symbol, "sell", sell_price, sell_quantity);
    wait_until([&]() { return client.get_orders_len() > 0; });
    spdlog::info("buy-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("buy-order is filled => finish");
    client.set_leverage_to_1(symbol);
    spdlog::info("leverage been set to 1");
    wait_idle(25);
    auto [buy_price, buy_quantity] = client.adjust_price_quantity(
        symbol, price * 1.04, client.get_last_order().filled_amount);
    client.place_fut_limit_order(symbol, "buy", buy_price, buy_quantity);
    wait_until([&]() { return client.get_orders_len() > 1; });
    spdlog::info("sell-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("sell-order is filled => finish");
    client.clear_orders();
}

void debug_place_listen_bybit_spot_v2() {
    double usdt_to_use = 20.0;
    std::string symbol = "FLTUSDT";
    double price = 0.3977;
    ClientPrivateBybit client = ClientPrivateBybit("spot");
    client.init_idle();
    spdlog::info("wait for onopen event");
    wait_until([&]() { return client.ws_onopen_received; });
    client.subscribe_to_private_events();
    spdlog::info("wait for subscribed event");
    wait_until([&]() { return client.is_subscribed_to_private_channels; });
    wait_idle();
    auto [buy_price, buy_quantity] =
        client.adjust_price_quantity(symbol, price * 1.04, usdt_to_use / price);
    client.place_spot_limit_order(symbol, "buy", buy_price, buy_quantity);
    wait_until([&]() { return client.get_orders_len() > 0; });
    spdlog::info("buy-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("buy-order is filled => finish");
    wait_idle();
    auto [sell_price, sell_quantity] = client.adjust_price_quantity(
        symbol, price * 0.96, client.get_last_order().filled_amount);
    client.place_spot_limit_order(symbol, "sell", sell_price, sell_quantity);
    wait_until([&]() { return client.get_orders_len() > 1; });
    spdlog::info("sell-order appeared");
    wait_until([&]() { return client.get_last_order().isFilled(); });
    spdlog::info("sell-order is filled => finish");
    client.clear_orders();
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

void execute_v1() {
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::map<std::string, long> last_timestamps;
    std::map<std::string, double> last_prices;
    ClientsHouse ch;
    ch.init_idle_public();
    std::thread _([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            ch.ping();
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
    ClientPublic* client_fut = ch.get_client_public(opp_row.fut_ex, "fut");
    client_fut->onmessage_trade = handle_trades;
    client_fut->subscribe_to_trades(opp_row.fut_symbol);
    ClientPublic* client_spot = ch.get_client_public(opp_row.spot_ex, "spot");
    client_spot->onmessage_trade = handle_trades;
    client_spot->subscribe_to_trades(opp_row.fut_symbol);
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        long now_millis_ = now_millis();
        std::string b1;
        for (auto o = last_timestamps.cbegin(); o != last_timestamps.cend();
             ++o) {
            b1 +=
                o->first + ":" + std::to_string(now_millis_ - o->second) + ",";
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
static std::optional<SpreadsReqV2> spreads_req_v2 = {};

class TradeContangoServiceImpl final : public TradeContango::Service {
    std::function<void(const std::vector<SpreadsReqV2> req)> on_fire_trade_v2;

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

    Status FireTradeV2(ServerContext* context, const FireTradeReqV2* req,
                       FireTradeRes* reply) override {
        SPDLOG_DEBUG("fire-trade-v2 req->list.size={}", req->list().size());
        reply->set_run_initiated(trade_obj_raw_f == 0 ? 1 : 0);
        std::vector<SpreadsReqV2> reqs;
        for (auto& obj : req->list()) {
            reqs.push_back(obj);
        }
        if (trade_obj_raw_f == 0) {
            spreads_req_v2 = reqs[0];
            for (auto& obj : reqs) {
                if (obj.diff_rel() > spreads_req_v2.value().diff_rel()) {
                    spdlog::debug("set spreads_req_v2={}", format_as(obj));
                    spreads_req_v2 = obj;
                }
            }
            trade_obj_raw_f = 1;
        }
        on_fire_trade_v2(reqs);
        return Status::OK;
    }

   public:
    TradeContangoServiceImpl(
        std::function<void(const std::vector<SpreadsReqV2> req)>
            on_fire_trade_v2_) {
        on_fire_trade_v2 = on_fire_trade_v2_;
    }
};

void run_listening_for_events_sync(
    std::function<void(const std::vector<SpreadsReqV2> reqs)>
        on_fire_trade_v2_ = [](std::vector<SpreadsReqV2> reqs) {}) {
    std::string server_address = absl::StrFormat("0.0.0.0:%d", 50051);
    TradeContangoServiceImpl service(on_fire_trade_v2_);
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    spdlog::info("Server listening on {}", server_address);
    server->Wait();
}

void execute_v2() {
    ClientsHouse ch;
    ch.init_idle_public();
    std::thread _1([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            ch.ping();
        }
    });
    std::thread _2([&]() {
        while (true) {
            while (trade_obj_raw_f == 0) {
                continue;
            }
            SpreadsReq obj = spreads_req.value();
            spdlog::info("obj={}", format_as(obj));
            auto client_fut = ch.get_client_public(obj.t_fut().ex(), "fut");
            std::string symbol_fut = obj.t_fut().s();
            client_fut->onmessage_depth = [&](const Depth depth) {};
            client_fut->subscribe_to_depth(symbol_fut);
            if (client_fut->order_book_cache.get_last_update_id() != 0) {
                throw std::runtime_error(
                    "fut order-book is not empty on start");
            }
            auto client_spot = ch.get_client_public(obj.t_spot().ex(), "spot");
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
            ch.close_all();
            spdlog::debug("all closed");
            ch.init_idle_public();
            spdlog::debug("all connected again");
            spreads_req = {};
            trade_obj_raw_f = 0;
        }
    });
    run_listening_for_events_sync();
}

void execute_v3() {
    ClientsHouse ch;
    ch.init_idle_public();
    ch.init_idle_private();
    std::thread _1([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            ch.ping();
        }
    });
    std::thread _2([&]() {
        while (true) {
            while (trade_obj_raw_f == 0) {
                continue;
            }
            SpreadsReq obj = spreads_req.value();
            spdlog::info("obj={}", format_as(obj));
            double usdt_to_use = 45.0;
            std::string ex_fut = obj.t_fut().ex();
            std::string symbol_fut = obj.t_fut().s();
            ClientPublic* client_public_fut =
                ch.get_client_public(ex_fut, "fut");
            client_public_fut->onmessage_depth = [&](const Depth depth) {};
            client_public_fut->subscribe_to_depth(symbol_fut);
            if (client_public_fut->order_book_cache.get_last_update_id() != 0) {
                throw std::runtime_error(
                    "fut order-book is not empty on start");
            }
            std::string ex_spot = obj.t_spot().ex();
            std::string symbol_spot = obj.t_spot().s();
            ClientPublic* client_public_spot =
                ch.get_client_public(ex_spot, "spot");
            client_public_spot->onmessage_depth = [&](const Depth depth) {};
            client_public_spot->subscribe_to_depth(symbol_spot);
            if (client_public_spot->order_book_cache.get_last_update_id() !=
                0) {
                throw std::runtime_error(
                    "spot order-book is not empty on start");
            }
            spdlog::info("wait for order-books to be downloaded");
            while (true) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                bool is_ready_fut =
                    client_public_fut->order_book_cache.get_last_update_id() !=
                    0;
                bool is_ready_spot =
                    client_public_spot->order_book_cache.get_last_update_id() !=
                    0;
                spdlog::info("tick is_ready_spot={} is_ready_fut={}",
                             is_ready_spot, is_ready_fut);
                if (is_ready_fut && is_ready_spot) {
                    double bid_fut =
                        client_public_fut->order_book_cache.get_top_bid()
                            .get_d();
                    double bid_spot =
                        client_public_spot->order_book_cache.get_top_bid()
                            .get_d();
                    double diff_rel_2 = (bid_fut - bid_spot) / bid_spot * 100;
                    spdlog::info("bid_spot={} bid_fut={} diff_rel_2={}",
                                 bid_spot, bid_fut, diff_rel_2);
                    spdlog::info("p_spot={} p_fut={} diff_rel={} obj={}",
                                 obj.t_spot().p(), obj.t_fut().p(),
                                 obj.diff_rel(), format_as(obj));
                    break;
                }
            }
            ClientPrivate* client_private_fut =
                ch.get_client_private(ex_fut, "fut");
            double bid_open_fut =
                client_public_fut->order_book_cache.get_top_bid().get_d();
            auto [sell_price_fut, sell_quantity_fut] =
                client_private_fut->adjust_price_quantity(
                    symbol_fut, bid_open_fut * 0.96,
                    usdt_to_use / bid_open_fut);
            client_private_fut->place_fut_limit_order(
                symbol_fut, "sell", sell_price_fut, sell_quantity_fut);
            wait_until(
                [&]() { return client_private_fut->get_orders_len() > 0; });
            SPDLOG_INFO("buy-order appeared");
            wait_until([&]() {
                return client_private_fut->get_last_order().isFilled();
            });
            SPDLOG_INFO("buy-order is filled");
            client_private_fut->set_leverage_to_1(symbol_fut);
            SPDLOG_INFO("leverage been set to 1");
            double bid_open_spot =
                client_public_spot->order_book_cache.get_top_bid().get_d();
            ClientPrivate* client_private_spot =
                ch.get_client_private(ex_spot, "spot");
            auto [buy_price_spot, buy_quantity_spot] =
                client_private_spot->adjust_price_quantity(
                    symbol_spot, bid_open_spot * 1.04,
                    usdt_to_use / bid_open_spot);
            client_private_spot->place_spot_limit_order(
                symbol_spot, "buy", buy_price_spot, buy_quantity_spot);
            wait_until(
                [&]() { return client_private_spot->get_orders_len() > 0; });
            SPDLOG_INFO("buy-order appeared");
            wait_until([&]() {
                return client_private_spot->get_last_order().isFilled();
            });
            SPDLOG_INFO(
                "buy-order is filled => start waiting for price converge");
            while (true) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                double bid_fut =
                    client_public_fut->order_book_cache.get_top_bid().get_d();
                double bid_spot =
                    client_public_spot->order_book_cache.get_top_bid().get_d();
                double diff_rel = (bid_fut - bid_spot) / bid_spot * 100;
                spdlog::info(
                    "bid_spot={:.6f} bid_fut={:.6f} diff_rel = { : .2f } ",
                    bid_spot, bid_fut, diff_rel);
                if (diff_rel < 0.5) {
                    SPDLOG_INFO("price converged => close trades obj={}",
                                format_as(obj));
                    break;
                }
            }
            double bid_close_fut =
                client_public_fut->order_book_cache.get_top_bid().get_d();
            auto [buy_price_fut, _] = client_private_fut->adjust_price_quantity(
                symbol_fut, bid_close_fut * 1.04, 0.0);
            std::string buy_quantity_fut = client_private_fut->conv_size_to_str(
                client_private_fut->get_last_order().filled_amount);
            client_private_fut->place_fut_limit_order(
                symbol_fut, "buy", buy_price_fut, buy_quantity_fut);
            wait_until(
                [&]() { return client_private_fut->get_orders_len() > 1; });
            SPDLOG_INFO("sell-order appeared");
            wait_until([&]() {
                return client_private_fut->get_last_order().isFilled();
            });
            SPDLOG_INFO("sell-order is filled => finish");
            client_private_fut->clear_orders();
            double bid_close_spot =
                client_public_spot->order_book_cache.get_top_bid().get_d();
            auto [sell_price, sell_quantity] =
                client_private_spot->adjust_price_quantity(
                    symbol_spot, bid_close_spot * 0.96,
                    client_private_spot->get_last_order().filled_amount);
            client_private_spot->place_spot_limit_order(
                symbol_spot, "sell", sell_price, sell_quantity);
            wait_until(
                [&]() { return client_private_spot->get_orders_len() > 1; });
            SPDLOG_INFO("sell-order appeared");
            wait_until([&]() {
                return client_private_spot->get_last_order().isFilled();
            });
            SPDLOG_INFO("sell-order is filled => finish");
            client_private_spot->clear_orders();
            TelegramBotPort::new_from_envs().notify_pretty(
                __FILENAME__, "trade-executed-2024-12-18");
            throw std::runtime_error(
                "test[trade-executed-2024-12-18] done => exit");
            // NOTE: order book cleaned up is checked here because sometimes
            // bybit sends events in the next order (snapshot, subscribe, ...)
            // NOTE: connections drop happens because of weird unsubscribe
            // mechanism implemented in exchanges
            spdlog::info("close connection and set-up them again");
            ch.close_all();
            spdlog::debug("all closed");
            ch.init_idle_public();
            spdlog::debug("all connected again");
            spreads_req = {};
            trade_obj_raw_f = 0;
        }
    });
    run_listening_for_events_sync();
}

void debug_fetch_tickers() {
    std::string symbol_gateio_fut = "PHIL_USDT";
    Ticker t_gateio_fut =
        ClientPublicGateio("fut").fetch_ticker(symbol_gateio_fut);
    std::cout << "t_gateio_fut=" << t_gateio_fut.toString() << std::endl;
    std::string symbol_gateio_spot = "ZZZ_USDT";
    Ticker t_gateio_spot =
        ClientPublicGateio("spot").fetch_ticker(symbol_gateio_spot);
    std::cout << "t_gateio_spot=" << t_gateio_spot.toString() << std::endl;
    std::string symbol_mexc_spot = "ETHUSDT";
    Ticker t_mexc_spot =
        ClientPublicMexc("spot").fetch_ticker(symbol_mexc_spot);
    std::cout << "t_mexc_spot=" << t_mexc_spot.toString() << std::endl;
    std::string symbol_bybit_fut = "XVSUSDT";
    Ticker t_bybit_fut =
        ClientPublicBybit("fut").fetch_ticker(symbol_bybit_fut);
    std::cout << "t_bybit_fut=" << t_bybit_fut.toString() << std::endl;
    std::string symbol_bybit_spot = "ODOSUSDT";
    Ticker t_bybit_spot =
        ClientPublicBybit("spot").fetch_ticker(symbol_bybit_spot);
    std::cout << "t_bybit_spot=" << t_bybit_spot.toString() << std::endl;
}

void debug_place_fetch_order_gateio_fut() {
    ClientPrivateGateio client_private_gateio_fut("fut");
    Order order_1 = client_private_gateio_fut.place_fut_limit_order(
        "DHX_USDT", "sell", "0.02", "1");
    std::cout << "order_gateio_fut_1=" << order_1.toString() << std::endl;
    Order order_2 = client_private_gateio_fut.fetch_order(order_1);
    std::cout << "order_2=" << order_2.toString() << std::endl;
}

void debug_place_fetch_order_gateio_spot() {
    ClientPrivateGateio client_private_gateio_spot("spot");
    Order order_gateio_spot_1 =
        client_private_gateio_spot.place_spot_limit_order("ZRX_USDT", "buy",
                                                          "0.38", "20");
    std::cout << "order_gateio_spot_1=" << order_gateio_spot_1.toString()
              << std::endl;
    Order order_gateio_spot_2 =
        client_private_gateio_spot.fetch_order(order_gateio_spot_1);
    std::cout << "order_gateio_spot_2=" << order_gateio_spot_2.toString()
              << std::endl;
}

void debug_place_fetch_order_mexc_spot() {
    ClientPrivateMexc client_private_mexc_spot("spot");
    Order order_mexc_spot_1 = client_private_mexc_spot.place_spot_limit_order(
        "XMRUSDT", "buy", "160", "0.05");
    std::cout << "order_mexc_spot_1=" << order_mexc_spot_1.toString()
              << std::endl;
    Order order_mexc_spot_2 =
        client_private_mexc_spot.fetch_order(order_mexc_spot_1);
    std::cout << "order_mexc_spot_2=" << order_mexc_spot_2.toString()
              << std::endl;
}

void debug_place_fetch_order_bybit_fut() {
    ClientPrivateBybit client("fut");
    Order order_1 =
        client.place_fut_limit_order("XVSUSDT", "buy", "6.5", "1.5");
    std::cout << "order_1=" << order_1.toString() << std::endl;
    Order order_2 = client.fetch_order(order_1);
    std::cout << "order_mexc_spot_2=" << order_2.toString() << std::endl;
}

void debug_place_fetch_order_bybit_spot() {
    ClientPrivateBybit client("spot");
    Order order_1 =
        client.place_spot_limit_order("GRASSUSDT", "buy", "2.17", "10");
    std::cout << "order_1=" << order_1.toString() << std::endl;
    Order order_2 = client.fetch_order(order_1);
    std::cout << "order_2=" << order_2.toString() << std::endl;
}

///
/// Print results in the next way https://prnt.sc/TwSTJHuWFEGO.
///
void print_out_on_execute_v4(std::map<std::string, Order>& orders_map,
                             ClientsHouse& ch, double usdt_to_use) {
    Order fo_t = orders_map["fut-open"];
    double fo_fee =
        ch.get_client_private(fo_t.ex, "fut")->fetch_order_fee_usdt(fo_t);
    Order so_t = orders_map["spot-open"];
    double so_fee =
        ch.get_client_private(so_t.ex, "spot")->fetch_order_fee_usdt(so_t);
    Order fc_t = orders_map["fut-close"];
    double fc_fee =
        ch.get_client_private(fc_t.ex, "fut")->fetch_order_fee_usdt(fc_t);
    Order sc_t = orders_map["spot-close"];
    double sc_fee =
        ch.get_client_private(sc_t.ex, "spot")->fetch_order_fee_usdt(sc_t);
    double spread_open = (fo_t.p_avg_fill - so_t.p_avg_fill) / so_t.p_avg_fill;
    double spread_close = (fc_t.p_avg_fill - sc_t.p_avg_fill) / sc_t.p_avg_fill;
    double fee_total = fo_fee + so_fee + fc_fee + sc_fee;
    double profit =
        (spread_close - spread_open) / 100.0 * usdt_to_use - fee_total;
    std::ostringstream out_ss;
    out_ss << "now:\t" << time_point_to_str(std::chrono::system_clock::now())
           << std::endl;
    out_ss << "obj:\t" << format_as(spreads_req_v2.value()) << std::endl;
    out_ss << "fut-open:\t" << fo_t.p_avg_fill << "\t" << fo_fee << std::endl;
    out_ss << "spot-open:\t" << so_t.p_avg_fill << "\t" << so_fee << std::endl;
    out_ss << "fut-close:\t" << fc_t.p_avg_fill << "\t" << fc_fee << std::endl;
    out_ss << "spot-close:\t" << sc_t.p_avg_fill << "\t" << sc_fee << std::endl;
    out_ss << "usdt-to-use:\t" << usdt_to_use << std::endl;
    out_ss << "spread-open:\t" << spread_open << std::endl;
    out_ss << "spread-close:\t" << spread_close << std::endl;
    out_ss << "total fees USDT:\t" << fee_total << std::endl;
    out_ss << "profit USDT:\t" << profit << std::endl;
    debug_print_balances(out_ss);
    std::cout << out_ss.str();
    std::ofstream outfile(".var/out-trade-contango-arbitrage-2024-11-11-v4",
                          std::ios_base::app);
    outfile << out_ss.str() << std::endl;
    outfile.close();
}

void debug_print_balances(std::ostream& stream) {
    auto mexc_spot = ClientPrivateMexc("spot").fetch_balances();
    auto gateio_fut = ClientPrivateGateio("fut").fetch_balances();
    auto gateio_spot = ClientPrivateGateio("spot").fetch_balances();
    auto bybit_unified = ClientPrivateBybit("").fetch_balances();
    stream << "mexc-spot:\t" << mexc_spot["USDT"] << std::endl;
    stream << "gateio-fut:\t" << gateio_fut["USDT"] << std::endl;
    stream << "gateio-spot:\t" << gateio_spot["USDT"] << std::endl;
    stream << "bybit-unified:\t" << bybit_unified["USDT"] << std::endl;
}

enum StateV4 { wait_for_spread, place_open_orders, place_close_orders };

///
/// Places 2 orders immediately with market (technically limit) buy.
///
void execute_v4() {
    // NOTE: it's impossible to fill 2 orders on (fut, spot) with the same
    //       prices because i cant use limit orders
    // NOTE: there are 2 ways to catch this spread:
    //       1. by being quicker than others
    //       2. statistically know that order will be n% => fill it through
    //       limit orders for fixed price
    ClientsHouse ch = ClientsHouse();
    ch.init_public_clients();
    ch.init_private_clients();
    StateV4 state = StateV4::wait_for_spread;
    std::map<std::string, Order> orders_map;
    std::map<std::string, Ticker> last_tickers_map;
    std::mutex orders_map_mutex;
    double usdt_to_use = 25.0;
    std::thread _place_wait_fut([&]() {
        SPDLOG_INFO("_place_wait_fut wait for place_open_orders");
        wait_until([&]() { return state == StateV4::place_open_orders; });
        SpreadsReqV2 obj = spreads_req_v2.value();
        ClientPrivate* client_fut =
            ch.get_client_private(obj.t_fut().ex(), "fut");
        {
            SPDLOG_INFO("_place_wait_fut place sell order");
            auto [sell_price, sell_quantity] =
                client_fut->adjust_price_quantity(
                    obj.t_fut().s(), obj.t_fut().p_bid() * 0.96,
                    usdt_to_use / obj.t_fut().p_bid());
            Order sell_order = client_fut->place_fut_limit_order(
                obj.t_fut().s(), "sell", sell_price, sell_quantity);
            SPDLOG_INFO("_place_wait_fut wait for sell order fill");
            wait_until([&]() {
                sell_order = client_fut->fetch_order(sell_order);
                return sell_order.st == "FILLED";
            });
            // TODO: set leverage to 1
            orders_map_mutex.lock();
            orders_map["fut-open"] = sell_order;
            orders_map_mutex.unlock();
        }
        {
            SPDLOG_INFO("_place_wait_fut wait for place_close_orders");
            wait_until([&]() { return state == StateV4::place_close_orders; });
            SPDLOG_INFO("_place_wait_fut place buy order");
            auto [buy_price, _] = client_fut->adjust_price_quantity(
                obj.t_fut().s(), last_tickers_map["fut"].ask * 1.04, 0.0);
            std::string buy_quantity = client_fut->conv_size_to_str(
                orders_map["fut-open"].filled_amount);
            Order buy_order = client_fut->place_fut_limit_order(
                obj.t_fut().s(), "buy", buy_price, buy_quantity);
            SPDLOG_INFO("_place_wait_fut wait for buy order fill");
            wait_until([&]() {
                buy_order = client_fut->fetch_order(buy_order);
                return buy_order.st == "FILLED";
            });
            orders_map_mutex.lock();
            orders_map["fut-close"] = buy_order;
            orders_map_mutex.unlock();
        }
    });
    std::thread _place_wait_spot([&]() {
        SPDLOG_INFO("_place_wait_spot wait for place_open_orders");
        wait_until([&]() { return state == StateV4::place_open_orders; });
        SpreadsReqV2 obj = spreads_req_v2.value();
        ClientPrivate* client_spot =
            ch.get_client_private(obj.t_spot().ex(), "spot");
        {
            SPDLOG_INFO("_place_wait_spot place buy order");
            auto [buy_price, buy_quantity] = client_spot->adjust_price_quantity(
                obj.t_spot().s(), obj.t_spot().p_ask() * 1.04,
                usdt_to_use / obj.t_spot().p_ask());
            Order buy_order = client_spot->place_spot_limit_order(
                obj.t_spot().s(), "buy", buy_price, buy_quantity);
            SPDLOG_INFO("_place_wait_spot wait for buy order fill");
            wait_until([&]() {
                buy_order = client_spot->fetch_order(buy_order);
                return buy_order.st == "FILLED";
            });
            orders_map_mutex.lock();
            orders_map["spot-open"] = buy_order;
            orders_map_mutex.unlock();
        }
        {
            SPDLOG_INFO("_place_wait_spot wait for place_close_orders");
            wait_until([&]() { return state == StateV4::place_close_orders; });
            SPDLOG_INFO("_place_wait_spot place sell order");
            auto [sell_price, sell_quantity] =
                client_spot->adjust_price_quantity(
                    obj.t_spot().s(), last_tickers_map["fut"].bid * 0.96,
                    orders_map["spot-open"].filled_amount);
            Order sell_order = client_spot->place_spot_limit_order(
                obj.t_spot().s(), "sell", sell_price, sell_quantity);
            SPDLOG_INFO("_place_wait_spot wait for sell order fill");
            wait_until([&]() {
                sell_order = client_spot->fetch_order(sell_order);
                return sell_order.st == "FILLED";
            });
            orders_map_mutex.lock();
            orders_map["spot-close"] = sell_order;
            orders_map_mutex.unlock();
        }
    });
    std::thread _1([&]() {
        wait_until([&]() { return trade_obj_raw_f != 0; });
        SpreadsReqV2 obj = spreads_req_v2.value();
        spdlog::info("set status to place_open_orders obj={}", format_as(obj));
        state = StateV4::place_open_orders;
        SPDLOG_INFO("wait for orders to appear in orders_map");
        wait_until([&]() {
            orders_map_mutex.lock();
            bool are_orders_appeared =
                orders_map.find("fut-open") != orders_map.end() &&
                orders_map.find("spot-open") != orders_map.end();
            orders_map_mutex.unlock();
            return are_orders_appeared;
        });
        SPDLOG_INFO("listen for prices converge");
        std::string symbol_fut = obj.t_fut().s();
        std::string symbol_spot = obj.t_spot().s();
        ClientPublic* client_fut =
            ch.get_client_public(obj.t_fut().ex(), "fut");
        ClientPublic* client_spot =
            ch.get_client_public(obj.t_spot().ex(), "spot");
        wait_until([&]() {
            long t1 = now_millis();
            // XXX: fetch them in parallel
            auto f = conv_symbol_price_to_atomic_v1;
            Ticker t_fut = client_fut->fetch_ticker(symbol_fut);
            auto [_1, t_fut_ask] = f(t_fut.s, t_fut.ask);
            auto [_2, t_fut_bid] = f(t_fut.s, t_fut.bid);
            Ticker t_spot = client_spot->fetch_ticker(symbol_spot);
            auto [_3, t_spot_ask] = f(t_spot.s, t_spot.ask);
            auto [_4, t_spot_bid] = f(t_spot.s, t_spot.bid);
            last_tickers_map["fut"] = t_fut;
            last_tickers_map["spot"] = t_spot;
            double diff_ask_bid = (t_fut_bid - t_spot_ask) / t_spot_ask * 100;
            double diff_bid_ask = (t_fut_ask - t_spot_bid) / t_spot_bid * 100;
            SPDLOG_INFO("diff_ask_bid={:.4f} diff_ask_bid={:.4f} dur={}",
                        diff_ask_bid, diff_bid_ask, now_millis() - t1);
            return diff_bid_ask < 0.5;  // XXX: adjust this value
        });
        state = StateV4::place_close_orders;
        wait_until([&]() {
            orders_map_mutex.lock();
            bool are_orders_appeared =
                orders_map.find("fut-close") != orders_map.end() &&
                orders_map.find("spot-close") != orders_map.end();
            orders_map_mutex.unlock();
            return are_orders_appeared;
        });
        SPDLOG_INFO("v4 execution is finished");
        print_out_on_execute_v4(orders_map, ch, usdt_to_use);
    });
    run_listening_for_events_sync();
}

class DebugInitObj {
   public:
    DebugInitObj() : myNum(44), myString("default-44") {
        std::cout << "DebugInitObj constructor/blueprint" << std::endl;
        myNum = 77;
    }
    DebugInitObj(int n) : myString("default-44") { myNum = n; }
    ~DebugInitObj() { std::cout << "DebugInitObj destructor" << std::endl; }
    int myNum;
    std::string myString;
};

void debug_init_obj() {
    {
        DebugInitObj o;
        std::cout << "o.myNum=" << o.myNum << std::endl;
    }
    {
        DebugInitObj o(144);
        std::cout << "o.myNum=" << o.myNum << std::endl;
    }
    {
        DebugInitObj o = DebugInitObj();
        std::cout << "o.myNum=" << o.myNum << std::endl;
    }
    {
        DebugInitObj* o = new DebugInitObj();
        std::cout << "o->myNum=" << o->myNum << std::endl;
    }
    {
        std::cout << (2 << 4) << std::endl;
        std::cout << (400 >> 1) << std::endl;
    }
    {
        mpf_class num1("123456789.123456789123456789");
        mpf_class num2("987654321.987654321987654321");
        mpf_class num3("0.00000000123456789");
        mpf_class sum = num1 + num2 + num3;
        std::cout << std::setprecision(12) << "Sum: " << sum.get_d()
                  << std::endl;
    }
}

class LibrdKafkaDeliveryReportCb : public RdKafka::DeliveryReportCb {
   public:
    void dr_cb(RdKafka::Message& message) {
        if (message.err()) {
            std::cerr << "% Message delivery failed: " << message.errstr()
                      << std::endl;
        } else {
            std::cerr << "% Message delivered to topic " << message.topic_name()
                      << " [" << message.partition() << "] at offset "
                      << message.offset() << std::endl;
        }
    }
};

class ProducerInt {
   public:
    LibrdKafkaDeliveryReportCb ex_dr_cb;
    std::string bootstrap_servers;
    std::string topic;

    ProducerInt(LibrdKafkaDeliveryReportCb& ex_dr_cb_,
                std::string bootstrap_servers_, std::string topic_) {
        ex_dr_cb = ex_dr_cb_;
        bootstrap_servers = bootstrap_servers_;
        topic = topic_;
    }

    void connect() {
        RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        std::string errstr;
        if (conf->set("bootstrap.servers", bootstrap_servers, errstr) !=
            RdKafka::Conf::CONF_OK) {
            std::cerr << "errstr=" << errstr << std::endl;
            exit(1);
        }
        if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        producer = RdKafka::Producer::create(conf, errstr);
        if (!producer) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            exit(1);
        }
        delete conf;
    }

    ~ProducerInt() { delete producer; }

    void produce_messages(std::vector<std::string> messages) {
        for (auto& message : messages) {
            RdKafka::ErrorCode err =
                producer->produce(topic, RdKafka::Topic::PARTITION_UA,
                                  RdKafka::Producer::RK_MSG_COPY,
                                  const_cast<char*>(message.c_str()),
                                  message.size(), NULL, 0, 0, NULL, NULL);
            if (err != RdKafka::ERR_NO_ERROR) {
                std::cerr << "% Failed to produce to topic " << topic << ": "
                          << RdKafka::err2str(err) << std::endl;
                if (err == RdKafka::ERR__QUEUE_FULL) {
                    producer->poll(1000);
                }
            } else {
                std::cerr << "% Enqueued message (" << message.size()
                          << " bytes)"
                          << "for topic " << topic << std::endl;
            }
        }
        producer->poll(100);
    }

    void flush() {
        std::cerr << "% Flushing final messages..." << std::endl;
        producer->flush(10 * 1000);
        if (producer->outq_len() > 0)
            std::cerr << "% " << producer->outq_len()
                      << " message(s) were not delivered" << std::endl;
    }

   private:
    RdKafka::Producer* producer;
};

nlohmann::json conv_spreads_req_v2_to_json(SpreadsReqV2& obj) {
    nlohmann::json obj_json = {
        {"p_ask_spot", obj.p_ask_spot()},
        {"p_bid_fut", obj.p_bid_fut()},
        {"diff_rel", obj.diff_rel()},
        {"t_spot",
         {
             {"ex", obj.t_spot().ex()},
             {"s", obj.t_spot().s()},
             {"st", obj.t_spot().st()},
             {"k", obj.t_spot().k()},
             {"ts", obj.t_spot().ts()},
             {"p_bid", obj.t_spot().p_bid()},
             {"p_ask", obj.t_spot().p_ask()},
             {"v", obj.t_spot().v()},
         }},
        {"t_fut",
         {
             {"ex", obj.t_fut().ex()},
             {"s", obj.t_fut().s()},
             {"st", obj.t_fut().st()},
             {"k", obj.t_fut().k()},
             {"ts", obj.t_fut().ts()},
             {"p_bid", obj.t_fut().p_bid()},
             {"p_ask", obj.t_fut().p_ask()},
             {"v", obj.t_fut().v()},
         }},
    };
    return obj_json;
}

void _write_spreads_to_topic_queries() {
    std::string _redpanda = R"(
    rpk topic create \
        -c retention.ms=900000 \
        -c segment.ms=900000 \
        -c segment.bytes=67108864 \
        -c retention.bytes=67108864 \
        trade-contango-arbitrage-2024-11-11-spreads
    )";
    std::string _clickhouse = R"(
    CREATE TABLE default.trade_contango_arbitrage_2024_11_11_spreads
    (
        read_topic String,
        read_error String,
        read_raw_message String,
        write_timestamp DateTime,
        diff_rel Float64,
        p_ask_spot Float64,
        p_bid_fut Float64,
        t_fut_ex String,
        t_fut_k String,
        t_fut_p_ask Float64,
        t_fut_p_bid Float64,
        t_fut_s String,
        t_fut_st String,
        t_fut_ts DateTime,
        t_fut_v Float64,
        t_spot_ex String,
        t_spot_k String,
        t_spot_p_ask Float64,
        t_spot_p_bid Float64,
        t_spot_s String,
        t_spot_st String,
        t_spot_ts DateTime,
        t_spot_v Float64
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMMDD(t_fut_ts)
    ORDER BY (t_fut_s, t_fut_ts);
    CREATE TABLE default.trade_contango_arbitrage_2024_11_11_spreads_queue
    (data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = 'redpanda-1:9093',
            kafka_topic_list = 'trade-contango-arbitrage-2024-11-11-spreads',
            kafka_group_name = 'clickhouse-consumer',
            kafka_format = 'JSONAsString',
            kafka_thread_per_consumer = 0,
            kafka_num_consumers = 1,
            kafka_handle_error_mode = 'stream',
            kafka_max_block_size = 100000;
    CREATE MATERIALIZED VIEW default.trade_contango_arbitrage_2024_11_11_spreads_mv
    TO default.trade_contango_arbitrage_2024_11_11_spreads AS
    SELECT
        _topic read_topic,
        _error read_error,
        _raw_message read_raw_message,
        _timestamp write_timestamp,
        JSONExtractFloat(data, 'diff_rel') diff_rel,
        JSONExtractFloat(data, 'p_ask_spot') p_ask_spot,
        JSONExtractFloat(data, 'p_bid_fut') p_bid_fut,
        JSONExtractString(data, 't_fut', 'ex') t_fut_ex,
        JSONExtractString(data, 't_fut', 'k') t_fut_k,
        JSONExtractFloat(data, 't_fut', 'p_ask') t_fut_p_ask,
        JSONExtractFloat(data, 't_fut', 'p_bid') t_fut_p_bid,
        JSONExtractString(data, 't_fut', 's') t_fut_s,
        JSONExtractString(data, 't_fut', 'st') t_fut_st,
        toDateTime(JSONExtractUInt(data, 't_fut', 'ts') / 1000) t_fut_ts,
        JSONExtractFloat(data, 't_fut', 'v') t_fut_v,
        JSONExtractString(data, 't_spot', 'ex') t_spot_ex,
        JSONExtractString(data, 't_spot', 'k') t_spot_k,
        JSONExtractFloat(data, 't_spot', 'p_ask') t_spot_p_ask,
        JSONExtractFloat(data, 't_spot', 'p_bid') t_spot_p_bid,
        JSONExtractString(data, 't_spot', 's') t_spot_s,
        JSONExtractString(data, 't_spot', 'st') t_spot_st,
        toDateTime(JSONExtractUInt(data, 't_spot', 'ts') / 1000) t_spot_ts,
        JSONExtractFloat(data, 't_spot', 'v') t_spot_v
    FROM default.trade_contango_arbitrage_2024_11_11_spreads_queue;
    SELECT
        t_spot_s,
        floor(min(diff_rel), 2) AS min,
        floor(max(diff_rel), 2) AS max,
        floor(avg(diff_rel), 2) AS avg,
        groupUniqArray(t_fut_ex) AS t_fut_exs,
        groupUniqArray(t_spot_ex) AS t_spot_exs,
        count() AS amount
    FROM default.trade_contango_arbitrage_2024_11_11_spreads
    GROUP BY t_spot_s
    ORDER BY amount DESC
    LIMIT 20
    )";
}

void write_spreads_to_topic() {
    LibrdKafkaDeliveryReportCb ex_dr_cb;
    std::string bootstrap_servers = "127.0.0.1:9092";
    std::string topic = "trade-contango-arbitrage-2024-11-11-spreads";
    ProducerInt producer_int(ex_dr_cb, bootstrap_servers, topic);
    producer_int.connect();
    run_listening_for_events_sync([&](std::vector<SpreadsReqV2> reqs) {
        std::vector<std::string> messages;
        for (auto& req : reqs) {
            auto req_json = conv_spreads_req_v2_to_json(req);
            messages.push_back(req_json.dump());
        }
        producer_int.produce_messages(messages);
        SPDLOG_INFO("produced {} messages", messages.size());
    });
    producer_int.flush();
}

void _wait_for_spread_converge_save_show_order_book_queries() {
    std::string _redpanda = R"(
    rpk topic create \
        -c retention.ms=900000 \
        -c segment.ms=900000 \
        -c segment.bytes=67108864 \
        -c retention.bytes=67108864 \
        spread-converge-save-show-order-book-queries-depth
    )";
    std::string _clickhouse = R"(
    CREATE TABLE default.spread_converge_save_show_order_book_queries_depth
    (
        read_topic String,
        read_error String,
        read_raw_message String,
        write_timestamp DateTime,
        u UInt64,
        ex_ts_millis UInt64,
        ex String,
        k String,
        s String,
        asks Array(Tuple(String, Float64)),
        bids Array(Tuple(String, Float64)),
        ex_ts DateTime DEFAULT toDateTime(ex_ts_millis) -- TODO: divide by 1000
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMMDD(ex_ts)
    ORDER BY (s, ex, k, ex_ts_millis);
    CREATE TABLE default.spread_converge_save_show_order_book_queries_depth_queue
    (data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = 'redpanda-1:9093',
            kafka_topic_list = 'spread-converge-save-show-order-book-queries-depth',
            kafka_group_name = 'clickhouse-consumer',
            kafka_format = 'JSONAsString',
            kafka_thread_per_consumer = 0,
            kafka_num_consumers = 1,
            kafka_handle_error_mode = 'stream',
            kafka_max_block_size = 100000;
    CREATE MATERIALIZED VIEW default.spread_converge_save_show_order_book_queries_depth_mv
    TO default.spread_converge_save_show_order_book_queries_depth AS
    SELECT
        _topic read_topic,
        _error read_error,
        _raw_message read_raw_message,
        _timestamp write_timestamp,
        JSONExtractUInt(data, 'u') u,
        JSONExtractUInt(data, 'ex_ts_millis') ex_ts_millis,
        JSONExtractString(data, 'ex') ex,
        JSONExtractString(data, 'k') k,
        JSONExtractString(data, 's') s,
        JSONExtract(data, 'asks', 'Array(Tuple(String, Float64))') asks,
        JSONExtract(data, 'bids', 'Array(Tuple(String, Float64))') bids
    FROM default.spread_converge_save_show_order_book_queries_depth_queue;
    )";
}

nlohmann::json conv_depth_to_json(const Depth d) {
    nlohmann::json depth_json = {
        {"u", d.u},       {"ex_ts_millis", d.ex_ts_millis},
        {"asks", d.asks}, {"bids", d.bids},
        {"ex", d.ex},     {"k", d.k},
        {"s", d.s},
    };
    return depth_json;
}

void wait_for_spread_converge_save_order_book() {
    // NOTE: fut price also moves => needs to keep this in mind when thinking
    // about such type of strategy
    std::string symbol_fut = "BITBOARD_USDT";
    std::string ex_fut = "gateio";
    std::string kind_fut = "fut";
    std::string symbol_spot = "BITBOARDUSDT";
    std::string ex_spot = "mexc";
    std::string kind_spot = "spot";
    std::string bootstrap_servers = "127.0.0.1:9092";
    std::string topic = "spread-converge-save-show-order-book-queries-depth";
    LibrdKafkaDeliveryReportCb ex_dr_cb;
    ProducerInt producer_int(ex_dr_cb, bootstrap_servers, topic);
    producer_int.connect();
    ClientsHouse ch;
    ch.init_idle_public();
    std::thread _ping_ws_clients([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            SPDLOG_INFO("ping");
            ch.ping();
        }
    });
    ClientPublic* client_fut = ch.get_client_public(ex_fut, kind_fut);
    ClientPublic* client_spot = ch.get_client_public(ex_spot, kind_spot);
    SPDLOG_INFO("start listening for depth messages and write them info topic");
    client_fut->onmessage_depth = [&](const Depth d) {
        producer_int.produce_messages({conv_depth_to_json(d).dump()});
    };
    client_fut->subscribe_to_depth(symbol_fut);
    client_spot->onmessage_depth = [&](const Depth d) {
        producer_int.produce_messages({conv_depth_to_json(d).dump()});
    };
    client_spot->subscribe_to_depth(symbol_spot);
    auto load_tickers_and_calc_diff = [&]() {
        auto f = conv_symbol_price_to_atomic_v1;
        Ticker t_fut = client_fut->fetch_ticker(symbol_fut);
        auto [_1, t_fut_ask] = f(t_fut.s, t_fut.ask);
        auto [_2, t_fut_bid] = f(t_fut.s, t_fut.bid);
        Ticker t_spot = client_spot->fetch_ticker(symbol_spot);
        auto [_3, t_spot_ask] = f(t_spot.s, t_spot.ask);
        auto [_4, t_spot_bid] = f(t_spot.s, t_spot.bid);
        double diff_ask_bid = (t_fut_bid - t_spot_ask) / t_spot_ask * 100;
        double diff_bid_ask = (t_fut_ask - t_spot_bid) / t_spot_bid * 100;
        return std::make_tuple(diff_ask_bid, diff_bid_ask);
    };
    SPDLOG_INFO("start waiting for spread");
    if (std::get<0>(load_tickers_and_calc_diff()) > 1.5) {
        throw std::runtime_error("spread on start must be < 1.5");
    }
    while (true) {
        auto [diff_ask_bid, _] = load_tickers_and_calc_diff();
        SPDLOG_INFO("diff_ask_bid={:.4f}", diff_ask_bid);
        if (diff_ask_bid > 1.5) {
            break;
        }
    }
    SPDLOG_INFO("spread appeared => wait for converge");
    while (true) {
        auto [_, diff_bid_ask] = load_tickers_and_calc_diff();
        SPDLOG_INFO("diff_bid_ask={:.4f}", diff_bid_ask);
        if (diff_bid_ask < 0.5) {
            break;
        }
    }
    SPDLOG_INFO("prices covnerged => finish");
    TelegramBotPort::new_from_envs().notify_pretty(
        __FILENAME__, "spread-converge-cought-2024-12-29");
    // XXX: unsubscribe and close connection in proper way
}

void wait_for_spread_converge_show_order_book() {
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::string QUERY_TIMESTAMPS = R"(
        SELECT
            toUInt64(ex_ts_millis / 1000) ex_ts_secs,
            toDateTime(ex_ts_secs) AS ex_ts_2
        FROM default.spread_converge_save_show_order_book_queries_depth
        WHERE ex_ts_secs >= 1735501040
        GROUP BY ex_ts_secs
        ORDER BY ex_ts_secs ASC
    )";
    std::string QUERY_DEPTH = R"(
        SELECT u, toJSONString(asks) asks_str, toJSONString(bids) bids_str
        FROM default.spread_converge_save_show_order_book_queries_depth
        WHERE ex_ts_millis >= 1735501040000
            AND ex_ts_millis <= %(ex_ts_millis_threshold_top)s
            AND k = '%(kind)s'
        ORDER BY ex_ts_millis ASC
    )";
    std::vector<long> tss_secs = {};
    int ts_i = 1;
    clickhouse_client.Select(QUERY_TIMESTAMPS, [&](const clickhouse::Block& b) {
        for (size_t i = 0; i < b.GetRowCount(); ++i) {
            long ex_ts_secs = b[0]->As<clickhouse::ColumnUInt64>()->At(i);
            tss_secs.push_back(ex_ts_secs);
        }
    });
    SPDLOG_DEBUG("tss_secs={}", tss_secs.size());
    auto fetch_construct_order_book = [&](long ex_ts_millis_threshold_top,
                                          std::string kind) {
        OrderBookCache order_book;
        std::string q1 =
            replace_first(QUERY_DEPTH, "%(ex_ts_millis_threshold_top)s",
                          fmt::format("{}", ex_ts_millis_threshold_top));
        std::string q2 = replace_first(q1, "%(kind)s", kind);
        clickhouse_client.Select(q2, [&](const clickhouse::Block& b) {
            for (size_t i = 0; i < b.GetRowCount(); ++i) {
                long u = b[0]->As<clickhouse::ColumnUInt64>()->At(i);
                std::string asks_str =
                    (std::string)b[1]->As<clickhouse::ColumnString>()->At(i);
                std::string bids_str =
                    (std::string)b[2]->As<clickhouse::ColumnString>()->At(i);
                std::vector<std::tuple<std::string, double>> asks = {};
                for (auto& obj : nlohmann::json::parse(asks_str)) {
                    asks.push_back({obj[0], obj[1]});
                }
                std::vector<std::tuple<std::string, double>> bids = {};
                for (auto& obj : nlohmann::json::parse(bids_str)) {
                    bids.push_back({obj[0], obj[1]});
                }
                long order_book_u = order_book.get_last_update_id();
                if (order_book_u == 0 || order_book_u + 1 == u) {
                    order_book.apply_orders(u, asks, bids);
                } else {
                    SPDLOG_WARN("{} order-book contains non increments depth",
                                kind);
                    order_book.apply_orders_force(u, asks, bids);
                }
            }
        });
        return order_book;
    };
    auto fetch_print_order_books = [&]() {
        OrderBookCache order_book_fut =
            fetch_construct_order_book(tss_secs[ts_i] * 1000, "fut");
        OrderBookCache order_book_spot =
            fetch_construct_order_book(tss_secs[ts_i] * 1000, "spot");
        std::cout << "\x1B[2J\x1B[H";
        std::cout << "tss_secs[0]\t" << tss_secs[0] << "\t"
                  << time_point_to_str(
                         std::chrono::system_clock::from_time_t(tss_secs[0]))
                  << std::endl;
        std::cout << "tss_secs[ts_i]\t" << tss_secs[ts_i] << "\t"
                  << time_point_to_str(
                         std::chrono::system_clock::from_time_t(tss_secs[ts_i]))
                  << std::endl;
        std::cout << "fut:" << std::endl;
        order_book_fut.print(5);
        std::cout << "spot:" << std::endl;
        order_book_spot.print(5);
    };
    while (true) {
        fetch_print_order_books();
        system("stty raw");
        char t = getchar();
        system("stty cooked");
        int t_int = (int)t;
        if (t_int == 3) {
            SPDLOG_INFO("ctrl+c pressed");
            break;
        } else if (t_int == 108) {
            ts_i = std::min(ts_i + 100, (int)tss_secs.size() - 1);
        } else if (t_int == 106) {
            ts_i = std::max(ts_i - 100, 0);
        } else if (t_int == 46) {
            ts_i = std::min(ts_i + 1, (int)tss_secs.size() - 1);
        } else if (t_int == 44) {
            ts_i = std::max(ts_i - 1, 0);
        }
    }
}

void debug_amend_order_gateio_fut() {
    ClientPrivateGateio client("fut");
    Order order =
        client.place_fut_limit_order("GOLDENCAT_USDT", "buy", "0.0000005", "1");
    SPDLOG_INFO("order placed");
    client.amend_fut_order(order, "0.00000049");
    SPDLOG_INFO("order amended");
}

void debug_amend_order_bybit_fut() {
    ClientPrivateBybit client("fut");
    Order order =
        client.place_fut_limit_order("DOGEUSDT", "buy", "0.302", "35");
    SPDLOG_INFO("order placed");
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    client.amend_fut_order(order, "0.301");
    SPDLOG_INFO("order amended");
}

///
/// On spread appearance place limit order on fut, than constantly amend it with
/// price spot*103% till it will be filled. When it's filled buy spot market.
/// Aim is to get into position with better spread.
///
void debug_buy_one_side_through_limit_order() {
    ClientsHouse ch = ClientsHouse();
    ch.init_public_clients();
    ch.init_private_clients();
    double usdt_to_use = 25.0;
    std::thread _1([&]() {
        wait_until([&]() { return trade_obj_raw_f != 0; });
        SpreadsReqV2 obj = spreads_req_v2.value();
        SPDLOG_INFO("run obj={}", format_as(obj));
        ClientPrivate* client_pr_fut =
            ch.get_client_private(obj.t_fut().ex(), "fut");
        ClientPublic* client_pub_fut =
            ch.get_client_public(obj.t_fut().ex(), "fut");
        ClientPrivate* client_pr_spot =
            ch.get_client_private(obj.t_spot().ex(), "spot");
        ClientPublic* client_pub_spot =
            ch.get_client_public(obj.t_spot().ex(), "spot");
        std::string symbol_fut = obj.t_fut().s();
        std::string symbol_spot = obj.t_spot().s();
        std::optional<Order> sell_order_fut = {};
        std::optional<Ticker> t_spot = {};
        {
            SPDLOG_INFO("place fut sell order");
            t_spot = client_pub_spot->fetch_ticker(symbol_spot);
            auto [sell_price, sell_quantity] =
                client_pr_fut->adjust_price_quantity(
                    symbol_fut, t_spot.value().ask * 1.03,
                    usdt_to_use / t_spot.value().ask);
            sell_order_fut = client_pr_fut->place_fut_limit_order(
                symbol_fut, "sell", sell_price, sell_quantity);
        }
        SPDLOG_INFO("wait fot fut fill (overwise and with spot price)");
        while (true) {
            sell_order_fut = client_pr_fut->fetch_order(sell_order_fut.value());
            if (sell_order_fut.value().st == "FILLED") {
                break;
            } else {
                t_spot = client_pub_spot->fetch_ticker(symbol_spot);
                auto [sell_price, _] = client_pr_fut->adjust_price_quantity(
                    symbol_fut, t_spot.value().ask * 1.03, .0);
                // XXX: change quantity too
                client_pr_fut->amend_fut_order(sell_order_fut.value(),
                                               sell_price);
            }
        }
        // TODO: set leverage to 1
        SPDLOG_INFO("fut sell order is filled => place spot order");
        auto [buy_price_spot, buy_quantity_spot] =
            client_pr_spot->adjust_price_quantity(
                symbol_spot, t_spot.value().bid * 1.04,
                usdt_to_use / t_spot.value().bid);
        Order buy_order_spot = client_pr_spot->place_spot_limit_order(
            symbol_spot, "buy", buy_price_spot, buy_quantity_spot);
        wait_until([&]() {
            buy_order_spot = client_pr_spot->fetch_order(buy_order_spot);
            return buy_order_spot.st == "FILLED";
        });
        SPDLOG_INFO("spot buy-order is filled => close orders immediately");
        {
            SPDLOG_INFO("place fut buy order");
            auto [buy_price_fut, _] = client_pr_fut->adjust_price_quantity(
                symbol_fut, t_spot.value().ask * 1.04, 0.0);
            std::string buy_quantity_fut = client_pr_fut->conv_size_to_str(
                sell_order_fut.value().filled_amount);
            Order buy_order_fut = client_pr_fut->place_fut_limit_order(
                symbol_fut, "buy", buy_price_fut, buy_quantity_fut);
            SPDLOG_INFO("wait fut for buy order fill");
            wait_until([&]() {
                buy_order_fut = client_pr_fut->fetch_order(buy_order_fut);
                return buy_order_fut.st == "FILLED";
            });
        }
        {
            SPDLOG_INFO("place spot sell order");
            auto [sell_price_spot, sell_quantity_spot] =
                client_pr_spot->adjust_price_quantity(
                    symbol_spot, t_spot.value().bid * 0.96,
                    buy_order_spot.filled_amount);
            Order sell_order_spot = client_pr_spot->place_spot_limit_order(
                symbol_spot, "sell", sell_price_spot, sell_quantity_spot);
            SPDLOG_INFO("wait for spot sell order fill");
            wait_until([&]() {
                sell_order_spot = client_pr_spot->fetch_order(sell_order_spot);
                return sell_order_spot.st == "FILLED";
            });
        }
        TelegramBotPort::new_from_envs().notify_pretty(
            __FILENAME__, "trade-with-one-side-buy-executed-2025-01-01");
    });
    run_listening_for_events_sync();
}

void _map_exchanges_tokens_queries() {
    std::string _clickhouse = R"(
    CREATE TABLE default.t_mexc_2025_01_02 (
        `symbol_raw` String,
        `s` String,
        `k` String,
        `orderTypes` Array(String),
        `isSpotTradingAllowed` Boolean,
        `permissions` Array(String),
        `filters` Array(String),
        `fullName` String,
        `tradeSideType` UInt8
    )
    ENGINE = TinyLog;
    INSERT INTO default.t_mexc_2025_01_02
    SELECT
        symbol_raw,
        JSONExtractString(symbol_raw, 'symbol') s,
        'spot' k,
        JSONExtract(symbol_raw, 'orderTypes', 'Array(String)') orderTypes,
        JSONExtractBool(symbol_raw, 'isSpotTradingAllowed') isSpotTradingAllowed,
        JSONExtract(symbol_raw, 'permissions', 'Array(String)') permissions,
        JSONExtract(symbol_raw, 'filters', 'Array(String)') filters,
        JSONExtractString(symbol_raw, 'fullName') fullName,
        JSONExtract(symbol_raw, 'tradeSideType', 'UInt8') tradeSideType
    FROM (
        SELECT arrayJoin(JSONExtractArrayRaw(json, 'symbols')) symbol_raw
        FROM url('https://api.mexc.com/api/v3/exchangeInfo', 'JSONAsString')
    );
    CREATE TABLE default.t_gateio_2025_01_02 (
        `current_pair_raw` String,
        `currency_raw` String,
        `contract_raw` String,
        `s` String,
        `k` String,
        `trade_status` String,
        `sell_start` DateTime,
        `buy_start` DateTime,
        `delisted` Boolean,
        `trade_disabled` Boolean,
        `in_delisting` Boolean
    )
    ENGINE = TinyLog;
    INSERT INTO default.t_gateio_2025_01_02
    SELECT
        '{}' current_pair_raw,
        '{}' currency_raw,
        json contract_raw,
        JSONExtractString(contract_raw, 'name') s,
        'fut' k,
        '' trade_status,
        toDateTime(0) sell_start,
        toDateTime(0) buy_start,
        false delisted,
        false trade_disabled,
        JSONExtractBool(contract_raw, 'in_delisting') in_delisting
    FROM url('https://api.gateio.ws/api/v4/futures/usdt/contracts', JSONAsString);
    INSERT INTO default.t_gateio_2025_01_02
    SELECT
        current_pair_raw,
        currency_raw,
        '{}' contract_raw,
        JSONExtractString(current_pair_raw, 'id') s,
        'spot' k,
        JSONExtractString(current_pair_raw, 'trade_status') trade_status,
        toDateTime(JSONExtract(current_pair_raw, 'sell_start', 'UInt64')) sell_start,
        toDateTime(JSONExtract(current_pair_raw, 'buy_start', 'UInt64')) buy_start,
        JSONExtractBool(currency_raw, 'delisted') delisted,
        JSONExtractBool(currency_raw, 'trade_disabled') trade_disabled,
        false in_delisting
    FROM (
        SELECT json current_pair_raw
        FROM url('https://api.gateio.ws/api/v4/spot/currency_pairs', 'JSONAsString')
    ) t1
    LEFT JOIN (
        SELECT json currency_raw
        FROM url('https://api.gateio.ws/api/v4/spot/currencies', 'JSONAsString')
    ) t2
        ON JSONExtractString(current_pair_raw, 'base') =
            JSONExtractString(currency_raw, 'currency');
    CREATE TABLE default.t_bybit_2025_01_02 (
        `symbol_raw` String,
        `s` String,
        `k` String,
        `baseCoin` String,
        `quoteCoin` String,
        `status` String,
        `fundingInterval` UInt64,
        `settleCoin` String,
        `upperFundingRate` Float64,
        `lowerFundingRate` Float64,
        `isPreListing` Boolean
    )
    ENGINE = TinyLog;
    INSERT INTO default.t_bybit_2025_01_02
    SELECT
        symbol_raw,
        JSONExtractString(symbol_raw, 'symbol') s,
        'fut' k,
        JSONExtractString(symbol_raw, 'baseCoin') baseCoin,
        JSONExtractString(symbol_raw, 'quoteCoin') quoteCoin,
        JSONExtractString(symbol_raw, 'status') status,
        JSONExtract(symbol_raw, 'fundingInterval', 'UInt64') fundingInterval,
        JSONExtractString(symbol_raw, 'settleCoin') settleCoin,
        JSONExtract(symbol_raw, 'upperFundingRate', 'Float64') upperFundingRate,
        JSONExtract(symbol_raw, 'lowerFundingRate', 'Float64') lowerFundingRate,
        JSONExtractBool(symbol_raw, 'isPreListing') isPreListing
    FROM (
        SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) symbol_raw
        FROM url('https://api.bybit.com/v5/market/instruments-info?category=linear', JSONAsString)
    );
    INSERT INTO default.t_bybit_2025_01_02
    SELECT
        symbol_raw,
        JSONExtractString(symbol_raw, 'symbol') s,
        'spot' k,
        JSONExtractString(symbol_raw, 'baseCoin') baseCoin,
        JSONExtractString(symbol_raw, 'quoteCoin') quoteCoin,
        JSONExtractString(symbol_raw, 'status') status,
        0 fundingInterval,
        '' settleCoin,
        .0 upperFundingRate,
        .0 lowerFundingRate,
        false isPreListing
    FROM (
        SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) symbol_raw
        FROM url('https://api.bybit.com/v5/market/instruments-info?category=spot', JSONAsString)
    );
    )";
}
