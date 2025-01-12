#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include "spreads_map.hxx"

#include <clickhouse/client.h>

#include <iostream>
#include <string>

#include "trade_contango.grpc.pb.h"

void SpreadsMap::init_ccid_map_from_clickhouse() {
    std::string QUERY_EX_K_B_R_CCID = R"(
        SELECT
            ex,
            'fut' AS k,
            base,
            target,
            coingecko_coin_id
        FROM default.t_fut_to_coingecko_coin_id
        UNION ALL
        WITH map('mxc', 'mexc', 'gate', 'gateio', 'bybit_spot', 'bybit') AS market_identifier_to_ex
        SELECT
            market_identifier_to_ex[market_identifier] AS ex,
            k,
            base,
            target,
            JSONExtractString(ticker_raw, 'coin_id') AS coingecko_coin_id
        FROM default.t_coingecko_exchanges_tickers_2025_01_05
        WHERE k = 'spot'
        GROUP BY
            market_identifier,
            k,
            base,
            target,
            coingecko_coin_id
    )";
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    clickhouse_client.Select(
        QUERY_EX_K_B_R_CCID, [&](const clickhouse::Block& b) {
            for (size_t i = 0; i < b.GetRowCount(); ++i) {
                std::string ex =
                    (std::string)b[0]->As<clickhouse::ColumnString>()->At(i);
                std::string k =
                    (std::string)b[1]->As<clickhouse::ColumnString>()->At(i);
                std::string base =
                    (std::string)b[2]->As<clickhouse::ColumnString>()->At(i);
                std::string target =
                    (std::string)b[3]->As<clickhouse::ColumnString>()->At(i);
                std::string ccid =
                    (std::string)b[4]->As<clickhouse::ColumnString>()->At(i);
                ex_k_b_t_ccid[{ex, k, base, target}] = ccid;
            }
        });
}

void SpreadsMap::insert(TickerSpread& t) {
    if (t.quote != "USDT") {
        SPDLOG_DEBUG("ignore non-usdt t.quote={}", t.quote);
        return;
    }
    t.ccid = ex_k_b_t_ccid[{t.ex, t.k, t.base, t.quote}];
    if (t.ccid == "") {
        SPDLOG_DEBUG("ccid not found for ex={} k={} base={} quote={}", t.ex,
                     t.k, t.base, t.quote);
        return;
    }
    if (ccid_ex_k_t.find(t.ccid) == ccid_ex_k_t.end()) {
        ccid_ex_k_t[t.ccid] = {};
    }
    if (ccid_ex_k_t[t.ccid].find(t.ex) == ccid_ex_k_t[t.ccid].end()) {
        ccid_ex_k_t[t.ccid][t.ex] = {};
    }
    ccid_ex_k_t[t.ccid][t.ex][t.k] = t;
}

///
/// Finds the lowest ask on spot from al exchanges.
/// Finds the highest bid on fut from all exs.
///
std::optional<std::tuple<TickerSpread, TickerSpread>> SpreadsMap::find_spreads(
    std::string ccid) {
    std::optional<TickerSpread> max_fut = {};
    std::optional<TickerSpread> min_spot = {};
    long m1_millis = now_millis() - 60000;
    for (auto o = ccid_ex_k_t[ccid].cbegin(); o != ccid_ex_k_t[ccid].cend();
         ++o) {
        std::string ex = o->first;
        for (auto o = ccid_ex_k_t[ccid][ex].cbegin();
             o != ccid_ex_k_t[ccid][ex].cend(); ++o) {
            std::string k = o->first;
            TickerSpread t = ccid_ex_k_t[ccid][ex][k];
            if (t.t_raw.ts < m1_millis) {
                continue;
            }
            if (k == "fut") {
                if (!max_fut.has_value()) {
                    max_fut = t;
                }
                if (t.t_raw.bid > max_fut.value().t_raw.bid) {
                    max_fut = t;
                }
            } else if (k == "spot") {
                if (!min_spot.has_value()) {
                    min_spot = t;
                }
                if (t.t_raw.ask < min_spot.value().t_raw.ask) {
                    min_spot = t;
                }
            }
        }
    }
    if (max_fut.has_value() && min_spot.has_value()) {
        return std::make_tuple(max_fut.value(), min_spot.value());
    }
    return {};
}

std::vector<std::tuple<TickerSpread, TickerSpread>>
SpreadsMap::find_spreads_all() {
    std::vector<std::tuple<TickerSpread, TickerSpread>> vec;
    for (auto o = ccid_ex_k_t.cbegin(); o != ccid_ex_k_t.cend(); ++o) {
        auto spread = find_spreads(o->first);
        if (spread.has_value()) {
            vec.push_back(spread.value());
        }
    }
    return vec;
}

trade_contango::FireTradeReqV3 SpreadsMap::find_spreads_all_req_v3() {
    double threshold_diff = std::stod(std::getenv("DIFF_REL_BOTTOM_THRESHOLD"));
    trade_contango::FireTradeReqV3 o_out;
    std::vector<std::tuple<TickerSpread, TickerSpread>> vec =
        find_spreads_all();
    int skipped_amount = 0;
    double max_diff_ask_bid = .0;
    for (auto& [t_fut, t_spot] : vec) {
        double diff_ask_bid =
            (t_fut.t_raw.bid - t_spot.t_raw.ask) / t_spot.t_raw.ask * 100;
        if (diff_ask_bid < threshold_diff) {
            skipped_amount++;
            max_diff_ask_bid = std::max(max_diff_ask_bid, diff_ask_bid);
            continue;
        }
        double diff_bid_ask =
            (t_fut.t_raw.ask - t_spot.t_raw.bid) / t_spot.t_raw.bid * 100;
        trade_contango::SpreadsReqV3* o = o_out.add_list();
        o->set_diff_ask_bid_rel(diff_ask_bid);
        o->set_diff_bid_ask_rel(diff_bid_ask);
        o->set_t_fut_ex(t_fut.ex);
        o->set_t_fut_s(t_fut.t_raw.s);
        o->set_t_fut_st("");
        o->set_t_fut_k(t_fut.k);
        o->set_t_fut_ts(t_fut.t_raw.ts);
        o->set_t_fut_p_bid(t_fut.t_raw.bid);
        o->set_t_fut_p_ask(t_fut.t_raw.ask);
        o->set_t_fut_v(.0);
        o->set_t_spot_ex(t_spot.ex);
        o->set_t_spot_s(t_spot.t_raw.s);
        o->set_t_spot_st("");
        o->set_t_spot_k(t_spot.k);
        o->set_t_spot_ts(t_spot.t_raw.ts);
        o->set_t_spot_p_bid(t_spot.t_raw.bid);
        o->set_t_spot_p_ask(t_spot.t_raw.ask);
        o->set_t_spot_v(.0);
    }
    SPDLOG_INFO(
        "vec.size()={} o_out.list_size()={} skipped_amount={} "
        "threshold_diff={} max_diff_ask_bid={}",
        vec.size(), o_out.list_size(), skipped_amount, threshold_diff,
        max_diff_ask_bid);
    return o_out;
}

void SpreadsMap::print() {
    for (auto o = ccid_ex_k_t.cbegin(); o != ccid_ex_k_t.cend(); ++o) {
        std::string ccid = o->first;
        for (auto o = ccid_ex_k_t[ccid].cbegin(); o != ccid_ex_k_t[ccid].cend();
             ++o) {
            std::string ex = o->first;
            for (auto o = ccid_ex_k_t[ccid][ex].cbegin();
                 o != ccid_ex_k_t[ccid][ex].cend(); ++o) {
                std::string k = o->first;
                TickerSpread t = ccid_ex_k_t[ccid][ex][k];
                std::cout << "ccid=" << ccid << "\tex=" << ex << " k=" << k
                          << " t=" << t.t_raw.toString() << std::endl;
            }
        }
    }
}
