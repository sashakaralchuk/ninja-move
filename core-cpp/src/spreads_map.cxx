#include "spreads_map.hxx"

#include <clickhouse/client.h>

#include <iostream>
#include <string>

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
                TickerSpread t = ccid_ex_k_t[ccid][ex][k];
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
