#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include <clickhouse/client.h>

#include <backward.hpp>
#include <iostream>

#include "src/clients.hxx"
#include "src/lib.hxx"

backward::SignalHandling sh{};

#define GEN_VARNAME(x) #x

void upload_fundings_curr();
void insert_fundings_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                         std::vector<FundingRes> fundings_vec);
void insert_tickers_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                        std::vector<TickerRes> tickers_vec);

std::map<std::string, void (*)()> FNS_MAP{
    GET_FN_NAME_TO_FN(upload_fundings_curr),
};

int main() {
    // NOTE: backpack.exchange - perps ain't available (Beta) on 2025-01-19
    // NOTE: avantisfi.com - use python sdk because blockchain call is needed
    // NOTE: regarding bingx funding intervals: it's possible to fetch fundings
    //       intervals from requests on page
    //       https://bingx.com/en/tradeInfo/swap-trade-info, but as an idea
    //       this could be extracted from historical fundings like group by
    //       symbol nextFundingTime1 - nextFundingTime0
    // XXX: conv_symbol_to_token_dumb
    //      workout exp bybit futures e.g. BTC-14FEB25
    //      workout USDC bybit futures e.g. AEVOPERP
    //      workout and filter pre-market tokens
    configure_logger();
    std::string v = std::getenv("MODE");
    auto fn = FNS_MAP.find(v);
    if (fn == FNS_MAP.end()) {
        throw std::runtime_error(fmt::format("unexpected MODE={}", v));
    }
    fn->second();
    return 0;
}

template <typename F>
void exec_safe(F&& f) {
    try {
        while (true) {
            f();
        }
    } catch (...) {
        SPDLOG_ERROR("fall");
    }
}

void upload_fundings_curr() {
    ClientPublicBinance client_binance_fut("fut");
    ClientPublicBinance client_binance_spot("spot");
    ClientPublicBybit client_bybit_fut("fut");
    ClientPublicBybit client_bybit_spot("spot");
    ClientPublicGateio client_gateio_fut("fut");
    ClientPublicMexc client_mexc_fut("fut");
    ClientPublicArkm client_arkm("fut");
    ClientPublicParadex client_paradex("fut");
    ClientPublicPolynomialFi client_polynomial_fi("fut");
    ClientPublicCoinEx client_coinex("fut");
    ClientPublicBingx client_bingx("fut");
    ClientPublicHyperliquid client_hyperliquid("fut");
    ClientPublicApexPro client_apex_pro("fut");
    ClientPublicApexOmni client_apex_omni("fut");
    ClientPublicAevo client_aevo("fut");
    ClientPublicBitunix client_bitunix("fut");
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::mutex clickhouse_client_mutex;
    auto exec_insert = [&](ClientPublic& client, std::string k = "fundings") {
        int attempt = 1;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                SPDLOG_DEBUG("insert ex={} attempt={}", client.ex, attempt);
                clickhouse_client_mutex.lock();
                std::string sql_str = "";
                if (k == "tickers") {
                    sql_str = client.gen_tickers_insert_sql();
                } else if (k == "fundings") {
                    sql_str = client.gen_fundings_insert_sql();
                } else {
                    throw new std::runtime_error(
                        fmt::format("unexpected k={}", k));
                }
                clickhouse_client.Execute(sql_str);
                clickhouse_client_mutex.unlock();
                return;
            } catch (...) {
                SPDLOG_ERROR("error for ex={} => wait 10s and continue",
                             client.ex);
                std::this_thread::sleep_for(std::chrono::seconds(10));
                clickhouse_client_mutex.unlock();
            }
        }
        throw new std::runtime_error(
            fmt::format("failed to insert={}", client.ex));
    };
    auto exec_insert_fundings_vec = [&](std::vector<FundingRes> fundings_vec) {
        SPDLOG_DEBUG("insert_fundings_vec ex={}", fundings_vec[0].ex);
        clickhouse_client_mutex.lock();
        insert_fundings_vec_into_clickhouse(clickhouse_client, fundings_vec);
        clickhouse_client_mutex.unlock();
    };
    auto exec_insert_tickers_vec = [&](std::vector<TickerRes> tickers_vec) {
        SPDLOG_DEBUG("insert_tickers_vec ex={}", tickers_vec[0].ex);
        clickhouse_client_mutex.lock();
        insert_tickers_vec_into_clickhouse(clickhouse_client, tickers_vec);
        clickhouse_client_mutex.unlock();
    };
    std::atomic<bool> pool_alive(true);
    std::thread _1([&]() {
        exec_safe([&]() {
            exec_insert(client_binance_fut, "tickers");
            exec_insert(client_binance_spot, "tickers");
            exec_insert(client_bybit_fut, "tickers");
            exec_insert(client_bybit_spot, "tickers");
            exec_insert(client_gateio_fut, "tickers");
            exec_insert(client_mexc_fut, "tickers");
            exec_insert(client_arkm);
            exec_insert(client_paradex);
            exec_insert(client_polynomial_fi);
            exec_insert(client_coinex);
            exec_insert(client_bingx);
            exec_insert_fundings_vec(client_hyperliquid.fetch_fundings_sync());
            exec_insert_tickers_vec(client_apex_pro.fetch_tickers_sync());
        });
        pool_alive = false;
    });
    std::thread _2([&]() {
        exec_safe([&]() {
            exec_insert_tickers_vec(client_apex_omni.fetch_tickers_sync());
        });
        pool_alive = false;
    });
    std::thread _3([&]() {
        exec_safe([&]() {
            exec_insert_fundings_vec(client_aevo.fetch_fundings_sync());
        });
        pool_alive = false;
    });
    std::thread _4([&]() {
        exec_safe([&]() {
            exec_insert_fundings_vec(client_bitunix.fetch_fundings_sync());
        });
        pool_alive = false;
    });
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!pool_alive) {
            break;
        }
    }
    SPDLOG_ERROR("fall");
    TelegramBotPort::new_from_envs().notify_pretty(__FILENAME__, "fall");
}

void insert_fundings_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                         std::vector<FundingRes> fundings_vec) {
    auto obj_raw_vec = std::make_shared<clickhouse::ColumnString>();
    auto obj_k_vec = std::make_shared<clickhouse::ColumnString>();
    auto s_vec = std::make_shared<clickhouse::ColumnString>();
    auto funding_rate_vec = std::make_shared<clickhouse::ColumnFloat64>();
    auto ts_vec = std::make_shared<clickhouse::ColumnUInt64>();
    auto ex_vec = std::make_shared<clickhouse::ColumnString>();
    auto k_vec = std::make_shared<clickhouse::ColumnString>();
    auto ts_write_vec = std::make_shared<clickhouse::ColumnDateTime>();
    long ts_write = now_millis() / 1000;
    for (auto& obj : fundings_vec) {
        obj_raw_vec->Append(obj.obj_raw.dump());
        obj_k_vec->Append(obj.obj_k);
        s_vec->Append(obj.s);
        funding_rate_vec->Append(obj.funding_rate);
        ts_vec->Append(obj.ts);
        ex_vec->Append(obj.ex);
        k_vec->Append(obj.k);
        ts_write_vec->Append(ts_write);
    }
    clickhouse::Block block;
    block.AppendColumn("obj_raw", obj_raw_vec);
    block.AppendColumn("obj_k", obj_k_vec);
    block.AppendColumn("s", s_vec);
    block.AppendColumn("funding_rate", funding_rate_vec);
    block.AppendColumn("ts", ts_vec);
    block.AppendColumn("ex", ex_vec);
    block.AppendColumn("k", k_vec);
    block.AppendColumn("ts_write", ts_write_vec);
    clickhouse_client.Insert("default.fundings_curr_2025_01_12", block);
}

void insert_tickers_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                        std::vector<TickerRes> tickers_vec) {
    auto obj_raw_vec = std::make_shared<clickhouse::ColumnString>();
    auto s_vec = std::make_shared<clickhouse::ColumnString>();
    auto funding_rate_vec = std::make_shared<clickhouse::ColumnFloat64>();
    auto ts_vec = std::make_shared<clickhouse::ColumnUInt64>();
    auto ex_vec = std::make_shared<clickhouse::ColumnString>();
    auto k_vec = std::make_shared<clickhouse::ColumnString>();
    auto ts_write_vec = std::make_shared<clickhouse::ColumnDateTime>();
    long ts_write = now_millis() / 1000;
    for (auto& obj : tickers_vec) {
        obj_raw_vec->Append(obj.obj_raw.dump());
        s_vec->Append(obj.s);
        funding_rate_vec->Append(obj.funding_rate);
        ts_vec->Append(obj.ts);
        ex_vec->Append(obj.ex);
        k_vec->Append(obj.k);
        ts_write_vec->Append(ts_write);
    }
    clickhouse::Block block;
    block.AppendColumn("obj_raw", obj_raw_vec);
    block.AppendColumn("s", s_vec);
    block.AppendColumn("funding_rate", funding_rate_vec);
    block.AppendColumn("ts", ts_vec);
    block.AppendColumn("ex", ex_vec);
    block.AppendColumn("k", k_vec);
    block.AppendColumn("ts_write", ts_write_vec);
    clickhouse_client.Insert("default.tickers", block);
}
