#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include <clickhouse/client.h>

#include <backward.hpp>
#include <iostream>
#include <unordered_set>

#include "src/clients.hxx"
#include "src/lib.hxx"

backward::SignalHandling sh{};

#define GEN_VARNAME(x) #x
#define GET_STR_COL_AT(vec, idx, i) \
    (std::string)(vec)[(idx)]->As<clickhouse::ColumnString>()->At((i))

void upload_exchanges_entities_curr();
void insert_fundings_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                         std::vector<FundingRes> fundings_vec);
void insert_tickers_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                        std::vector<TickerRes> tickers_vec);
void check_send_spot_fut_perp_spread_alert(
    clickhouse::Client& clickhouse_client, double diff_rel_threshold);
void observe_send_spot_fut_perp_spread_converge_v1();

std::map<std::string, void (*)()> FNS_MAP{
    GET_FN_NAME_TO_FN(upload_exchanges_entities_curr),
    GET_FN_NAME_TO_FN(observe_send_spot_fut_perp_spread_converge_v1),
};

int main() {
    // NOTE: backpack.exchange - perps ain't available (Beta) on 2025-01-19
    // NOTE: avantisfi.com - use python sdk because blockchain call is needed
    // NOTE: regarding bingx funding intervals: it's possible to fetch fundings
    //       intervals from requests on page
    //       https://bingx.com/en/tradeInfo/swap-trade-info, but as an idea
    //       this could be extracted from historical fundings like group by
    //       symbol nextFundingTime1 - nextFundingTime0
    // NOTE: regaring mexc funding intervals: it's possible to fetch fundings
    //       from endpoint api/v1/contract/funding_rate/{symbol} and then make
    //       nextFundingTime1 - nextFundingTime0
    // XXX: add exchanges from https://defillama.com/protocols/Derivatives
    //      and coingecko https://www.coingecko.com/en/exchanges
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
    } catch (const std::exception& e) {
        SPDLOG_ERROR("e={}", e.what());
    } catch (...) {
        SPDLOG_ERROR("e=\"Unknown exception caught!\"");
    }
}

void backoff_call(std::function<void(int)> f_success,
                  std::function<void()> f_error, std::string ex) {
    int attempt = 1;
    for (int attempt = 1; attempt <= 3; attempt++) {
        try {
            f_success(attempt);
            return;
        } catch (const std::exception& e) {
            f_error();
            SPDLOG_ERROR("backoff-error => wait 10s and continue ex={} e={}",
                         ex, e.what());
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
    throw std::runtime_error(
        fmt::format("backoff-error: all attempts are gone ex={}", ex));
}

void upload_exchanges_entities_curr() {
    ClientPublicBinance client_binance_fut("fut");
    ClientPublicBinance client_binance_spot("spot");
    ClientPublicBybit client_bybit_fut("fut");
    ClientPublicBybit client_bybit_spot("spot");
    ClientPublicGateio client_gateio_fut("fut");
    ClientPublicGateio client_gateio_spot("spot");
    ClientPublicMexc client_mexc_fut("fut");
    ClientPublicMexc client_mexc_spot("spot");
    ClientPublicBitget client_bitget_fut("fut");
    ClientPublicBitget client_bitget_spot("spot");
    ClientPublicOkx client_okx_fut("fut");
    ClientPublicOkx client_okx_spot("spot");
    ClientPublicKucoin client_kucoin_fut("fut");
    ClientPublicKucoin client_kucoin_spot("spot");
    ClientPublicArkm client_arkm("fut");
    ClientPublicParadex client_paradex("fut");
    ClientPublicPolynomialFi client_polynomial_fi("fut");
    ClientPublicCoinEx client_coinex_fut("fut");
    ClientPublicBingx client_bingx_fut("fut");
    ClientPublicHyperliquid client_hyperliquid("fut");
    ClientPublicApexPro client_apex_pro("fut");
    ClientPublicApexOmni client_apex_omni("fut");
    ClientPublicAevo client_aevo("fut");
    ClientPublicBitunix client_bitunix("fut");
    ClientPublicLBank client_lbank_fut("fut");
    ClientPublicLBank client_lbank_spot("spot");
    ClientPublicBitmart client_bitmart_fut("fut");
    ClientPublicBitmart client_bitmart_spot("spot");
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::mutex clickhouse_client_mutex;
    auto exec_insert = [&](ClientPublic& client, std::string k) {
        backoff_call(
            [&](int attempt) {
                SPDLOG_DEBUG("exec_insert ex={} k={} attempt={}", client.ex,
                             client.kind, attempt);
                clickhouse_client_mutex.lock();
                std::string sql_str = "";
                if (k == "tickers") {
                    sql_str = client.gen_tickers_insert_sql();
                } else if (k == "fundings") {
                    sql_str = client.gen_fundings_insert_sql();
                } else {
                    throw std::runtime_error(fmt::format("unexpected k={}", k));
                }
                clickhouse_client.Execute(sql_str);
                clickhouse_client_mutex.unlock();
            },
            [&]() { clickhouse_client_mutex.unlock(); }, client.ex);
    };
    auto exec_insert_fundings_sync = [&](ClientPublicFetchFundingsSync& c) {
        backoff_call(
            [&](int attempt) {
                SPDLOG_DEBUG("exec_insert_fundings_sync ex={} k={} attempt={}",
                             c.ex, c.k, attempt);
                std::vector<FundingRes> fundings_vec = c.fetch_fundings_sync();
                clickhouse_client_mutex.lock();
                insert_fundings_vec_into_clickhouse(clickhouse_client,
                                                    fundings_vec);
                clickhouse_client_mutex.unlock();
            },
            [&] { clickhouse_client_mutex.unlock(); }, c.ex);
    };
    auto exec_insert_tickers_sync = [&](ClientPublicFetchTickersSync& c) {
        backoff_call(
            [&](int attempt) {
                SPDLOG_DEBUG("exec_insert_tickers_sync ex={} k={} attempt={}",
                             c.ex, c.k, attempt);
                std::vector<TickerRes> tickers_vec = c.fetch_tickers_sync();
                clickhouse_client_mutex.lock();
                insert_tickers_vec_into_clickhouse(clickhouse_client,
                                                   tickers_vec);
                clickhouse_client_mutex.unlock();
            },
            [&] { clickhouse_client_mutex.unlock(); }, c.ex);
    };
    SPDLOG_INFO("re-upload gateio-contracts");
    clickhouse_client.Execute(ClientPublicGateio::QUERY_TRUNCATE_CONTRACTS);
    clickhouse_client.Execute(ClientPublicGateio::QUERY_INSERT_CONTRACTS);
    SPDLOG_INFO("re-upload bybit-instruments-info");
    clickhouse_client.Execute(
        ClientPublicBybit::QUERY_TRUNCATE_INSTRUMENTS_INFO);
    clickhouse_client.Execute(ClientPublicBybit::QUERY_INSERT_INSTRUMENTS_INFO);
    SPDLOG_INFO("re-upload paradex-markets");
    clickhouse_client.Execute(ClientPublicParadex::QUERY_TRUNCATE_MARKETS);
    clickhouse_client.Execute(ClientPublicParadex::QUERY_INSERT_MARKETS);
    // XXX: use boost.asio here, install instruction -
    // https://www.youtube.com/watch?v=CP_U4sb75cM
    std::atomic<bool> pool_alive(true);
    std::thread _check_alert_threshold([&]() {
        exec_safe([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            clickhouse_client_mutex.lock();
            double diff_rel_threshold = 5.0;
            check_send_spot_fut_perp_spread_alert(clickhouse_client,
                                                  diff_rel_threshold);
            clickhouse_client_mutex.unlock();
        });
        pool_alive = false;
    });
    std::thread _main_list([&]() {
        exec_safe([&]() {
            exec_insert(client_bybit_fut, "tickers");
            exec_insert(client_bybit_spot, "tickers");
            exec_insert(client_gateio_fut, "tickers");
            exec_insert(client_gateio_spot, "tickers");
            exec_insert(client_mexc_fut, "tickers");
            exec_insert(client_mexc_spot, "tickers");
            exec_insert(client_bitget_fut, "tickers");
            exec_insert(client_bitget_spot, "tickers");
            exec_insert(client_kucoin_fut, "tickers");
            exec_insert(client_kucoin_spot, "tickers");
            exec_insert_tickers_sync(client_lbank_fut);
            exec_insert_tickers_sync(client_lbank_spot);
            exec_insert(client_bitmart_fut, "tickers");
            exec_insert(client_bitmart_spot, "tickers");
        });
        pool_alive = false;
    });
    std::thread _remaining_list([&]() {
        exec_safe([&]() {
            exec_insert(client_binance_fut, "tickers");
            exec_insert(client_binance_spot, "tickers");
            exec_insert(client_okx_fut, "tickers");
            exec_insert(client_okx_spot, "tickers");
            exec_insert(client_arkm, "fundings");
            exec_insert(client_paradex, "fundings");
            exec_insert(client_polynomial_fi, "fundings");
            exec_insert(client_coinex_fut, "fundings");
            exec_insert(client_coinex_fut, "tickers");
            exec_insert(client_bingx_fut, "fundings");
            exec_insert(client_bingx_fut, "tickers");
            exec_insert_fundings_sync(client_hyperliquid);
            exec_insert_tickers_sync(client_apex_pro);
            exec_insert_tickers_sync(client_apex_omni);
            exec_insert_fundings_sync(client_aevo);
            exec_insert_fundings_sync(client_bitunix);
        });
        pool_alive = false;
    });
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!pool_alive) {
            break;
        }
    }
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

std::string read_grafan_spot_fut_perp_spread_sql(std::string title_key) {
    std::ifstream f("../grafana/dashboards/default.json");
    nlohmann::json obj_file = nlohmann::json::parse(f);
    std::string query_spreads_str = "";
    for (auto& obj : obj_file["dashboard"]["panels"]) {
        if (obj["title"] == title_key) {
            query_spreads_str = obj["targets"][0]["query"];
        }
        for (auto& obj_int : obj["panels"]) {
            if (obj_int["title"] == title_key) {
                query_spreads_str = obj_int["targets"][0]["query"];
            }
        }
    }
    if (query_spreads_str.length() == 0) {
        throw std::runtime_error("sql haven't extracted");
    }
    return query_spreads_str;
}

void check_send_spot_fut_perp_spread_alert(
    clickhouse::Client& clickhouse_client, double diff_rel_threshold) {
    std::vector<std::tuple<std::string, std::string>> messages;
    std::ostringstream message_oss;
    auto query_key_and_extend_message = [&](std::string query_key,
                                            std::unordered_set<std::string>&
                                                keys_to_ignore) {
        std::string query_spreads_str =
            read_grafan_spot_fut_perp_spread_sql(query_key);
        std::vector<std::string> messages_int = {};
        clickhouse_client.Select(query_spreads_str, [&](const clickhouse::Block&
                                                            b) {
            for (size_t i = 0; i < b.GetRowCount(); ++i) {
                std::string key = GET_STR_COL_AT(b, 0, i);
                double diff_rel = b[1]->As<clickhouse::ColumnFloat64>()->At(i);
                std::string ex_link_fut = GET_STR_COL_AT(b, 2, i);
                std::string ex_link_spot = GET_STR_COL_AT(b, 3, i);
                std::string ex_fut = GET_STR_COL_AT(b, 10, i);
                std::string ex_spot = GET_STR_COL_AT(b, 11, i);
                if (diff_rel > diff_rel_threshold &&
                    keys_to_ignore.find(key) == keys_to_ignore.end()) {
                    messages_int.push_back(fmt::format(
                        "\\* {}: diff\\_rel={} [{}-fut]({}) vs [{}-spot]({})\n",
                        key, diff_rel, ex_fut, ex_link_fut, ex_spot,
                        ex_link_spot));
                }
            }
        });
        if (messages_int.size() > 0) {
            message_oss << fmt::format("### {}\n", query_key);
            for (auto& m : messages_int) {
                message_oss << m;
            }
        }
    };
    std::unordered_set<std::string> ccids_to_ignore = {
        "nakamoto-games", "axelar",          "peaq-2",
        "blockstack",     "cryptogpt-token", "ice"};
    query_key_and_extend_message("contango-ex-vs-ex", ccids_to_ignore);
    std::unordered_set<std::string> tokens_to_ignore = {
        "NAKA", "AXL", "YFI", "LAI", "PEAQ", "STX", "ICE"};
    query_key_and_extend_message("contango-ex-vs-ex-on-base-token",
                                 tokens_to_ignore);
    std::unordered_set<std::string> t = {};
    query_key_and_extend_message("spot-vs-spot-ex-vs-ex-on-base-token", t);
    query_key_and_extend_message("fut-vs-fut-ex-vs-ex-on-base-token", t);
    if (message_oss.str().empty()) {
        SPDLOG_INFO("there is no spreads with diff_rel_threshold={}",
                    diff_rel_threshold);
        return;
    }
    nlohmann::json meta = {
        {"message", "check-send-spot-fut-perp-spread"},
        {"diff_rel_threshold", diff_rel_threshold},
        {"filename", __FILENAME__},
        {"now", time_point_to_str(std::chrono::system_clock::now())},
    };
    message_oss << "```json\n" << meta.dump(2) << "\n```";
    SPDLOG_INFO("send telegram diff_rel_threshold={} messages={}",
                diff_rel_threshold, message_oss.str());
    TelegramBotPort::new_from_envs().encode_notify_markdown(message_oss.str());
}

void observe_send_spot_fut_perp_spread_converge_v1() {
    std::string input_ex_spot = std::getenv("INPUT_EX_SPOT");
    std::string input_ex_fut_perp = std::getenv("INPUT_EX_FUT_PERP");
    std::string input_ccid = std::getenv("INPUT_CCID");
    double diff_rel_threshold = 1.0;
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    std::string query_spreads_str =
        read_grafan_spot_fut_perp_spread_sql("contango-ex-vs-ex");
    while (true) {
        double diff_rel_iter = -1.0;
        clickhouse_client.Select(query_spreads_str, [&](const clickhouse::Block&
                                                            b) {
            for (size_t i = 0; i < b.GetRowCount(); ++i) {
                std::string ccid =
                    (std::string)b[0]->As<clickhouse::ColumnString>()->At(i);
                double diff_rel = b[1]->As<clickhouse::ColumnFloat64>()->At(i);
                std::string ex_fut =
                    (std::string)b[10]->As<clickhouse::ColumnString>()->At(i);
                std::string ex_spot =
                    (std::string)b[11]->As<clickhouse::ColumnString>()->At(i);
                if (ccid == input_ccid && ex_fut == input_ex_fut_perp &&
                    ex_spot == input_ex_spot) {
                    diff_rel_iter = diff_rel;
                }
            }
        });
        if (diff_rel_iter < diff_rel_threshold) {
            SPDLOG_INFO("send telegram diff_rel_threshold={}",
                        diff_rel_threshold);
            TelegramBotPort::new_from_envs().notify_pretty_v2(
                {std::make_tuple("message", "spread-disappeared"),
                 std::make_tuple("filename", __FILENAME__),
                 std::make_tuple("input_ccid", input_ccid),
                 std::make_tuple("diff_rel_iter",
                                 std::to_string(diff_rel_iter))});
            break;
        } else {
            SPDLOG_INFO(
                "spread still exists diff_rel_iter={} diff_rel_threshold={}",
                diff_rel_iter, diff_rel_threshold);
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}
