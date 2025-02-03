#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include <clickhouse/client.h>

#include <iostream>

#include "src/clients.hxx"
#include "src/lib.hxx"

#define GEN_VARNAME(x) #x

void upload_fundings_curr();
void insert_fundings_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                         std::vector<FundingRes> fundings_vec);

std::map<std::string, void (*)()> FNS_MAP{
    GET_FN_NAME_TO_FN(upload_fundings_curr),
};

int main() {
    // NOTE: backpack.exchange - perps ain't available (Beta) on 2025-01-19
    configure_logger();
    std::string v = std::getenv("MODE");
    auto fn = FNS_MAP.find(v);
    if (fn == FNS_MAP.end()) {
        throw std::runtime_error(fmt::format("unexpected MODE={}", v));
    }
    fn->second();
    return 0;
}

void upload_fundings_curr() {
    ClientPublicBybit client_bybit("fut");
    ClientPublicGateio client_gateio("fut");
    ClientPublicMexc client_mexc("fut");
    ClientPublicHyperliquid client_hyperliquid("fut");
    ClientPublicArkm client_arkm("fut");
    ClientPublicParadex client_paradex("fut");
    ClientPublicPolynomialFi client_plynomial_fi("fut");
    ClientPublicApexPro client_apex_pro("fut");
    ClientPublicApexOmni client_apex_omni("fut");
    ClientPublicAevo client_aevo("fut");
    clickhouse::Client clickhouse_client(
        clickhouse::ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    auto run_insert = [&](ClientPublic& client) {
        SPDLOG_DEBUG("insert ex={}", client.ex);
        clickhouse_client.Execute(client.gen_fundings_insert_sql());
    };
    while (true) {
        run_insert(client_bybit);
        run_insert(client_gateio);
        run_insert(client_mexc);
        run_insert(client_arkm);
        run_insert(client_paradex);
        run_insert(client_plynomial_fi);
        auto f = insert_fundings_vec_into_clickhouse;
        f(clickhouse_client, client_hyperliquid.fetch_fundings_sync());
        f(clickhouse_client, client_apex_pro.fetch_fundings_sync());
        f(clickhouse_client, client_apex_omni.fetch_fundings_sync());
        f(clickhouse_client, client_aevo.fetch_fundings_sync());
        int secs = 30;
        SPDLOG_INFO("sleep for {}s", secs);
        std::this_thread::sleep_for(std::chrono::seconds(secs));
    }
}

void insert_fundings_vec_into_clickhouse(clickhouse::Client& clickhouse_client,
                                         std::vector<FundingRes> fundings_vec) {
    auto ticker_raw_vec = std::make_shared<clickhouse::ColumnString>();
    auto symbol_vec = std::make_shared<clickhouse::ColumnString>();
    auto funding_rate_vec = std::make_shared<clickhouse::ColumnFloat64>();
    auto next_funding_time_vec = std::make_shared<clickhouse::ColumnInt64>();
    auto ts_vec = std::make_shared<clickhouse::ColumnInt64>();
    auto ex_vec = std::make_shared<clickhouse::ColumnString>();
    auto k_vec = std::make_shared<clickhouse::ColumnString>();
    for (auto& obj : fundings_vec) {
        ticker_raw_vec->Append(obj.ticker_raw.dump());
        symbol_vec->Append(obj.symbol);
        funding_rate_vec->Append(obj.funding_rate);
        next_funding_time_vec->Append(obj.next_funding_time);
        ts_vec->Append(obj.ts);
        ex_vec->Append(obj.ex);
        k_vec->Append(obj.k);
    }
    clickhouse::Block block;
    block.AppendColumn("ticker_raw", ticker_raw_vec);
    block.AppendColumn("symbol", symbol_vec);
    block.AppendColumn("fundingRate", funding_rate_vec);
    block.AppendColumn("nextFundingTime", next_funding_time_vec);
    block.AppendColumn("ts", ts_vec);
    block.AppendColumn("ex", ex_vec);
    block.AppendColumn("k", k_vec);
    clickhouse_client.Insert("default.fundings_curr_2025_01_12", block);
}
