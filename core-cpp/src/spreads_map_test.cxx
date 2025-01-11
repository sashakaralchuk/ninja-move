#include "spreads_map.hxx"

#include <gtest/gtest.h>

class SpreadsMapMock : public SpreadsMap {
   public:
    SpreadsMapMock() {
        ex_k_b_t_ccid[{"bybit", "fut", "ETH", "USDT"}] = "t1";
        ex_k_b_t_ccid[{"bybit", "spot", "ETH", "USDT"}] = "t1";
        ex_k_b_t_ccid[{"gateio", "fut", "ETH", "USDT"}] = "t1";
        ex_k_b_t_ccid[{"gateio", "spot", "ETH", "USDT"}] = "t1";
    }
};

TickerSpread create_ticker(std::string ex, std::string k, double bid,
                           double ask) {
    return TickerSpread{
        .t_raw =
            Ticker{
                .s = "",
                .ask = ask,
                .bid = bid,
            },
        .ex = ex,
        .k = k,
        .base = "ETH",
        .quote = "USDT",
        .ccid = "t1",
    };
}

TEST(SpreadsMap, find_spreads) {
    {
        SpreadsMapMock sm;
        TickerSpread t1 = create_ticker("bybit", "spot", 3000.0, 3001.0);
        TickerSpread t2 = create_ticker("bybit", "fut", 3010.0, 3011.0);
        sm.insert(t1);
        sm.insert(t2);
        auto [t_fut, t_spot] = sm.find_spreads("t1").value();
        EXPECT_EQ(t_fut.t_raw.bid, 3010.0);
        EXPECT_EQ(t_spot.t_raw.ask, 3001.0);
    }
    {
        SpreadsMapMock sm;
        TickerSpread t_spot_1 = create_ticker("bybit", "spot", 3000.0, 3001.0);
        TickerSpread t_spot_2 = create_ticker("gateio", "spot", 2996.0, 2997.0);
        TickerSpread t_fut_1 = create_ticker("bybit", "fut", 3010.0, 3011.0);
        TickerSpread t_fut_2 = create_ticker("gateio", "fut", 3030.0, 3031.0);
        sm.insert(t_spot_1);
        sm.insert(t_spot_2);
        sm.insert(t_fut_1);
        sm.insert(t_fut_2);
        auto [t_fut, t_spot] = sm.find_spreads("t1").value();
        EXPECT_EQ(t_fut.t_raw.bid, 3030.0);
        EXPECT_EQ(t_spot.t_raw.ask, 2997.0);
    }
    {
        SpreadsMapMock sm;
        TickerSpread t_spot_1 =
            create_ticker("bybit", "spot", 60000.0, 60000.0);
        TickerSpread t_spot_2 =
            create_ticker("bybit", "spot", 61000.0, 61000.0);
        TickerSpread t_fut_1 = create_ticker("bybit", "fut", 65000.0, 65000.0);
        TickerSpread t_fut_2 = create_ticker("bybit", "fut", 62000.0, 62000.0);
        TickerSpread t_fut_3 = create_ticker("bybit", "fut", 63000.0, 63000.0);
        sm.insert(t_spot_1);
        sm.insert(t_spot_2);
        sm.insert(t_fut_1);
        sm.insert(t_fut_2);
        sm.insert(t_fut_3);
        auto [t_fut, t_spot] = sm.find_spreads("t1").value();
        EXPECT_EQ(t_fut.t_raw.bid, 63000.0);
        EXPECT_EQ(t_spot.t_raw.ask, 61000.0);
    }
}
