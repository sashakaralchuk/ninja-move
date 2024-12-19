#include "clients.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(ClientsTest, conv_to_dec_str_v2_ExpectedBehaviour) {
    EXPECT_EQ(conv_to_dec_str_v2(0.019, "0.01"), "0.01");
    EXPECT_EQ(conv_to_dec_str_v2(0.019, "0.005"), "0.015");
    EXPECT_EQ(conv_to_dec_str_v2(0.219, "0.1"), "0.2");
    EXPECT_EQ(conv_to_dec_str_v2(0.019123, "0.00001"), "0.01912");
    EXPECT_EQ(conv_to_dec_str_v2(111.012, "10"), "110");
    EXPECT_EQ(conv_to_dec_str_v2(111, "1"), "111");
}

TEST(ClientsTest, gen_precision_str_ExpectedBehaviour) {
    try {
        gen_precision_str(-1);
        EXPECT_TRUE(false);
    } catch (...) {
    }
    EXPECT_EQ(gen_precision_str(0), "1");
    EXPECT_EQ(gen_precision_str(1), "0.1");
    EXPECT_EQ(gen_precision_str(2), "0.01");
}

TEST(ClientsTest, cover_n_zeros_tokens) {
    double usdt_to_use = 20.0;
    double price = 0.0000003305;
    double quantity = usdt_to_use / price;
    std::string order_price_round = "0.0000000001";
    std::string quanto_multiplier = "10000000";
    EXPECT_EQ(conv_to_dec_str_v2(price, order_price_round), "0.0000003305");
    EXPECT_EQ(conv_to_dec_str_v2(quantity, quanto_multiplier), "60000000");
}

class ClientPrivateGateioMock : public ClientPrivateGateio {
   public:
    ClientPrivateGateioMock(std::string kind) : ClientPrivateGateio(kind) {
        fut_exchange_info = nlohmann::json::parse(R"(
            [
                {
                    "order_price_round": "0.0000000001",
                    "name": "GOLDENCAT_USDT",
                    "quanto_multiplier": "10000000"
                },
                {
                    "order_price_round": "0.05",
                    "name": "ETH_USDT",
                    "quanto_multiplier": "0.01"
                }
            ]
        )");
    }
    ~ClientPrivateGateioMock() noexcept override {}
};

TEST(ClientsTest, ClientPrivateGateio_adjust_price_quantity_fut) {
    setenv("GATEIO_API_KEY", "", 1);
    setenv("GATEIO_API_SECRET", "", 1);
    ClientPrivateGateioMock client = ClientPrivateGateioMock("fut");
    {
        double t1_usdt_to_use = 54.0;
        double t2_usdt_to_use = 55.0;
        double t1_price = 0.0000009;
        auto t1_GOLDENCAT_USDT = client.adjust_price_quantity(
            "GOLDENCAT_USDT", t1_price, t1_usdt_to_use / t1_price);
        EXPECT_EQ(std::get<0>(t1_GOLDENCAT_USDT), "0.0000009000");
        EXPECT_EQ(std::get<1>(t1_GOLDENCAT_USDT), "6");
        auto t2_GOLDENCAT_USDT = client.adjust_price_quantity(
            "GOLDENCAT_USDT", t1_price, t2_usdt_to_use / t1_price);
        EXPECT_EQ(std::get<0>(t2_GOLDENCAT_USDT), "0.0000009000");
        EXPECT_EQ(std::get<1>(t2_GOLDENCAT_USDT), "6");
    }
    {
        double t1_usdt_to_use = 48.0;
        double t1_price = 4000.00192191290;
        auto t1_ETH_USDT = client.adjust_price_quantity(
            "ETH_USDT", t1_price, t1_usdt_to_use / t1_price);
        EXPECT_EQ(std::get<0>(t1_ETH_USDT), "4000.00");
        EXPECT_EQ(std::get<1>(t1_ETH_USDT), "1");
    }
}

class ClientPrivateBybitMock : public ClientPrivateBybit {
   public:
    ClientPrivateBybitMock(std::string kind) : ClientPrivateBybit(kind) {
        fut_exchange_info = nlohmann::json::parse(R"(
            {
                "result": {
                    "list": [
                        {
                            "symbol": "VRAUSDT",
                            "priceFilter": {"tickSize": "0.000001"},
                            "lotSizeFilter": {"qtyStep": "100"}
                        },
                        {
                            "symbol": "ETHUSDT",
                            "priceFilter": {"tickSize": "0.01"},
                            "lotSizeFilter": {"qtyStep": "0.01"}
                        }
                    ]
                }
            }
        )");
    }
    ~ClientPrivateBybitMock() noexcept override {}
};

TEST(ClientsTest, ClientPrivateBybit_adjust_price_quantity_fut) {
    setenv("BYBIT_API_KEY", "", 1);
    setenv("BYBIT_API_SECRET", "", 1);
    ClientPrivateBybitMock client = ClientPrivateBybitMock("fut");
    {
        double usdt_to_use = 20.0;
        double price = 0.0058;
        auto [buy_price, buy_quantity] =
            client.adjust_price_quantity("VRAUSDT", price, usdt_to_use / price);
        EXPECT_EQ(buy_price, "0.005800");
        EXPECT_EQ(buy_quantity, "3400");
    }
    {
        double usdt_to_use = 40.0;
        double price = 3800.0;
        auto [buy_price, buy_quantity] =
            client.adjust_price_quantity("ETHUSDT", price, usdt_to_use / price);
        EXPECT_EQ(buy_price, "3800.00");
        EXPECT_EQ(buy_quantity, "0.01");
    }
}

TEST(ClientsTest, ClientPrivate_conv_size_to_str) {
    EXPECT_EQ(rstrip_zeros("7.000000"), "7.");
    EXPECT_EQ(rstrip_zeros("7."), "7.");
    EXPECT_EQ(rstrip_zeros("0"), "");
    setenv("BYBIT_API_KEY", "", 1);
    setenv("BYBIT_API_SECRET", "", 1);
    ClientPrivateBybitMock client = ClientPrivateBybitMock("fut");
    EXPECT_EQ(client.conv_size_to_str(7.0), "7");
    EXPECT_EQ(client.conv_size_to_str(0.03), "0.03");
}
