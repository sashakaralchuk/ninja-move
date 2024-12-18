#include "clients.hpp"

#include <gtest/gtest.h>

TEST(ClientsTest, conv_to_dec_str_v2_ExpectedBehaviour) {
    EXPECT_EQ(conv_to_dec_str_v2(0.019, "0.01"), "0.01");
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

TEST(ClientsTest, ClientPrivateGateio_adjust_price_quantity) {
    // double usdt_to_use = 20.0;
    // std::string symbol = "FLT_USDT";
    // double price = 0.4126;
    // {"contract":"GOLDENCAT_USDT","size":-50000000,"iceberg":0,"price":"0.0000007295","tif":"gtc","text":"t-my-custom-id","stp_act":"-"}
    // TODO: workout another exchanges for such type of things
    // TODO: fix next thing
    // gateio: for usdt=54 price=0.0000009 contracts=60000000 in api must be
    // sent 6
    // https://prnt.sc/k00XErVQdmvu
    // https://prnt.sc/agmJhpQquRgD
    double usdt_to_use = 45.0;
    double price = 0.0000007295;
    // 0.0000007295;
    EXPECT_EQ(1, 2);
    // TODO: then run test with debug fn
    // TODO: also track this for tokens where quanto_multiplier is less than 0
    // TODO: do here test like wait 25 ticks and close order and mark sums on
    // balances
}

// TODO: add logic here and trade something for 45$
// TODO: think on improvin speed - open order just on message receive and
// look on prices through http requests
// TODO: write code to ahndle whole strategy + then commit code
// TODO: keep in PrivateClient just one order and one position without vector
// TODO: think on do the same with on-chain exchanges + join private chat "миша
// флипает"
// TODO: think how to abuse mms algorithms
