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
