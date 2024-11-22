#include "models.hpp"

#include <gmpxx.h>
#include <gtest/gtest.h>
#include <string.h>

#include <iomanip>

TEST(ModelsTest, TradeIntNewMillisThreshold) {
    long threshold = (long)365 * 24 * 60 * 60 * 1000;
    try {
        TradeInt::new_("", "", "", threshold - 1, 0.0, 0.0);
        EXPECT_EQ(0, 1);
    } catch (...) {
    }
    try {
        TradeInt::new_("", "", "", threshold, 0.0, 0.0);
    } catch (...) {
        EXPECT_EQ(0, 1);
    }
    try {
        TradeInt::new_("", "", "", threshold + 1, 0.0, 0.0);
    } catch (...) {
        EXPECT_EQ(0, 1);
    }
}

TEST(ModelsTest, OrderBookCacheDefaultBehaviour) {
    OrderBookCache cache1;
    cache1.apply_orders(1, {{"3.0", 1.0}, {"4.0", 2.0}},
                        {{"1.0", 3.0}, {"2.0", 4.0}});
    EXPECT_TRUE(cache1.get_bottom_ask() == mpf_class("3.0"));
    EXPECT_TRUE(cache1.get_top_bid() == mpf_class("2.0"));
    OrderBookCache cache2;
    cache2.apply_orders(
        1, {{"0.000000000000000003", 1.0}, {"0.000000000000000004", 2.0}},
        {{"0.000000000000000001", 3.0}, {"0.000000000000000002", 4.0}});
    EXPECT_TRUE(cache2.get_bottom_ask() == mpf_class("0.000000000000000003"));
    EXPECT_TRUE(cache2.get_top_bid() == mpf_class("0.000000000000000002"));
}
