#include "models.hpp"

#include <gtest/gtest.h>

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
    OrderBookCache cache;
    cache.apply_orders(1, {{3.0, 1.0}, {4.0, 2.0}}, {{1.0, 3.0}, {2.0, 4.0}});
    EXPECT_EQ(cache.get_bottom_ask(), 3.0);
    EXPECT_EQ(cache.get_top_bid(), 2.0);
}
