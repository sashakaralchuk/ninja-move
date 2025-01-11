#ifndef SRC_MODELS_
#define SRC_MODELS_

#include <gmpxx.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <map>

struct TradeInt {
    std::string ex;
    std::string s;
    std::string k;
    long ts;
    double p;
    double v;
    std::string toString() const {
        return "{ex=" + ex + ",s=" + s + ",k=" + k +
               ",ts=" + std::to_string(ts) + ",p=" + std::to_string(p) +
               ",v=" + std::to_string(v) + "}";
    }
    static TradeInt new_(std::string ex, std::string s, std::string k, long ts,
                         double p, double v) {
        long threshold = (long)365 * 24 * 60 * 60 * 1000;
        if (ts < threshold) {
            throw std::runtime_error("invalid ts=" + std::to_string(ts));
        }
        return TradeInt{ex, s, k, ts, p, v};
    }
};

struct Depth {
    long u;
    long ex_ts_millis;
    std::string ex;
    std::string k;
    std::string s;
    std::vector<std::tuple<std::string, double>> asks;
    std::vector<std::tuple<std::string, double>> bids;
};

class OrderBookCache {
   public:
    OrderBookCache();

    void apply_orders(long u,
                      std::vector<std::tuple<std::string, double>> asks_in,
                      std::vector<std::tuple<std::string, double>> bids_in);

    void apply_orders_force(
        long u, std::vector<std::tuple<std::string, double>> asks_in,
        std::vector<std::tuple<std::string, double>> bids_in);

    void print();

    void print(int rows_to_print);

    mpf_class get_top_bid();

    mpf_class get_bottom_ask();

    long get_last_update_id();

    void clear();

   private:
    long last_update_id = 0;
    std::map<std::string, double> asks;
    std::map<std::string, double> bids;
};

struct Order {
    std::string ex;
    std::string k;
    std::string id;
    long open_ts;
    std::string s;
    std::string p;
    double p_avg_fill;
    double v;
    double filled_amount;
    std::string st;
    double fee_usdt;
    std::string toString() const {
        return fmt::format(
            "{{ex={},k={},id={},open_ts={},s={},p={},p_avg_fill={},v={},st={},"
            "filled_amount={},fee_usdt={}}}",
            ex, k, id, open_ts, s, p, p_avg_fill, v, st, filled_amount,
            fee_usdt);
    }
    bool isFilled() { return st == "FILLED"; }
};

struct Ticker {
    std::string s;
    double ask;
    double bid;
    long ts;
    std::string toString() const {
        return fmt::format("Ticker{{s={},ask={},bid={},ts={}}}", s, ask, bid,
                           ts);
    }
};

long now_millis();

std::string time_point_to_str(std::chrono::system_clock::time_point tp,
                              std::string format_str = "%F %T %Z");

#endif  // SRC_MODELS_
