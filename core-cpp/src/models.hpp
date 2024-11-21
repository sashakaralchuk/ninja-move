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

class OrderBookCache {
   public:
    OrderBookCache();

    void apply_orders(long u, std::vector<std::tuple<double, double>> asks_in,
                      std::vector<std::tuple<double, double>> bids_in);

    void print();

    double get_top_bid();

    double get_bottom_ask();

   private:
    long last_update_id = 0;
    std::map<std::string, double> asks;
    std::map<std::string, double> bids;
};
