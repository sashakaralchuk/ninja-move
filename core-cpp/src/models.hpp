#include <iostream>

struct TradeInt {
    std::string ex;
    std::string s;
    std::string k;
    long ts;
    double p;
    double v;
    static TradeInt new_(std::string ex, std::string s, std::string k, long ts,
                         double p, double v) {
        long threshold = (long)365 * 24 * 60 * 60 * 1000;
        if (ts < threshold) {
            throw std::runtime_error("invalid ts=" + std::to_string(ts));
        }
        return TradeInt{ex, s, k, ts, p, v};
    }
};

std::ostream& operator<<(std::ostream& os, TradeInt const& o) {
    os << "{ex=" << o.ex << ",s=" << o.s << ",k=" << o.k << ",ts=" << o.ts
       << ",p=" << o.p << ",v=" << o.v << "}";
    return os;
}
