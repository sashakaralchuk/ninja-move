#include <map>
#include <string>

#include "models.hpp"

struct TickerSpread {
    Ticker t_raw;
    std::string ex;
    std::string k;
    std::string base;
    std::string quote;
    std::string ccid;
};

class SpreadsMap {
   public:
    void init_ccid_map_from_clickhouse();
    void insert(TickerSpread& t);
    std::optional<std::tuple<TickerSpread, TickerSpread>> find_spreads(
        std::string ccid);
    std::vector<std::tuple<TickerSpread, TickerSpread>> find_spreads_all();

   protected:
    std::map<std::tuple<std::string, std::string, std::string, std::string>,
             std::string>
        ex_k_b_t_ccid;
    std::map<std::string,
             std::map<std::string, std::map<std::string, TickerSpread>>>
        ccid_ex_k_t;
};
