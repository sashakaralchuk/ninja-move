#include <WebSocketClient.h>

#include <iostream>

#include "models.hpp"

std::string conv_to_dec_str_v2(double price, std::string tick_size);

std::string gen_precision_str(int precision);

struct Order {
    std::string ex;
    std::string k;
    std::string id;
    long open_ts;
    std::string s;
    std::string p;
    double v;
    double filled_amount;
    std::string st;
    std::string toString() const {
        return fmt::format(
            "{{ex={},k={},id={},open_ts={},s={},p={},v={},st={},"
            "filled_amount={}}}",
            ex, k, id, open_ts, s, p, v, st, filled_amount);
    }
    bool isFilled() { return st == "FILLED"; }
};

class ClientPublic : public hv::WebSocketClient {
   public:
    bool ws_onopen_received;
    bool ws_onclose_received;
    std::string ex;
    std::string kind;
    OrderBookCache order_book_cache;
    std::function<void(const TradeInt trade_int)> onmessage_trade;
    std::function<void(const Depth depth)> onmessage_depth;

    ClientPublic(std::string ex_, std::string kind_,
                 hv::EventLoopPtr loop = NULL);
    ~ClientPublic();

    virtual void init_idle() = 0;
    virtual void subscribe_to_trades(std::string& symbol) = 0;
    virtual void subscribe_to_depth(std::string& symbol) = 0;
    virtual void unsubscribe_from_depth(std::string& symbol) = 0;
    virtual void ping() = 0;

   protected:
    virtual void handle_onmessage(const std::string& msg) = 0;
    void init_idle_(std::string& url);
};

class ClientPrivate : public hv::WebSocketClient {
   public:
    bool ws_onopen_received;
    bool ws_onclose_received;
    bool is_subscribed_to_private_channels;
    std::string ex;
    std::string kind;
    std::function<void()> onmessage_private_event;

    ClientPrivate(std::string ex_, std::string kind_,
                  hv::EventLoopPtr loop = NULL);
    ~ClientPrivate();

    virtual void init_idle() = 0;
    virtual void subscribe_to_private_events() = 0;
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity) = 0;
    virtual void place_fut_limit_order(std::string symbol, std::string side,
                                       std::string price,
                                       std::string quantity) = 0;
    virtual void place_spot_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity) = 0;
    Order get_last_order();
    int get_orders_len();
    void clear_orders();

   protected:
    std::optional<nlohmann::json> fut_exchange_info;
    std::optional<nlohmann::json> spot_exchange_info;
    std::vector<Order> orders;
    virtual void handle_onmessage(const std::string& msg) = 0;
    void init_idle_(std::string& url);
};

class ClientPublicGateio : public ClientPublic {
   public:
    ClientPublicGateio(std::string kind);

    void init_idle();

    void subscribe_to_trades(std::string& symbol);

    void subscribe_to_depth(std::string& symbol);

    void unsubscribe_from_depth(std::string& symbol);

    void ping();

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol);

   protected:
    void handle_onmessage(const std::string& msg);
};

class ClientPrivateGateio : public ClientPrivate {
   public:
    ClientPrivateGateio(std::string kind);

    virtual void init_idle();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual void place_fut_limit_order(std::string symbol, std::string side,
                                       std::string price, std::string quantity);
    virtual void place_spot_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string method, std::string t, std::string url,
                         std::string query_str, std::string payload_str);
};

class ClientPublicMexc : public ClientPublic {
   public:
    ClientPublicMexc(std::string kind);

    void init_idle();

    void subscribe_to_trades(std::string& symbol);

    void subscribe_to_depth(std::string& symbol);

    void unsubscribe_from_depth(std::string& symbol);

    void ping();

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    bool subscribed_to_depth = false;
};

class ClientPrivateMexc : public ClientPrivate {
   public:
    ClientPrivateMexc(std::string kind);

    virtual void init_idle();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual void place_fut_limit_order(std::string symbol, std::string side,
                                       std::string price, std::string quantity);
    virtual void place_spot_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string& query);
    std::string create_listen_key();
};

class ClientPublicBybit : public ClientPublic {
   public:
    ClientPublicBybit(std::string kind);

    void init_idle();

    void subscribe_to_trades(std::string& symbol);

    void subscribe_to_depth(std::string& symbol);

    void unsubscribe_from_depth(std::string& symbol);

    void ping();

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    Depth parse_depth(nlohmann::json& msg_obj);
};

class ClientPrivateBybit : public ClientPrivate {
   public:
    ClientPrivateBybit(std::string kind);

    virtual void init_idle();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual void place_fut_limit_order(std::string symbol, std::string side,
                                       std::string price, std::string quantity);
    virtual void place_spot_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string qs0, std::string payload0);
};

class ClientPublicHtx : public ClientPublic {
   public:
    ClientPublicHtx(std::string kind);

    void init_idle();

    void subscribe_to_trades(std::string& symbol);

    void subscribe_to_depth(std::string& symbol);

    void unsubscribe_from_depth(std::string& symbol);

    void ping();

    static std::string parse_symbol_from_trade_ch(std::string& ch);

    static std::string parse_symbol_from_depth_ch(std::string& ch);

    static int gzDecompress(const char* src, int srcLen, const char* dst,
                            int dstLen);

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::vector<Depth> parse_depth_from_res(nlohmann::json obj);
};

class ClientPrivateHtx : public ClientPrivate {
   public:
    ClientPrivateHtx(std::string kind);

    virtual void init_idle();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual void place_fut_limit_order(std::string symbol, std::string side,
                                       std::string price, std::string quantity);
    virtual void place_spot_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string qs0, std::string payload0);
};
