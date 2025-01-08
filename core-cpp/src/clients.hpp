#include <WebSocketClient.h>
#include <curl/curl.h>

#include <iostream>

#include "models.hpp"

std::string conv_to_dec_str_v2(double price, std::string tick_size);

std::string gen_precision_str(int precision);

std::string rstrip_zeros(std::string s);

long now_millis();

std::string time_point_to_str(std::chrono::system_clock::time_point tp,
                              std::string format_str = "%F %T %Z");

std::tuple<std::string, double> conv_symbol_price_to_atomic_v1(
    std::string symbol, double price);

nlohmann::json exec_http_get_req(std::string& url,
                                 curl_slist* headers = nullptr,
                                 bool log_res = false);

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
    virtual Ticker fetch_ticker(std::string& symbol) = 0;
    virtual std::vector<Ticker> fetch_tickers() = 0;

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
    virtual void init_exchange_info() = 0;
    virtual void subscribe_to_private_events() = 0;
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity) = 0;
    virtual Order place_fut_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity) = 0;
    virtual Order amend_fut_order(Order& order, std::string price) = 0;
    virtual void set_leverage_to_1(std::string symbol) = 0;
    virtual Order place_spot_limit_order(std::string symbol, std::string side,
                                         std::string price,
                                         std::string quantity) = 0;
    virtual Order fetch_order(Order& order) = 0;
    virtual double fetch_order_fee_usdt(Order& order) = 0;
    virtual std::map<std::string, double> fetch_balances() = 0;
    Order get_last_order();
    int get_orders_len();
    void clear_orders();
    std::string conv_size_to_str(double size);
    nlohmann::json get_exchange_info();

   protected:
    std::optional<nlohmann::json> fut_exchange_info;
    std::optional<nlohmann::json> spot_exchange_info;
    // XXX: keep in ClientPrivate just one order for (buy, sell) for (fut, spot)
    // without vector inside
    std::vector<Order> orders;
    // NOTE: how to handle situation when you already sent order amend message
    // and than order fills (race condition), you have to do this in one place
    // based on (is-update-sent, timestamp), also you have to handle all
    // possible states like amend declined, amend success etc
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
    Ticker fetch_ticker(std::string& symbol);
    std::vector<Ticker> fetch_tickers();

    std::vector<Depth> fetch_depth_snapshot(std::string& symbol);

   protected:
    void handle_onmessage(const std::string& msg);
};

class ClientPrivateGateio : public ClientPrivate {
   public:
    ClientPrivateGateio(std::string kind);

    virtual void init_idle();
    virtual void init_exchange_info();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual Order place_fut_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);
    virtual void set_leverage_to_1(std::string symbol);
    virtual Order amend_fut_order(Order& order, std::string price);
    virtual Order place_spot_limit_order(std::string symbol, std::string side,
                                         std::string price,
                                         std::string quantity);
    virtual Order fetch_order(Order& order);
    virtual double fetch_order_fee_usdt(Order& order);
    virtual std::map<std::string, double> fetch_balances();

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
    Ticker fetch_ticker(std::string& symbol);
    std::vector<Ticker> fetch_tickers();
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
    virtual void init_exchange_info();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual Order place_fut_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);
    virtual void set_leverage_to_1(std::string symbol);
    virtual Order amend_fut_order(Order& order, std::string price);
    virtual Order place_spot_limit_order(std::string symbol, std::string side,
                                         std::string price,
                                         std::string quantity);
    virtual Order fetch_order(Order& order);
    virtual double fetch_order_fee_usdt(Order& order);
    virtual std::map<std::string, double> fetch_balances();

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
    Ticker fetch_ticker(std::string& symbol);
    std::vector<Ticker> fetch_tickers();

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    Depth parse_depth(nlohmann::json& msg_obj);
};

class ClientPrivateBybit : public ClientPrivate {
   public:
    ClientPrivateBybit(std::string kind);

    virtual void init_idle();
    virtual void init_exchange_info();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual Order place_fut_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);
    virtual void set_leverage_to_1(std::string symbol);
    virtual Order amend_fut_order(Order& order, std::string price);
    virtual Order place_spot_limit_order(std::string symbol, std::string side,
                                         std::string price,
                                         std::string quantity);
    virtual Order fetch_order(Order& order);
    virtual double fetch_order_fee_usdt(Order& order);
    virtual std::map<std::string, double> fetch_balances();

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
    Ticker fetch_ticker(std::string& symbol);
    std::vector<Ticker> fetch_tickers();
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
    virtual void init_exchange_info();
    virtual void subscribe_to_private_events();
    virtual std::tuple<std::string, std::string> adjust_price_quantity(
        std::string symbol, double price, double quantity);
    virtual Order place_fut_limit_order(std::string symbol, std::string side,
                                        std::string price,
                                        std::string quantity);
    virtual void set_leverage_to_1(std::string symbol);
    virtual Order amend_fut_order(Order& order, std::string price);
    virtual Order place_spot_limit_order(std::string symbol, std::string side,
                                         std::string price,
                                         std::string quantity);
    virtual Order fetch_order(Order& order);
    virtual double fetch_order_fee_usdt(Order& order);
    virtual std::map<std::string, double> fetch_balances();

   protected:
    void handle_onmessage(const std::string& msg);

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string qs0, std::string payload0);
};

class TelegramBotPort {
   public:
    TelegramBotPort(std::string token_, std::string chat_id_);
    static TelegramBotPort new_from_envs();
    void notify_pretty(std::string message, std::string action);

   private:
    std::string token;
    std::string chat_id;
};
