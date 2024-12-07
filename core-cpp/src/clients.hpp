#include <WebSocketClient.h>

#include <iostream>

#include "models.hpp"

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

class ClientPrivateMexc : public ClientPublic {
   public:
    ClientPrivateMexc(std::string kind);

    void init_idle_private(std::string& listen_key);

    void subscribe_to_private_events();

    void place_spot_limit_order();

    static void place_fut_limit_order();

    std::string create_listen_key();

   private:
    std::string api_key;
    std::string api_secret;
    std::string sign_str(std::string& query);
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
    std::vector<Depth> parse_depth_from_res(std::string s);
};
