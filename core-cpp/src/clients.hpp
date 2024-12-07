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