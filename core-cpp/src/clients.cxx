#include "clients.hpp"

#include <spdlog/spdlog.h>

ClientPublic::ClientPublic(std::string ex_, std::string kind_,
                           hv::EventLoopPtr loop)
    : WebSocketClient(loop) {
    ex = ex_;
    kind = kind_;
}

ClientPublic::~ClientPublic() {}

void ClientPublic::init_idle_(std::string& url) {
    ws_onopen_received = false;
    ws_onclose_received = false;
    onopen = [&]() {
        ws_onopen_received = true;
        spdlog::info("{} onopen kind={}", ex, kind);
    };
    onclose = [&]() {
        spdlog::info("{} onclose kind={}", ex, kind);
        ws_onclose_received = true;
    };
    onmessage = [=](const std::string& msg) { handle_onmessage(msg); };
    setPingInterval(10000);
    reconn_setting_t reconn;
    reconn_setting_init(&reconn);
    reconn.min_delay = 100;
    reconn.max_delay = 1000;
    reconn.delay_policy = 2;
    setReconnect(&reconn);
    http_headers headers;
    open(url.c_str(), headers);
}
