#include <iostream>
#include <nlohmann/json.hpp>

#include "WebSocketClient.h"
#include "spdlog/cfg/env.h"
#include "spdlog/spdlog.h"

void listen_gateio_tickers();

int main() {
    spdlog::cfg::load_env_levels();
    listen_gateio_tickers();
    return 0;
}

class WSClientGateio : public hv::WebSocketClient {
   public:
    WSClientGateio(hv::EventLoopPtr loop = NULL) : WebSocketClient(loop) {}
    ~WSClientGateio() {}

    void init_spot_idle() {
        onopen = []() { spdlog::info("gateio onopen"); };
        onclose = []() { spdlog::info("gateio onclose"); };
        setPingInterval(10000);
        reconn_setting_t reconn;
        reconn_setting_init(&reconn);
        reconn.min_delay = 100;
        reconn.max_delay = 1000;
        reconn.delay_policy = 2;
        setReconnect(&reconn);
        http_headers headers;
        open("wss://api.gateio.ws:443/ws/v4/", headers);
    }

    void subscribe_to_spot_trades() {
        int ts_secs =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000 /
            1000;
        std::string symbol = "BTC_USDT";
        std::string t_template = R"({
            "time": %d,
            "channel": "spot.trades",
            "event": "subscribe",
            "payload": ["%s"]
        })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), ts_secs, symbol.c_str());
        send(t);
    }
};

class WSClientMexc : public hv::WebSocketClient {
   public:
    WSClientMexc(hv::EventLoopPtr loop = NULL) : WebSocketClient(loop) {}
    ~WSClientMexc() {}

    void init_fut_idle() {
        onopen = []() { spdlog::info("mexc onopen"); };
        onclose = []() { spdlog::info("mexc onclose"); };
        setPingInterval(10000);
        reconn_setting_t reconn;
        reconn_setting_init(&reconn);
        reconn.min_delay = 100;
        reconn.max_delay = 1000;
        reconn.delay_policy = 2;
        setReconnect(&reconn);
        http_headers headers;
        open("wss://contract.mexc.com:443/edge", headers);
    }

    void subscribe_to_fut_trades() {
        std::string symbol = "BTC_USDT";
        std::string t_template = R"({
                "method": "sub.deal",
                "param": {"symbol": "%s"}
            })";
        char t[256];
        snprintf(t, sizeof(t), t_template.c_str(), symbol.c_str());
        send(t);
    }

    void ping() { send(R"({"method": "ping"})"); }
};

void listen_gateio_tickers() {
    std::map<std::string, long> last_timestamps;
    WSClientGateio ws_gateio;
    ws_gateio.init_spot_idle();
    ws_gateio.onmessage = [&last_timestamps](const std::string& msg) {
        nlohmann::json msg_obj = nlohmann::json::parse(msg);
        std::string event = msg_obj["event"];
        if (event == "subscribe") {
            return;
        }
        if (event == "update") {
            last_timestamps["gateio"] = msg_obj["time_ms"];
            return;
        }
        throw std::runtime_error("gateio unexpected event=" + event);
    };
    WSClientMexc ws_mexc;
    ws_mexc.init_fut_idle();
    ws_mexc.onmessage = [&last_timestamps](const std::string& msg) {
        nlohmann::json msg_obj = nlohmann::json::parse(msg);
        std::string channel = msg_obj["channel"];
        if (channel == "rs.sub.deal" || channel == "pong") {
            return;
        }
        if (channel == "push.deal") {
            last_timestamps["mexc"] = msg_obj["ts"];
            return;
        }
        if (channel == "rs.error") {
            spdlog::error("mexc error: {}", msg);
        }
        throw std::runtime_error("mexc unexpected channel=" + channel);
    };
    for (int i = 0; i < 100; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (ws_gateio.isConnected() && ws_mexc.isConnected()) {
            spdlog::info("connections set up");
            break;
        }
    }
    if (!ws_gateio.isConnected() || !ws_mexc.isConnected()) {
        throw std::runtime_error("connections haven't been set up");
    }
    ws_gateio.subscribe_to_spot_trades();
    ws_mexc.subscribe_to_fut_trades();
    std::thread _([&ws_mexc]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            spdlog::info("ping");
            ws_mexc.ping();
        }
    });
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        long now_millis =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000;
        std::string b;
        for (auto o = last_timestamps.cbegin(); o != last_timestamps.cend();
             ++o) {
            b += o->first + ":" + std::to_string(now_millis - o->second) + ",";
        }
        if (!b.empty()) {
            b.pop_back();
        }
        spdlog::info("last_timestamps={}", "{" + b + "}");
        if (!ws_gateio.isConnected() || !ws_mexc.isConnected()) {
            if (!ws_gateio.isConnected()) {
                ws_gateio.close();
            }
            if (!ws_mexc.isConnected()) {
                ws_mexc.close();
            }
            break;
        }
    }
}
