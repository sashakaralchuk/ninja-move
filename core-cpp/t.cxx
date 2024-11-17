#include <clickhouse/client.h>
#include <curl/curl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "WebSocketClient.h"
#include "tt/SqrtLibrary/mysqrt.h"
#include "tt/hello/hello.hpp"

using namespace clickhouse;

void do_req();
void parse_json();
void listen_binance_tickers();
void do_clickhouse_req();
void listen_gatio_tickers_v2();

void hallow_from_t2();

int main() {
    hallow_from_t2();
    std::cout << "out: " << mathfunctions::detail::mysqrt_f(4) << std::endl;
    printf("Hello, World! 2\n");
    hello::say_hello();
    do_req();
    parse_json();
    do_clickhouse_req();
    {
        char *t = std::getenv("LISTEN_BINANCE_TICKERS");
        if (t != NULL && strcmp(t, "1") == 0) {
            listen_binance_tickers();
        }
    }
    {
        char *t = std::getenv("LISTEN_GATEIO_TICKERS_V2");
        if (t != NULL && strcmp(t, "1") == 0) {
            listen_gatio_tickers_v2();
        }
    }
    return 0;
}

static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                            void *userp) {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
}

void do_req() {
    CURL *curl;
    CURLcode res;
    std::string readBuffer;
    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL,
                         "https://jsonplaceholder.typicode.com/todos/1");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        std::cout << readBuffer << std::endl;
    } else {
        printf("curl_easy_init() failed\n");
    }
}

void parse_json() {
    auto j3 = nlohmann::json::parse(R"({"happy": true, "pi": 3.141})");
    double t = j3["pi"];
    std::cout << "parse_json: " << t << std::endl;
}

void do_clickhouse_req() {
    std::string query =
        "select count() from default.trade_contango_arbitrage_v1_diff_tracks";
    Client client(ClientOptions().SetHost("127.0.0.1").SetPort(9000));
    unsigned int reqsAmount = 0;
    client.Select(
        "SELECT count() FROM default.trade_contango_arbitrage_v1_diff_tracks",
        [&reqsAmount](const Block &block) {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                reqsAmount = block[0]->As<ColumnUInt64>()->At(i);
            }
        });
    std::cout << "do_clickhouse_req: " << reqsAmount << std::endl;
}

void listen_binance_tickers() {
    hv::WebSocketClient ws;
    ws.onopen = []() { printf("onopen\n"); };
    ws.onmessage = [](const std::string &msg) {
        printf("onmessage: %.*s\n", (int)msg.size(), msg.data());
    };
    ws.onclose = []() { printf("onclose\n"); };
    reconn_setting_t reconn;
    reconn_setting_init(&reconn);
    reconn.min_delay = 1000;
    reconn.max_delay = 10000;
    reconn.delay_policy = 2;
    ws.setReconnect(&reconn);
    const char *url = "wss://stream.binance.com/stream?streams=btcusdt@ticker";
    ws.open(url);
    std::string str;
    while (std::getline(std::cin, str)) {
        if (!ws.isConnected()) break;
        if (str == "quit") {
            ws.close();
            break;
        }
        ws.send(str);
    }
}

void listen_gatio_tickers_v2() {
    hv::WebSocketClient ws;
    ws.onopen = [&ws]() {
        printf("onopen\n");
        char *t =
            "{\"time\": 1731778195, \"channel\": \"spot.tickers\", "
            "\"event\": \"subscribe\", \"payload\": [\"BTC_USDT\"]}";
        ws.send(t);
    };
    ws.onmessage = [](const std::string &msg) {
        printf("onmessage: %.*s\n", (int)msg.size(), msg.data());
    };
    ws.onclose = []() { printf("onclose\n"); };
    reconn_setting_t reconn;
    reconn_setting_init(&reconn);
    reconn.min_delay = 1000;
    reconn.max_delay = 10000;
    reconn.delay_policy = 2;
    ws.setReconnect(&reconn);
    const char *url = "wss://api.gateio.ws/ws/v4/";
    ws.open(url);
    std::string str;
    while (std::getline(std::cin, str)) {
        if (!ws.isConnected()) break;
        if (str == "quit") {
            ws.close();
            break;
        }
        ws.send(str);
    }
}
