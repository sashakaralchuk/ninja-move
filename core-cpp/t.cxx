#include <clickhouse/client.h>
#include <curl/curl.h>
#include <libwebsockets.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "tt/SqrtLibrary/mysqrt.h"
#include "tt/hello/hello.hpp"

using namespace clickhouse;

void do_req();
void parse_json();
void listen_binance_tickers();
void do_clickhouse_req();
void run_step_1();

int main() {
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
        char *t = std::getenv("RUN_STEP_1");
        if (t != NULL && strcmp(t, "1") == 0) {
            run_step_1();
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

static int callback(struct lws *wsi, enum lws_callback_reasons reason,
                    void *user, void *in, size_t len) {
    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            lws_callback_on_writable(wsi);
            break;
        case LWS_CALLBACK_CLIENT_RECEIVE: {
            std::string o = std::string((char *)in);
            std::cout << "LWS_CALLBACK_CLIENT_RECEIVE: " << o << std::endl;
            break;
        }
        default:
            break;
    }

    return 0;
}

static struct lws_protocols protocols[] = {
    {"", callback, 0, 65536}, {NULL, NULL, 0, 0} /* terminator */
};
static struct lws *web_socket = NULL;

/// could be usefull https://github.com/binance-exchange/binacpp
void listen_binance_tickers() {
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.gid = -1;
    info.uid = -1;
    info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    struct lws_context *context = lws_create_context(&info);
    time_t old = 0;
    while (1) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        if (!web_socket && tv.tv_sec != old) {
            struct lws_client_connect_info ccinfo = {0};
            memset(&ccinfo, 0, sizeof(ccinfo));
            ccinfo.context = context;
            ccinfo.address = "stream.binance.com";
            ccinfo.port = 443;
            ccinfo.path = "/stream?streams=btcusdt@ticker";
            ccinfo.host = lws_canonical_hostname(context);
            ccinfo.origin = "origin";
            ccinfo.protocol = protocols[0].name;
            ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED |
                                    LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
            web_socket = lws_client_connect_via_info(&ccinfo);
        }
        if (tv.tv_sec != old) {
            lws_callback_on_writable(web_socket);
            old = tv.tv_sec;
        }
        lws_service(context, 250);
    }
    lws_context_destroy(context);
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

static bool message_sent = false;

static int callback_listen_gateio_spot_tickers(struct lws *wsi,
                                               enum lws_callback_reasons reason,
                                               void *user, void *in,
                                               size_t len) {
    std::cout << reason << std::endl;
    switch (reason) {
        case LWS_CALLBACK_CLIENT_WRITEABLE: {
            if (!message_sent) {
                char *t =
                    "{\"time\": 1731778195, \"channel\": \"spot.tickers\", "
                    "\"event\": \"subscribe\", \"payload\": [\"BTC_USDT\"]}";
                int t_len = std::strlen(t);
                char buf[LWS_PRE + t_len];
                lws_strncpy(&buf[LWS_PRE], t, t_len);
                unsigned char *buf_t = (unsigned char *)buf;
                lws_write(web_socket, &buf_t[LWS_PRE], t_len, LWS_WRITE_TEXT);
                message_sent = true;
                std::cout << "message sent" << std::endl;
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            lws_callback_on_writable(wsi);
            break;
        case LWS_CALLBACK_CLIENT_RECEIVE: {
            std::string o = std::string((char *)in);
            std::cout << "LWS_CALLBACK_CLIENT_RECEIVE: " << o << std::endl;
            break;
        }
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            fprintf(stderr, "Connection error: %s\n", (char *)in);
            break;
        default:
            break;
    }

    return 0;
}

static struct lws_protocols protocols_gateio[] = {
    {"", callback_listen_gateio_spot_tickers, 0, 65536},
    {NULL, NULL, 0, 0} /* terminator */
};

void listen_gateio_spot_tickers() {
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols_gateio;
    info.gid = -1;
    info.uid = -1;
    info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    struct lws_context *context = lws_create_context(&info);
    time_t old = 0;
    while (1) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        if (!web_socket && tv.tv_sec != old) {
            struct lws_client_connect_info ccinfo = {0};
            memset(&ccinfo, 0, sizeof(ccinfo));
            ccinfo.context = context;
            ccinfo.address = "api.gateio.ws";
            ccinfo.port = 443;
            ccinfo.path = "/ws/v4/";
            ccinfo.host = lws_canonical_hostname(context);
            ccinfo.origin = "origin";
            ccinfo.protocol = protocols_gateio[0].name;
            ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED |
                                    LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
            ccinfo.userdata = NULL;
            web_socket = lws_client_connect_via_info(&ccinfo);
        }
        if (tv.tv_sec != old) {
            lws_callback_on_writable(web_socket);
            old = tv.tv_sec;
        }
        lws_service(context, 250);
    }
    lws_context_destroy(context);
}

void run_step_1() { listen_gateio_spot_tickers(); }
