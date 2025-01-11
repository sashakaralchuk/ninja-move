#include "models.hxx"

#include <gmpxx.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>

// NOTE: to implement without hashmap: store 2 lists with prices/amounts
OrderBookCache::OrderBookCache() {}

void OrderBookCache::apply_orders(
    long u, std::vector<std::tuple<std::string, double>> asks_in,
    std::vector<std::tuple<std::string, double>> bids_in) {
    if (last_update_id != 0 && u != last_update_id + 1) {
        throw std::runtime_error(
            "unexpected u=" + std::to_string(u) +
            " last_update_id=" + std::to_string(last_update_id));
        return;
    }
    apply_orders_force(u, asks_in, bids_in);
}

void OrderBookCache::apply_orders_force(
    long u, std::vector<std::tuple<std::string, double>> asks_in,
    std::vector<std::tuple<std::string, double>> bids_in) {
    for (auto& [p, v] : asks_in) {
        asks[p] = v;
        if (v == 0) {
            asks.erase(p);
        }
    }
    for (auto& [p, v] : bids_in) {
        bids[p] = v;
        if (v == 0) {
            bids.erase(p);
        }
    }
    last_update_id = u;
}

void OrderBookCache::print() { print(10); }

void OrderBookCache::print(int rows_to_print) {
    std::vector<std::tuple<mpf_class, std::string, std::string>> asks_l;
    for (auto [k, v] : asks) {
        char price_str[32];
        char vol_str[32];
        snprintf(price_str, sizeof(price_str), "%.12f", stod(k));
        snprintf(vol_str, sizeof(vol_str), "%.12f", v);
        asks_l.push_back(
            {mpf_class(k, 18), std::string(price_str), std::string(vol_str)});
    }
    std::vector<std::tuple<mpf_class, std::string, std::string>> bids_l;
    for (auto [k, v] : bids) {
        char price_str[32];
        char vol_str[32];
        snprintf(price_str, sizeof(price_str), "%.12f", stod(k));
        snprintf(vol_str, sizeof(vol_str), "%.12f", v);
        bids_l.push_back(
            {mpf_class(k, 18), std::string(price_str), std::string(vol_str)});
    }
    sort(asks_l.begin(), asks_l.end(), [](const auto& a, const auto& b) {
        return std::get<0>(a) > std::get<0>(b);
    });
    sort(bids_l.begin(), bids_l.end(), [](const auto& a, const auto& b) {
        return std::get<0>(a) > std::get<0>(b);
    });
    size_t max_len_price = 0;
    size_t max_len_vol = 0;
    size_t offset_asks = std::min((size_t)rows_to_print, asks_l.size());
    size_t offset_bids = std::min(rows_to_print, (int)bids_l.size());
    for (auto i = asks_l.end() - offset_asks; i != asks_l.end(); i++) {
        auto [p, price_str, vol_str] = (*i);
        max_len_price = std::max(max_len_price, price_str.length());
        max_len_vol = std::max(max_len_vol, vol_str.length());
    }
    for (auto i = bids_l.begin(); i != bids_l.begin() + offset_bids; i++) {
        auto [p, price_str, vol_str] = (*i);
        max_len_price = std::max(max_len_price, price_str.length());
        max_len_vol = std::max(max_len_vol, vol_str.length());
    }
    for (auto i = asks_l.end() - offset_asks; i != asks_l.end(); i++) {
        auto [p, price_str, vol_str] = (*i);
        for (int i = 0; i < max_len_vol + max_len_price; i++) {
            std::cout << " ";
        }
        for (int i = 0; i < max_len_price - price_str.length(); i++) {
            std::cout << " ";
        }
        std::cout << "  " << price_str << " ";
        for (int i = 0; i < max_len_vol - vol_str.length(); i++) {
            std::cout << " ";
        }
        std::cout << vol_str << std::endl;
    }
    for (auto i = bids_l.begin(); i != bids_l.begin() + offset_bids; i++) {
        auto [p, price_str, vol_str] = (*i);
        for (int i = 0; i < max_len_vol - vol_str.length(); i++) {
            std::cout << " ";
        }
        std::cout << vol_str << " ";
        for (int i = 0; i < max_len_price - price_str.length(); i++) {
            std::cout << " ";
        }
        std::cout << price_str << std::endl;
    }
}

mpf_class OrderBookCache::get_top_bid() {
    mpf_class max = mpf_class(-1000000000, 18);
    for (auto [k, _] : bids) {
        max = std::max(max, mpf_class(k, 18));
    }
    return max;
}

mpf_class OrderBookCache::get_bottom_ask() {
    mpf_class min = mpf_class(1000000000, 18);
    for (auto [k, _] : asks) {
        min = std::min(min, mpf_class(k, 18));
    }
    return min;
}

long OrderBookCache::get_last_update_id() { return last_update_id; }

void OrderBookCache::clear() {
    last_update_id = 0;
    asks.clear();
    bids.clear();
}

long now_millis() {
    return std::chrono::system_clock::now().time_since_epoch().count() / 1000;
}

std::string time_point_to_str(std::chrono::system_clock::time_point tp,
                              std::string format_str) {
    std::time_t now_raw = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream now_oss;
    now_oss << std::put_time(std::gmtime(&now_raw), format_str.c_str());
    return now_oss.str();
}
