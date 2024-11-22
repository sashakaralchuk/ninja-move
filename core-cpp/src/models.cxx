#include "models.hpp"

#include <gmpxx.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>

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
    std::vector<std::tuple<mpf_class, double>> asks_l;
    for (auto [k, v] : asks) {
        asks_l.push_back({mpf_class(k, 18), v});
    }
    std::vector<std::tuple<mpf_class, double>> bids_l;
    for (auto [k, v] : bids) {
        bids_l.push_back({mpf_class(k, 18), v});
    }
    sort(asks_l.begin(), asks_l.end(), [](const auto& a, const auto& b) {
        return std::get<0>(a) > std::get<0>(b);
    });
    sort(bids_l.begin(), bids_l.end(), [](const auto& a, const auto& b) {
        return std::get<0>(a) > std::get<0>(b);
    });
    int shift_len_p =
        std::to_string((int)(std::get<0>(bids_l[0]).get_d())).length() + 1 +
        12 + 1 + 14;
    std::string shift_str = "";
    for (int i = 0; i < shift_len_p; i++) {
        shift_str += " ";
    }
    for (auto i = asks_l.end() - rows_to_print; i != asks_l.end(); i++) {
        auto [p, v] = (*i);
        printf("%s %.12f %.12f\n", shift_str.c_str(), p.get_d(), v);
    }
    for (auto i = bids_l.begin(); i != bids_l.begin() + rows_to_print; i++) {
        auto [p, v] = (*i);
        printf("%.12f %.12f\n", v, p.get_d());
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
