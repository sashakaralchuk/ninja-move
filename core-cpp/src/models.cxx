#include "models.hpp"

#include <spdlog/spdlog.h>

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

OrderBookCache::OrderBookCache() {}

void OrderBookCache::apply_orders(
    long u, std::vector<std::tuple<double, double>> asks_in,
    std::vector<std::tuple<double, double>> bids_in) {
    // TODO: implement for coins with price 0.0000000000001 (store 18 signs)
    // NOTE: to implement without hashmap: store 2 lists with prices / amounts
    if (last_update_id != 0 && u != last_update_id + 1) {
        spdlog::warn("unexpected u={} last_update_id={}", u, last_update_id);
        return;
    }
    for (auto& [price, val] : asks_in) {
        asks[std::to_string(price)] = val;
        if (val == 0) {
            asks.erase(std::to_string(price));
        }
    }
    for (auto& [price, val] : bids_in) {
        bids[std::to_string(price)] = val;
        if (val == 0) {
            bids.erase(std::to_string(price));
        }
    }
    last_update_id = u;
}

void OrderBookCache::print() {
    std::vector<std::string> asks_l;
    for (auto [k, _] : asks) {
        asks_l.push_back(k);
    }
    std::vector<std::string> bids_l;
    for (auto [k, _] : bids) {
        bids_l.push_back(k);
    }
    sort(asks_l.begin(), asks_l.end(), std::greater<>());
    sort(bids_l.begin(), bids_l.end(), std::greater<std::string>());
    std::string shift = " ";
    for (int i = 0; i < bids_l[0].length(); i++) {
        shift += " ";
    }
    for (auto p : asks_l) {
        std::cout << shift << p << std::endl;
    }
    for (auto p : bids_l) {
        std::cout << p << std::endl;
    }
}

double OrderBookCache::get_top_bid() {
    double max = -INFINITY;
    for (auto [k, _] : bids) {
        max = std::max(max, std::stod(k));
    }
    return max;
}

double OrderBookCache::get_bottom_ask() {
    double min = INFINITY;
    for (auto [k, _] : asks) {
        min = std::min(min, std::stod(k));
    }
    return min;
}
