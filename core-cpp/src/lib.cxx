#include "lib.hxx"

#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>

void configure_logger() {
    auto level = spdlog::level::from_str(std::getenv("SPDLOG_LEVEL"));
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(
        std::make_shared<spdlog::sinks::ansicolor_stdout_sink_st>());
    sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_st>(
        ".var/logs/logfile", 0, 0));
    for (auto& s : sinks) {
        s->set_level(level);
        s->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [t=%t] [%l] [%s %! %#] %v");
    }
    auto l = std::make_shared<spdlog::logger>("t", begin(sinks), end(sinks));
    l->set_level(level);
    spdlog::set_default_logger(l);
}