use log;
use log::LevelFilter;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Logger, Root};
use log4rs::Config;


const LOG_PATTERN: &'static str = "{d(%Y-%m-%d %H:%M:%S)} | {({l}):5.5} | {f}:{L} — {m}{n}";


fn _symbol(date: &str, symbol: &str) -> (Appender, Logger) {
    let format_roller = format!(".logs/difference/{}/{}/{{}}", symbol, date);
    let format_default = format!(".logs/difference/{}/{}/default", symbol, date);
    let format_step = format!("step_{}", symbol);
    let format_appender = format!("appender_{}", symbol);
    let trigger_size = byte_unit::n_mb_bytes!(1) as u64;
    let trigger = Box::new(SizeTrigger::new(trigger_size));
    let roller = Box::new(
        FixedWindowRoller::builder()
            .base(0)
            .build(format_roller.as_str(), 5)
            .unwrap(),
    );
    let compound_policy = Box::new(CompoundPolicy::new(trigger, roller));
    let roller_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build(format_default.as_str(), compound_policy)
        .unwrap();
    let appender = Appender::builder()
        .build(format_appender.as_str(), Box::new(roller_appender));
    let logger = Logger::builder()
        .appender(format_appender.as_str())
        .build(format_step.as_str(), LevelFilter::Debug);
    (appender, logger)
}


fn _config(date: &str, symbols: &[&str]) -> Config {
    let mut builder = Config::builder();
    for symbol in symbols {
        let (appender, logger) = _symbol(date, symbol);
        builder = builder.appender(appender);
        builder = builder.logger(logger);
    }
    builder
        .build(Root::builder().build(LevelFilter::Debug))
        .unwrap()
}

fn main() {
    let symbols = ["ETHUSDT", "BNBUSDT"];
    let handle = log4rs::init_config(
        _config("2023-06-12", &symbols),
    ).unwrap();

    let log = |target: &str, i: i32| {
        log::debug!(target: target, "debug {}", i);
        log::info!(target: target, "info");
        log::warn!(target: target, "warn");
        log::error!(target: target, "error");
        log::trace!(target: target, "trace");
    };

    let n1 = 10_i32.pow(6);
    for i in 1..n1 {
        log("step_ETHUSDT", i);
        log("step_BNBUSDT", i);
    }

    handle.set_config(_config("2023-06-13", &symbols));

    let n2 = 10_i32.pow(6);
    for i in 1..n2 {
        log("step_ETHUSDT", i);
        log("step_BNBUSDT", i);
    }
}
