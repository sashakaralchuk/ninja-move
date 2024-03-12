fn main() {
    let _ = log4rs::init_file("log4rs.yml", Default::default()).unwrap();
    let target = "step";
    log::debug!(target: target, "debug");
    log::info!(target: target, "info");
    log::warn!(target: target, "warn");
    log::error!(target: target, "error");
    log::trace!(target: target, "trace");
    let n = 10_i32.pow(6);
    for i in 1..n {
        log::info!(target: "step", "info message lala {}", i);
    }
}
