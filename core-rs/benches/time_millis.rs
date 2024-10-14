use std::time::{Instant, SystemTime};

use chrono::Utc;
use criterion::{criterion_group, criterion_main, Criterion};

/// cargo bench --bench time_millis
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Utc:now", |b| b.iter(|| Utc::now()));
    c.bench_function("Instant::now", |b| b.iter(|| Instant::now()));
    c.bench_function("SystemTime::now", |b| b.iter(|| SystemTime::now()));
    c.bench_function("SystemTime::as_millis", |b| {
        b.iter(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
