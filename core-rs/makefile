env_vars=${shell cat .env | grep -v "\#" | xargs}
commit_hash=${shell git rev-parse --short HEAD}

monitor-v1:
	${env_vars} cargo run --bin monitor -- --command v1 --write

monitor-v2:
	${env_vars} cargo run --bin monitor -- --command v2

generate-mat-views:
	${env_vars} cargo run --bin monitor -- \
		--command generate-mat-views \
		--write

trade-limit-market:
	${env_vars} cargo run --bin limit_market_v1

run-two-top-instersects-trade:
	COMMIT_HASH_STR=${commit_hash} \
	${env_vars} \
	cargo run --bin two_top_intersects -- --command trade

run-two-top-instersects-create-tables:
	${env_vars} cargo run --bin two_top_intersects -- --command create-tables

run-two-top-instersects-listen-save-candles:
	${env_vars} cargo run --bin two_top_intersects -- --command listen-save-candles

run-two-top-instersects-listen-save-spreads:
	${env_vars} cargo run --bin two_top_intersects -- --command listen-save-spreads

run-example-place-bybit-market-order:
	${env_vars} cargo run --example place_bybit_market_order

test-2top-instersects:
	cargo test --bin two_top_intersects

test-bybit-domain:
	${env_vars} cargo test domain::bybit::tests

run-trade-emas:
	${env_vars} cargo run --bin trade_emas -- --command draw-graph

test:
	${env_vars} cargo test

use-logger:
	cargo run --example use_logger

bench-time-libs:
	cargo bench --bench time_millis

run-example-log-dynamic:
	cargo run --example log_dynamic

run-example-log-to-file:
	cargo run --example log_to_file
