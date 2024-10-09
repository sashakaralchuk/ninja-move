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

run-trade-emas-collect-bybit-tickers:
	${env_vars} cargo run --bin trade_emas -- --command collect-bybit-tickers

TRADE_EMAS_ENVS := \
	RUST_LOG=info \
	COMMIT_HASH_STR=${commit_hash} \
	TRADE_EMAS_BACKTEST_EXPERIMENT_NAME="experiemnt-trade-emas-2024-10-06" \
	TRADE_EMAS_BACKTEST_SAVE_BACKTESTS_OUTS=1 \
	TRADE_EMAS_BACKTEST_SAVE_DEBUG_CANDLES=1 \
	TRADE_EMAS_BACKTEST_SAVE_DEBUG_EMAS=0 \
	TRADE_EMAS_BACKTEST_TIMESTAMP_START="2024-04-07 00:00:00" \
	TRADE_EMAS_BACKTEST_TIMESTAMP_END="2024-05-15 00:00:00" \
	TRADE_EMAS_BACKTEST_TIMESTAMP_TRADING_START="2024-04-12 18:15:00" \
	TRADE_EMAS_BACKTEST_CANDLE_TIMEFRAME=1h \
	TRADE_EMAS_BACKTEST_EMA_LEN=12 \
	TRADE_EMAS_BACKTEST_STOP_LOSS_REL_VAL=0.005 \
	TRADE_EMAS_BACKTEST_TRAILING_THRESHOLD_REL_VAL=0.5 \
	TRADE_EMAS_BACKTEST_TRAILING_THRESHOLD_MIN_INCR_REL_VAL=0.002 \
	TRADE_EMAS_BACKTEST_CANDLE_CLOSE_DEST=close \

run-trade-emas-run-backtest:
	${TRADE_EMAS_ENVS} cargo run --bin trade_emas -- --command run-backtest

run-trade-emas-run-backtest-batch:
	${TRADE_EMAS_ENVS} cargo run --bin trade_emas -- --command run-backtest-batch

run-trade-emas-upload-tickers-to-database:
	${env_vars} cargo run --bin trade_emas -- --command upload-tickers-to-database

run-trade-show-2024-04-07-candles:
	${env_vars} cargo run --bin trade_emas -- --command show-2024-04-07-candles

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

run-docs:
	# find . -maxdepth 1 | grep -v target
	cargo doc && cargo doc --open
