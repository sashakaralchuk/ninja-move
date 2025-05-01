env_vars=${shell cat .env | grep -v "\#" | xargs}

run:
	# cd build && cmake .. && make && ./t
	# cmake -S . -B build && cmake --build build -j 12

test:
	# make && ctest
	# cmake --build build -j 12 && ctest --test-dir build --output-on-failure -j 12

run-trade-contango-arbitrage-2024-11-11:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		THRESHOLD_REL=3.0 \
		TICKERS_VERSION=execute_v5 \
		./build/trade_contango_arbitrage_2024_11_11

run-trade-contango-arbitrage-2024-11-11-req:
	grpcurl -plaintext \
	-d '{"list":[{"p_ask_spot":0.0,"p_bid_fut":0.0,"diff_rel":0.0,"t_spot":{"ex":"-","s":"-","st":"-","k":"-","ts":0,"p_bid":0.0,"p_ask":0.0,"v":0.0},"t_fut":{"ex":"-","s":"-","st":"-","k":"-","ts":0,"p_bid":0.0,"p_ask":0.0,"v":0.0}}]}' \
	127.0.0.1:50051 \
	trade_contango.TradeContango/FireTradeV2

print-doc-on-remote-server-execution:
	# * figure out latency: curl -o /dev/null -s -w 'Total: %{time_total}s\n' 'https://api.gateio.ws/api/v4/futures/usdt/tickers?contract=BTC_USDT'
	# * figure out ip + country: dig +short 'api.gateio.ws' | grep "^[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*$" | xargs -I{} curl "ipinfo.io/{}/country"
	# * server: default choice - gcp, hetzner, digitalocean

run-trade-contango-arbitrage-2024-11-11-debug-print-balances:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=info \
		TICKERS_VERSION=debug_print_balances \
		./build/trade_contango_arbitrage_2024_11_11

run-trade-contango-arbitrage-2024-11-11-write-spreads-to-topic:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		TICKERS_VERSION=write_spreads_to_topic \
		./build/trade_contango_arbitrage_2024_11_11

run-wait-for-spread-converge-save-order-book:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		TICKERS_VERSION=wait_for_spread_converge_save_order_book \
		./build/trade_contango_arbitrage_2024_11_11

run-wait-for-spread-converge-show-order-book:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		TICKERS_VERSION=wait_for_spread_converge_show_order_book \
		./build/trade_contango_arbitrage_2024_11_11

run-trade-contango-arbitrage-2024-11-11-fetch-process-tickers:
	cmake --build build -j 12 --target trade_contango_arbitrage_2024_11_11 && \
		${env_vars} \
		SPDLOG_LEVEL=info \
		TICKERS_VERSION=fetch_process_tickers \
		DIFF_REL_BOTTOM_THRESHOLD=1.5 \
		./build/trade_contango_arbitrage_2024_11_11

run-busybox-upload-exchanges-entities-curr:
	cmake --build build -j 12 --target busybox_2025_01_12 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		MODE=upload_exchanges_entities_curr \
		./build/busybox_2025_01_12

run-busybox-observe-send-spot-fut-perp-spread-converge-v1:
	cmake --build build -j 12 --target busybox_2025_01_12 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		MODE=observe_send_spot_fut_perp_spread_converge_v1 \
		INPUT_EX_SPOT=$(ex-spot) \
		INPUT_EX_FUT_PERP=$(ex-fut-perp) \
		INPUT_CCID=$(input-ccid) \
		./build/busybox_2025_01_12

clone-libs:
	git clone git@github.com:curl/curl.git libs/curl && \
	git clone git@github.com:nlohmann/json.git libs/json && \
	git clone git@github.com:ithewei/libhv.git libs/libhv && \
	git clone git@github.com:ClickHouse/clickhouse-cpp.git libs/clickhouse-cpp && \
	git clone git@github.com:gabime/spdlog.git libs/spdlog

install-gmp:
	# cd libs
	# curl -fLO https://gmplib.org/download/gmp/gmp-6.3.0.tar.xz
	# tar -xf gmp-6.3.0.tar.xz
	# cd gmp-6.3.0
	# ./configure --prefix=$(pwd)/build
	# make all
	# make install
	# cp gmpxx.h $(pwd)/build/include

install-grpc:
	# https://grpc.io/docs/languages/cpp/quickstart
	# git clone --recurse-submodules -b v1.66.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
	# cd grpc
	# mkdir -p cmake/build
	# cd cmake/build
	# cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX="$(pwd)/../../install_dir" ../..
	# make -j 16
	# make install

install-cpp-backward:
	# git clone git@github.com:bombela/backward-cpp.git
	# cd backward-cpp
	# mkdir build
	# cd build
	# cmake ..
	# make

fetch-grafana-dashboard:
	# https://grafana.com/docs/grafana/latest/administration/service-accounts/migrate-api-keys/#example
	curl -s -X GET -H "Authorization: Bearer $GRAFANA_API_KEY" \
		"http://localhost:3000/api/search?query=" | jq '.[] | {uid,title}'
	curl -s -X GET -H "Authorization: Bearer $GRAFANA_API_KEY" \
		"http://localhost:3000/api/dashboards/uid/aec6cilacsidcc" | jq
	curl -s -X GET -H "Authorization: Bearer $GRAFANA_API_KEY" \
		"http://localhost:3000/api/dashboards/uid/fec14y22vtjpcb" | jq \
		> ../grafana/dashboards/default.json
	curl -s -X GET -H "Authorization: Bearer $GRAFANA_API_KEY" \
		"http://localhost:3000/api/dashboards/uid/bed1x3o4e6y2oe" | jq \
		> ../grafana/dashboards/spreads-curr-rasul-hasanov.json
	curl -s -X GET -H "Authorization: Bearer $GRAFANA_API_KEY" \
		"http://localhost:3000/api/v1/provisioning/alert-rules" | jq \
		> ../grafana/alert-rules.json

run-grafana-localtunnel:
	# npx localtunnel --port 3000
	# curl https://loca.lt/mytunnelpassword
