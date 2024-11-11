run:
	cd build ** cmake .. && make && ./t

install-libs:
	git clone git@github.com:curl/curl.git libs/curl && \
	git clone git@github.com:nlohmann/json.git libs/json && \
	git clone https://libwebsockets.org/repo/libwebsockets libs/libwebsockets && \
	git clone git@github.com:ClickHouse/clickhouse-cpp.git libs/clickhouse-cpp
