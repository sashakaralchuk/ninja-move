env_vars=${shell cat .env | grep -v "\#" | xargs}

run:
	# cd build && cmake .. && make && ./t
	# cmake -S . -B build && cmake --build build -j 12

test:
	# make && ctest
	# cmake --build build -j 12 && ctest --test-dir build --output-on-failure -R NumbersThresholds -j 12

run-trigger-on-contango-2024-11-11:
	# make && SPDLOG_LEVEL=debug THRESHOLD_REL=3.0 ./run_trigger_on_contango_2024_11_11
	cmake --build build -j 12 && \
		${env_vars} \
		SPDLOG_LEVEL=debug \
		THRESHOLD_REL=3.0 \
		./build/run_trigger_on_contango_2024_11_11

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
