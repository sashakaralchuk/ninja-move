cmake_minimum_required(VERSION 3.15)

project(core-cpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(say-hello-lib tt/hello/hello.hpp tt/hello/hello.cxx)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

set(CMAKE_PREFIX_PATH /Users/yy/dev/python/ninja-move/core-cpp/libs/grpc/install_dir)
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

find_library(LIBHV NAMES hv PATHS libs/libhv/build/lib)
find_library(CURL NAMES curl PATHS libs/curl/build/lib)
find_library(SPDLOG NAMES spdlog PATHS libs/spdlog/build)
find_library(ZLIB NAMES z PATHS libs/zlib/build)
find_library(GMP NAMES gmp PATHS libs/gmp-6.3.0/build/lib)
find_library(BACKWARD NAMES backward PATHS libs/backward-cpp/build)
find_library(LIBRDKAFKA NAMES rdkafka++ PATHS libs/librdkafka/build/src-cpp)

add_subdirectory(tt/SqrtLibrary)
add_subdirectory(libs/json)
add_subdirectory(libs/clickhouse-cpp)
add_subdirectory(libs/googletest)
add_subdirectory(libs/cpp-channel)

get_filename_component(hw_proto "../protos/trade_contango.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/trade_contango.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/trade_contango.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/trade_contango.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/trade_contango.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND $<TARGET_FILE:protobuf::protoc>
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
    absl::check
    gRPC::grpc++_reflection
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(t t.cxx tt/t2.cxx)
target_include_directories(t PRIVATE
    libs/libhv/build/include/hv
    libs/curl/include/curl
    libs/clickhouse-cpp
    libs/clickhouse-cpp/contrib/absl
)
target_link_libraries(t PRIVATE
    say-hello-lib
    SqrtLibrary
    nlohmann_json::nlohmann_json
    ${CURL}
    ${LIBHV}
    clickhouse-cpp-lib
)

add_executable(trade_contango_arbitrage_2024_11_11
    trade_contango_arbitrage_2024_11_11.cxx
    src/models.cxx
    src/clients.cxx
    src/spreads_map.cxx
)
target_include_directories(trade_contango_arbitrage_2024_11_11 PRIVATE
    libs/libhv/build/include/hv
    libs/spdlog/include
    libs/clickhouse-cpp
    libs/zlib
    libs/curl/include/curl
    libs/gmp-6.3.0/build/include
    libs/backward-cpp
    libs/librdkafka/src-cpp
    libs/cpp-channel/include
)
target_link_libraries(trade_contango_arbitrage_2024_11_11 PRIVATE
    nlohmann_json::nlohmann_json
    ${LIBHV}
    ${SPDLOG}
    clickhouse-cpp-lib
    ${ZLIB}
    ${CURL}
    ${GMP}
    OpenSSL::SSL
    hw_grpc_proto
    absl::check
    absl::flags
    absl::flags_parse
    absl::log
    gRPC::grpc++_reflection
    gRPC::grpc++
    protobuf::libprotobuf
    ${BACKWARD}
    ${LIBRDKAFKA}
)

enable_testing()
include(GoogleTest)
add_executable(models_test src/models_test.cxx src/models.cxx)
target_include_directories(models_test PRIVATE
    libs/spdlog/include
    libs/gmp-6.3.0/build/include
)
target_link_libraries(models_test
    GTest::gtest_main
    nlohmann_json::nlohmann_json
    ${SPDLOG}
    ${GMP}
)
gtest_discover_tests(models_test)
add_executable(clients_test
    src/clients_test.cxx
    src/clients.cxx
    src/models.cxx
)
target_include_directories(clients_test PRIVATE
    libs/libhv/build/include/hv
    libs/spdlog/include
    libs/clickhouse-cpp
    libs/zlib
    libs/curl/include/curl
    libs/gmp-6.3.0/build/include
)
target_link_libraries(clients_test
    GTest::gtest_main
    GTest::gmock_main
    nlohmann_json::nlohmann_json
    ${LIBHV}
    ${SPDLOG}
    clickhouse-cpp-lib
    ${ZLIB}
    ${CURL}
    ${GMP}
    OpenSSL::SSL
    hw_grpc_proto
    absl::check
    absl::flags
    absl::flags_parse
    absl::log
    gRPC::grpc++_reflection
    gRPC::grpc++
    protobuf::libprotobuf
)
gtest_discover_tests(clients_test)
add_executable(spreads_map_test
    src/spreads_map_test.cxx
    src/spreads_map.cxx
    src/models.cxx
)
target_include_directories(spreads_map_test PRIVATE
    libs/spdlog/include
    libs/gmp-6.3.0/build/include
    libs/clickhouse-cpp
)
target_link_libraries(spreads_map_test
    GTest::gtest_main
    GTest::gmock_main
    ${GMP}
    ${SPDLOG}
    clickhouse-cpp-lib
    nlohmann_json::nlohmann_json
)
gtest_discover_tests(spreads_map_test)

configure_file(t.cxx t_temp_source.cxx)
