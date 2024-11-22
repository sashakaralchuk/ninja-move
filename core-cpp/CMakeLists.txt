cmake_minimum_required(VERSION 3.15)

project(core-cpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(say-hello-lib tt/hello/hello.hpp tt/hello/hello.cxx)

find_library(LIBHV NAMES hv PATHS libs/libhv/build/lib)
find_library(CURL NAMES curl PATHS libs/curl/build/lib)
find_library(SPDLOG NAMES spdlog PATHS libs/spdlog/build)
find_library(ZLIB NAMES z PATHS libs/zlib/build)
find_library(GMP NAMES gmp PATHS libs/gmp-6.3.0/build/lib)

add_subdirectory(tt/SqrtLibrary)
add_subdirectory(libs/json)
add_subdirectory(libs/clickhouse-cpp)
add_subdirectory(libs/googletest)

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

add_executable(run_trigger_on_contango_2024_11_11
    run_trigger_on_contango_2024_11_11.cxx
    src/models.cxx
)
target_include_directories(run_trigger_on_contango_2024_11_11 PRIVATE
    libs/libhv/build/include/hv
    libs/spdlog/include
    libs/clickhouse-cpp
    libs/clickhouse-cpp/contrib/absl
    libs/zlib
    libs/curl/include/curl
    libs/gmp-6.3.0/build/include
)
target_link_libraries(run_trigger_on_contango_2024_11_11 PRIVATE
    nlohmann_json::nlohmann_json
    ${LIBHV}
    ${SPDLOG}
    clickhouse-cpp-lib
    ${ZLIB}
    ${CURL}
    ${GMP}
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

configure_file(t.cxx t_temp_source.cxx)
