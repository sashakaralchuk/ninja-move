cmake_minimum_required(VERSION 3.15)

project(core-cpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(say-hello-lib tt/hello/hello.hpp tt/hello/hello.cxx)

find_library(LIBHV NAMES hv PATHS libs/libhv/build/lib)
find_library(CURL NAMES curl PATHS libs/curl/build/lib)

add_subdirectory(tt/SqrtLibrary)
add_subdirectory(libs/json)
add_subdirectory(libs/clickhouse-cpp)

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

configure_file(t.cxx t_temp_source.cxx)
