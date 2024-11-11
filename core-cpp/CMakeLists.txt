cmake_minimum_required(VERSION 3.15)

project(core-cpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(
    say-hello-lib
    tt/hello/hello.hpp
    tt/hello/hello.cxx
    )

add_subdirectory(tt/SqrtLibrary)
add_subdirectory(libs/json)
add_subdirectory(libs/libwebsockets)
add_subdirectory(libs/clickhouse-cpp)
add_subdirectory(libs/curl)

add_executable(t t.cxx)

target_link_libraries(t PRIVATE say-hello-lib)
target_link_libraries(t PUBLIC SqrtLibrary)
target_link_libraries(t PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(t PRIVATE websockets)
target_link_libraries(t PRIVATE clickhouse-cpp-lib)
target_link_libraries(t PRIVATE libcurl)

configure_file(t.cxx t_temp_source.cxx)
