cmake_minimum_required(VERSION 3.0.0)
project(DB36CPP VERSION 0.1.0)

enable_testing()

set(BOOST_INCLUDE_LIBRARIES config)
set(BOOST_INCLUDE_LIBRARIES multiprecision)
set(BOOST_ENABLE_CMAKE ON)
set(CMAKE_CXX_STANDARD 20)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON)

include (FetchContent)
FetchContent_Declare(
    Boost_MP
    GIT_REPOSITORY https://github.com/boostorg/multiprecision.git
    GIT_TAG boost-1.81.0
)

FetchContent_Declare(
    Boost_CONFIG
    GIT_REPOSITORY https://github.com/boostorg/config.git
    GIT_TAG boost-1.81.0
)

FetchContent_Declare(
    Google_Test
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG main
)

FetchContent_Declare(
    Google_Benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG main
)

FetchContent_MakeAvailable(Boost_CONFIG)
FetchContent_MakeAvailable(Boost_MP)
FetchContent_MakeAvailable(Google_Test)
FetchContent_MakeAvailable(Google_Benchmark)

add_library(DB36CPP src/blob.cpp)
target_link_libraries(DB36CPP PRIVATE Boost::config Boost::multiprecision) 

add_executable(benchmark_executable src/tests/benchmark.cpp)
target_link_libraries(benchmark_executable PRIVATE benchmark)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
