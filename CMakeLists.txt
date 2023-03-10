cmake_minimum_required(VERSION 3.0.0)
project(DB36CPP VERSION 0.1.0)


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
FetchContent_MakeAvailable(Boost_MP)

FetchContent_Declare(
    Boost_CONFIG
    GIT_REPOSITORY https://github.com/boostorg/config.git
    GIT_TAG boost-1.81.0
)
FetchContent_MakeAvailable(Boost_CONFIG)

enable_testing()

add_library(DB36CPP src/blob.cpp)
target_link_libraries(DB36CPP PRIVATE Boost::config Boost::multiprecision) 

add_subdirectory(src/tests)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
