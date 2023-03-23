cmake_minimum_required(VERSION 3.0.0)
project(DB36CPP VERSION 0.1.0)


set(BOOST_INCLUDE_LIBRARIES config)
set(BOOST_INCLUDE_LIBRARIES multiprecision)
set(BOOST_ENABLE_CMAKE ON)
set(CMAKE_CXX_STANDARD 20)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON)

include (FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG main
)
FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)

enable_testing()

add_library(DB36CPP src/blob.cpp)
target_link_libraries(DB36CPP PUBLIC GTest::GTest) 

add_subdirectory(src/tests)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
