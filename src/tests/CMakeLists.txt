cmake_minimum_required(VERSION 3.0.0)

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

add_executable(gtests gtests.cpp)
target_link_libraries(gtests PRIVATE GTest::GTest DB36CPP)

add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark PRIVATE DB36CPP)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
