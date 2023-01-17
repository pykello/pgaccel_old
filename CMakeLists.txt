project(PgAccel)

cmake_minimum_required(VERSION 3.18)
set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

FetchContent_Declare(
    touca
    GIT_REPOSITORY https://github.com/trytouca/trytouca
    GIT_TAG        v1.8.0
    SOURCE_SUBDIR  sdk/cpp
)
FetchContent_MakeAvailable(touca)

find_package(PAPI REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512bw -pthread -O3")

file(GLOB pgaccel_lib_SRC
     "src/*.h"
     "src/*.cc"
     "src/*.hpp"
)

add_library(pgaccel_static STATIC ${pgaccel_lib_SRC})
target_link_libraries(pgaccel_static PRIVATE
                      Arrow::arrow_shared Parquet::parquet_shared tbb)

add_executable(pgaccel "pgaccel.cc")
target_include_directories(pgaccel PRIVATE "src")
target_link_libraries(pgaccel PRIVATE
                      readline
                      ${PAPI_LIBRARIES}
                      pgaccel_static)

# tests
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

file(GLOB pgaccel_test_SRC
     "tests/*.h"
     "tests/*.cc"
     "tests/*.hpp"
)

add_executable(
  run_tests
  ${pgaccel_test_SRC}
)
target_include_directories(run_tests PRIVATE "src" "tests")
target_link_libraries(
  run_tests
  GTest::gtest_main
  pgaccel_static
)

include(GoogleTest)
gtest_discover_tests(run_tests)

file(GLOB pgaccel_touca_tests_SRC
     "touca_tests/*.h"
     "touca_tests/*.cc"
     "touca_tests/*.hpp"
)

add_executable(
  run_touca_tests
  ${pgaccel_touca_tests_SRC}
)

target_include_directories(run_touca_tests PRIVATE "src" "touca_tests")
target_link_libraries(
  run_touca_tests
  touca
  pgaccel_static
)
