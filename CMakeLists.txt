project(PgAccel)

cmake_minimum_required(VERSION 3.18)
set(CMAKE_CXX_STANDARD 20)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512bw -pthread")

file(GLOB pgaccel_lib_SRC
     "src/*.h"
     "src/*.cc"
     "src/*.hpp"
)

add_library(pgaccel_static STATIC ${pgaccel_lib_SRC})
target_link_libraries(pgaccel_static PRIVATE
                      Arrow::arrow_shared Parquet::parquet_shared)

add_executable(pgaccel "pgaccel.cc")
target_include_directories(pgaccel PRIVATE "src")
target_link_libraries(pgaccel PRIVATE
                      readline
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
