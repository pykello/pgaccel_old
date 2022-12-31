project(PgAccel)

cmake_minimum_required(VERSION 3.22)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512bw")

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
