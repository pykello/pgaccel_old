project(PgAccel)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512bw")

file(GLOB pgaccel_SRC
     "*.h"
     "*.cc"
     "*.hpp"
)

add_executable(pgaccel ${pgaccel_SRC})
target_link_libraries(pgaccel PRIVATE
                      readline
                      Arrow::arrow_shared Parquet::parquet_shared)
