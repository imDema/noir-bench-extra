cmake_minimum_required(VERSION 3.22)
project(cOMPIling)

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

# Enable colors in ninja
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -fopenmp -ggdb3")
set(CMAKE_CXX_FLAGS_RELEASE "-march=native -mtune=native -Ofast")
set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address")

add_executable(main main.cpp include/argument_parser/argument_parser.cpp include/csv.cpp)
target_include_directories(main PUBLIC include)
target_link_libraries(main ${MPI_CXX_LIBRARIES})
# Enable LTO
set_property(TARGET main PROPERTY INTERPROCEDURAL_OPTIMIZATION True)
