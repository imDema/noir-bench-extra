cmake_minimum_required(VERSION 3.22)
project(Wordcount)

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

# Enable colors in ninja
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -ggdb3")
set(CMAKE_CXX_FLAGS_RELEASE "-march=native -mtune=native -Ofast -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address")

add_executable(main src/main.cpp)
target_include_directories(main PUBLIC src)
target_link_libraries(main ${MPI_CXX_LIBRARIES})

add_executable(main_separate_root src/main_separate_root.cpp)
target_include_directories(main_separate_root PUBLIC src)
target_link_libraries(main_separate_root ${MPI_CXX_LIBRARIES})

add_executable(main_three_steps src/main_three_steps.cpp)
target_include_directories(main_three_steps PUBLIC src)
target_link_libraries(main_three_steps ${MPI_CXX_LIBRARIES})

add_executable(main_three_steps_isend src/main_three_steps_isend.cpp)
target_include_directories(main_three_steps_isend PUBLIC src)
target_link_libraries(main_three_steps_isend ${MPI_CXX_LIBRARIES})

add_executable(main_three_steps_batch src/main_three_steps_batch.cpp)
target_include_directories(main_three_steps_batch PUBLIC src)
target_link_libraries(main_three_steps_batch ${MPI_CXX_LIBRARIES})

add_executable(main_batched src/main_batched.cpp)
target_include_directories(main_batched PUBLIC src)
target_link_libraries(main_batched ${MPI_CXX_LIBRARIES})

# Enable LTO
set_property(TARGET main PROPERTY INTERPROCEDURAL_OPTIMIZATION True)
