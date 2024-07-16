cmake_minimum_required(VERSION 3.19)
set (CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
project(simdb CXX)

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${SIMDB_BASE}/inc)
include(${SIMDB_BASE}/cmake/simdb-config.cmake)

add_subdirectory(test EXCLUDE_FROM_ALL)
install(DIRECTORY include/ DESTINATION include)
