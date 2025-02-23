cmake_minimum_required(VERSION 3.19)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")
project(simdb CXX)

add_library(simdb src/Pipeline.cpp)
set(SIMDB_STATIC_LIBS ${PROJECT_BINARY_DIR}/libsimdb.a)

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(simdb PUBLIC ${SIMDB_BASE}/include)
include(${SIMDB_BASE}/cmake/simdb-config.cmake)

add_subdirectory(test EXCLUDE_FROM_ALL)

#
# Installation
#
install(DIRECTORY include/simdb/ DESTINATION include/simdb)
install(FILES ${SIMDB_STATIC_LIBS} DESTINATION lib)
install(DIRECTORY cmake/ DESTINATION lib/cmake/simdb)
