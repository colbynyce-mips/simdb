project(StructConstellations_test)
add_executable(StructConstellations_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(StructConstellations_test StructConstellations_test_RUN)
