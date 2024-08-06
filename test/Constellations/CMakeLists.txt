project(Constellations_test)
add_executable(Constellations_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(Constellations_test Constellations_test_RUN)
