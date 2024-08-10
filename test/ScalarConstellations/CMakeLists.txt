project(ScalarConstellations_test)
add_executable(ScalarConstellations_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(ScalarConstellations_test ScalarConstellations_test_RUN)
