project(StatsCollection_test)
add_executable(StatsCollection_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(StatsCollection_test StatsCollection_test_RUN)
