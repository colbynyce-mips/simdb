project(AllCollections_test)
add_executable(AllCollections_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(AllCollections_test AllCollections_test_RUN)
