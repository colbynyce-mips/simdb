project(IterableCollections_test)
add_executable(IterableCollections_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(IterableCollections_test IterableCollections_test_RUN)
