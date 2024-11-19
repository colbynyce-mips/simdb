project(AnyCollections_test)
add_executable(AnyCollections_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(AnyCollections_test AnyCollections_test_RUN)
