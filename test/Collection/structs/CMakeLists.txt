project(StructCollections_test)
add_executable(StructCollections_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(StructCollections_test StructCollections_test_RUN)
