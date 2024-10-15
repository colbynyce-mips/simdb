project(IterablesValidity_test)
add_executable(IterablesValidity_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(IterablesValidity_test IterablesValidity_test_RUN)
