project(Schema_test)
add_executable(Schema_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(Schema_test Schema_test_RUN)
