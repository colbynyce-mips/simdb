project(Thread_test)
add_executable(Thread_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(Thread_test Thread_test_RUN)
