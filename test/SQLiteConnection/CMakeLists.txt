project(SQLiteConnection_test)
add_executable(SQLiteConnection_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(SQLiteConnection_test SQLiteConnection_test_RUN)
