project(simdb_test)
include(CTest)
add_custom_target(regress)
add_custom_command(
    TARGET regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

add_subdirectory(Collection)
add_subdirectory(SQLiteConnection)
add_subdirectory(PerfDiagnostics)
