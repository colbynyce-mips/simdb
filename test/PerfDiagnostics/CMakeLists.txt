project(PerfDiagnostics_test)
add_executable(PerfDiagnostics_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(PerfDiagnostics_test PerfDiagnostics_test_RUN)
