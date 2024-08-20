###############################################################
#                     RequiredLibraries                       #
###############################################################

# Find RapidJSON
find_package (RapidJSON 1.1 REQUIRED)
include_directories (SYSTEM ${RapidJSON_INCLUDE_DIRS})
message (STATUS "Using RapidJSON CPP ${RapidJSON_VERSION}")

# Find SQLite3
find_package (SQLite3 3.19 REQUIRED)
include_directories (SYSTEM ${SQLite3_INCLUDE_DIRS})
message (STATUS "Using SQLite3 ${SQLite3_VERSION}")

# Find zlib
find_package(ZLIB REQUIRED)
include_directories(SYSTEM ${ZLIB_INCLUDE_DIRS})
message (STATUS "Using zlib ${ZLIB_VERSION_STRING}")

# Populate the SimDB_LIBS variable with the required libraries for
# basic SimDB linking
set (SimDB_LIBS sqlite3 ZLIB::ZLIB pthread)
