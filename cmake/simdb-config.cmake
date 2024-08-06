###############################################################
#                     RequiredLibraries                       #
###############################################################

# Find SQLite3
find_package(SQLite3 3.19 REQUIRED)
find_package(ZLIB REQUIRED)
set(SimDB_LIBS ${SQLite3_LIBRARIES} ZLIB::ZLIB pthread)
