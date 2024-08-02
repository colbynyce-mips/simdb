###############################################################
#                     RequiredLibraries                       #
###############################################################

# Find SQLite3
find_package(SQLite3 3.19 REQUIRED)
set(SimDB_LIBS ${SQLite3_LIBRARIES} pthread)
