###############################################################
#                     RequiredLibraries                       #
###############################################################

# Find SQLite3
find_package(SQLite3 3.19 REQUIRED)

if(DEFINED SQLite3_INCLUDE_DIRS)
  set(SYMLINK_SRC ${SQLite3_INCLUDE_DIRS}/sqlite3.h)
  set(SYMLINK_DST ${SIMDB_BASE}/cmake/sqlite3.h)
  execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${SYMLINK_SRC} ${SYMLINK_DST})
  message(STATUS "Created symlink: ${SYMLINK_DST} -> ${SYMLINK_SRC}")
endif()

set(SimDB_LIBS ${SQLite3_LIBRARIES} pthread)
