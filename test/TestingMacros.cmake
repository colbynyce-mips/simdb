#
# Testing macros
#

# MACROS for adding to the targets. Use these to add your tests.

# simdb_regress enforces that your binary gets built as part of the
# regression commands.
macro (simdb_regress target)
  add_dependencies(simdb_regress ${target} )
endmacro(simdb_regress)

# A function to add a simdb test with various options
function(simdb_fully_named_test name target)
  add_test (NAME ${name} COMMAND $<TARGET_FILE:${target}> ${ARGN})
  simdb_regress(${target})
  target_link_libraries      (${target} ${SimDB_LIBS})
  target_include_directories (${target} PUBLIC "${SIMDB_BASE}/include")
endfunction (simdb_fully_named_test)

# Tell simdb to run the following target with the following name.
macro(simdb_named_test name target)
  simdb_fully_named_test(${name} ${target} TRUE ${ARGN})
endmacro(simdb_named_test)

# Just add the executable to the testing using defaults.
macro (simdb_test target)
  simdb_named_test(${target} ${target} ${ARGN})
  target_link_libraries(${target} ${SimDB_LIBS})
endmacro (simdb_test)
