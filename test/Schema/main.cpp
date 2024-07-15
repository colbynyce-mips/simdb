/*!
 * \brief Schema tests for SimDB functionality that is not
 * specific to any particular database format (SQLite, HDF5,
 * etc.)
 */

#include "simdb/test/SimDBTester.hpp"
#include "simdb/schema/Schema.hpp"
#include "simdb/Errors.hpp"

#define PRINT_ENTER_TEST                                                              \
  std::cout << std::endl;                                                             \
  std::cout << "*************************************************************"        \
            << "*** Beginning '" << __FUNCTION__ << "'"                               \
            << "*************************************************************"        \
            << std::endl;

int main()
{
    // Just verify that we can create a Schema without actually
    // turning it into a SQLite database. This test is a placeholder
    // for a full SQLite test, then it will be removed as it would
    // be redundant.
    using dt = simdb::ColumnDataType;

    simdb::Schema schema;

    schema.addTable("Metadata")
        //.addColumn("Name", dt::string_t)
        //.addColumn("TheInt", dt::int32_t)
        //.addColumn("TheDouble", dt::double_t)
        //.addColumn("TheIndexedDouble", dt::double_t)->index()
        //.addColumn("TheOtherIndexedDouble", dt::double_t)->indexAgainst({"Name", "TheInt"})
        .addColumn("SomeString", dt::string_t)->setDefaultValue("foo")
        //.addColumn("SomeString", dt::string_t)->setDefaultValue(std::string("foo"))
        //.addColumn("SomeInt", dt::int32_t)->setDefaultValue(4)
        .addColumn("SomeDouble", dt::double_t)->setDefaultValue(3.14);
}
