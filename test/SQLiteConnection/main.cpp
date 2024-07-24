/*
 \brief Tests for SQLite connections, INSERT, UPDATE, etc.
 */

#include "simdb/test/SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

int main()
{
    using dt = simdb::ColumnDataType;

    simdb::Schema schema;

    schema.addTable("Metadata")
        .addColumn("Name", dt::string_t)
        .addColumn("TheInt", dt::int32_t)
        .addColumn("TheDouble", dt::double_t)
        .addColumn("TheIndexedDouble", dt::double_t)->index()
        .addColumn("TheOtherIndexedDouble", dt::double_t)->indexAgainst({"Name", "TheInt"})
        .addColumn("SomeString", dt::string_t)->setDefaultValue("foo")
        .addColumn("OtherString", dt::string_t)->setDefaultValue(std::string("foo"))
        .addColumn("SomeInt", dt::int32_t)->setDefaultValue(4)
        .addColumn("SomeDouble", dt::double_t)->setDefaultValue(3.14);

    simdb::DatabaseManager db_mgr;
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    // Create some records and verify the INSERT was successful.
    int dbid = db_mgr.INSERT(SQL_TABLE("Metadata"), SQL_COLUMNS("TheInt", "TheDouble"), SQL_VALUES(777, 3.14));
    EXPECT_NOTEQUAL(dbid, 0);
    EXPECT_NOTEQUAL(dbid, -1);
}
