/*
 \brief Tests for SQLite connections, INSERT, UPDATE, etc.
 */

#include "simdb/test/SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

static constexpr auto TEST_INT32          = std::numeric_limits<int32_t>::max();
static constexpr auto TEST_INT64          = std::numeric_limits<int64_t>::max();
static constexpr auto TEST_UINT32         = std::numeric_limits<uint32_t>::max();
static constexpr auto TEST_UINT64         = std::numeric_limits<uint64_t>::max();
static constexpr auto TEST_DOUBLE         = std::numeric_limits<double>::max();
static const std::string TEST_STRING      = "TheExampleString";
static const std::vector<int> TEST_VECTOR = {1,2,3,4,5};

int main()
{
    using dt = simdb::ColumnDataType;

    simdb::Schema schema;

    schema.addTable("IntegerTypes")
        .addColumn("SomeInt32" , dt::int32_t)
        .addColumn("SomeInt64" , dt::int64_t)
        .addColumn("SomeUInt32", dt::uint32_t)
        .addColumn("SomeUInt64", dt::uint64_t);

    schema.addTable("FloatingPointTypes")
        .addColumn("SomeDouble", dt::double_t);

    schema.addTable("StringTypes")
        .addColumn("SomeString", dt::string_t);

    schema.addTable("BlobTypes")
        .addColumn("SomeBlob", dt::blob_t);

    schema.addTable("DefaultValues")
        .addColumn("DefaultInt32" , dt::int32_t )->setDefaultValue(TEST_INT32)
        .addColumn("DefaultInt64" , dt::int64_t )->setDefaultValue(TEST_INT64)
        .addColumn("DefaultUInt32", dt::uint32_t)->setDefaultValue(TEST_UINT32)
        .addColumn("DefaultUInt64", dt::uint64_t)->setDefaultValue(TEST_UINT64)
        .addColumn("DefaultDouble", dt::double_t)->setDefaultValue(TEST_DOUBLE)
        .addColumn("DefaultString", dt::string_t)->setDefaultValue(TEST_STRING);

    simdb::DatabaseManager db_mgr;
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    // Verify set/get APIs for integer types
    auto record1 = db_mgr.INSERT(SQL_TABLE("IntegerTypes"),
                                 SQL_COLUMNS("SomeInt32", "SomeInt64", "SomeUInt32", "SomeUInt64"),
                                 SQL_VALUES(TEST_INT32, TEST_INT64, TEST_UINT32, TEST_UINT64));

    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64);
    EXPECT_EQUAL(record1->getPropertyUInt32("SomeUInt32"), TEST_UINT32);
    EXPECT_EQUAL(record1->getPropertyUInt64("SomeUInt64"), TEST_UINT64);

    // Verify set/get APIs for floating-point types
    auto record2 = db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"),
                                 SQL_COLUMNS("SomeDouble"),
                                 SQL_VALUES(TEST_DOUBLE));

    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE);

    // Verify set/get APIs for string types
    auto record3 = db_mgr.INSERT(SQL_TABLE("StringTypes"),
                                 SQL_COLUMNS("SomeString"),
                                 SQL_VALUES(TEST_STRING));

    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING);

    // Verify set/get APIs for blob types
    auto record4 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                 SQL_COLUMNS("SomeBlob"),
                                 SQL_VALUES(TEST_VECTOR));

    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);
}
