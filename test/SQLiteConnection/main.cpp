/*
 \brief Tests for SQLite connections, INSERT, UPDATE, etc.
 */

#include "simdb/test/SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

static constexpr auto TEST_INT32           = std::numeric_limits<int32_t>::max();
static constexpr auto TEST_INT64           = std::numeric_limits<int64_t>::max();
static constexpr auto TEST_UINT32          = std::numeric_limits<uint32_t>::max();
static constexpr auto TEST_UINT64          = std::numeric_limits<uint64_t>::max();
static constexpr auto TEST_DOUBLE          = std::numeric_limits<double>::max();
static const std::string TEST_STRING       = "TheExampleString";
static const std::vector<int> TEST_VECTOR  = {1,2,3,4,5};
static const std::vector<int> TEST_VECTOR2 = {6,7,8,9,10};
static const simdb::Blob TEST_BLOB         = TEST_VECTOR;
static const simdb::Blob TEST_BLOB2        = TEST_VECTOR2;

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

    record1->setPropertyInt32("SomeInt32", TEST_INT32 / 2);
    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32 / 2);

    record1->setPropertyInt64("SomeInt64", TEST_INT64 / 2);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64 / 2);

    record1->setPropertyUInt32("SomeUInt32", TEST_UINT32 / 2);
    EXPECT_EQUAL(record1->getPropertyUInt32("SomeUInt32"), TEST_UINT32 / 2);

    record1->setPropertyUInt64("SomeUInt64", TEST_UINT64 / 2);
    EXPECT_EQUAL(record1->getPropertyUInt64("SomeUInt64"), TEST_UINT64 / 2);

    // Verify set/get APIs for floating-point types
    auto record2 = db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"),
                                 SQL_COLUMNS("SomeDouble"),
                                 SQL_VALUES(TEST_DOUBLE));

    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE);

    record2->setPropertyDouble("SomeDouble", TEST_DOUBLE / 2);
    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE / 2);

    // Verify set/get APIs for string types
    auto record3 = db_mgr.INSERT(SQL_TABLE("StringTypes"),
                                 SQL_COLUMNS("SomeString"),
                                 SQL_VALUES(TEST_STRING));

    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING);

    record3->setPropertyString("SomeString", TEST_STRING + "2");
    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING + "2");

    // Verify set/get APIs for blob types
    auto record4 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                 SQL_COLUMNS("SomeBlob"),
                                 SQL_VALUES(TEST_VECTOR));

    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record4->setPropertyBlob("SomeBlob", TEST_VECTOR2);
    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    auto record5 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                 SQL_COLUMNS("SomeBlob"),
                                 SQL_VALUES(TEST_BLOB));

    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record5->setPropertyBlob("SomeBlob", TEST_BLOB2.data_ptr, TEST_BLOB2.num_bytes);
    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    // Verify setDefaultValue()
    auto record6 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    EXPECT_EQUAL(record6->getPropertyInt32("DefaultInt32"), TEST_INT32);
    EXPECT_EQUAL(record6->getPropertyInt64("DefaultInt64"), TEST_INT64);
    EXPECT_EQUAL(record6->getPropertyUInt32("DefaultUInt32"), TEST_UINT32);
    // TODO EXPECT_EQUAL(record6->getPropertyUInt64("DefaultUInt64"), TEST_UINT64);
    EXPECT_EQUAL(record6->getPropertyString("DefaultString"), TEST_STRING);
    EXPECT_WITHIN_EPSILON(record6->getPropertyDouble("DefaultDouble"), TEST_DOUBLE);

    // Verify findRecord() with bad ID.
    auto record7 = db_mgr.findRecord("DefaultValues", 404);
    EXPECT_EQUAL(record7.get(), nullptr);
    auto record8 = db_mgr.getRecord("DefaultValues", record6->getId());
    EXPECT_NOTEQUAL(record8.get(), nullptr);
    EXPECT_EQUAL(record8->getId(), record6->getId());

    // Verify record deletion.
    auto record9 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    auto record10 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    auto record11 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    EXPECT_TRUE(record9->removeFromTable());
    EXPECT_FALSE(record9->removeFromTable());
    EXPECT_FALSE(db_mgr.removeRecordFromTable("DefaultValues", record9->getId()));
    EXPECT_TRUE(db_mgr.removeRecordFromTable("DefaultValues", record10->getId()));
    EXPECT_FALSE(db_mgr.removeRecordFromTable("DefaultValues", record10->getId()));
    EXPECT_NOTEQUAL(db_mgr.findRecord("DefaultValues", record11->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.removeAllRecordsFromTable("DefaultValues"), 0);
    EXPECT_EQUAL(db_mgr.findRecord("DefaultValues", record11->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.findRecord("StringTypes", record3->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.removeAllRecordsFromAllTables(), 0);
    EXPECT_EQUAL(db_mgr.findRecord("StringTypes", record3->getId()).get(), nullptr);

    // To get ready for testing the SqlQuery class, first create some new records.
    //
    // IntegerTypes
    // ---------------------------------------------------------------------------------
    // SomeInt32    SomeInt64    SomeUInt32    SomeUInt64
    // 111          555          789           50505050
    // 222          555          444           50505050
    // 333          555          789           50505050
    auto int_id1 = db_mgr.INSERT(SQL_TABLE("IntegerTypes"),
                                 SQL_COLUMNS("SomeInt32", "SomeInt64", "SomeUInt32", "SomeUInt64"),
                                 SQL_VALUES(111, 555, 789, 50505050));

    auto int_id2 = db_mgr.INSERT(SQL_TABLE("IntegerTypes"),
                                 SQL_COLUMNS("SomeInt32", "SomeInt64", "SomeUInt32", "SomeUInt64"),
                                 SQL_VALUES(222, 555, 444, 50505050));

    auto int_id3 = db_mgr.INSERT(SQL_TABLE("IntegerTypes"),
                                 SQL_COLUMNS("SomeInt32", "SomeInt64", "SomeUInt32", "SomeUInt64"),
                                 SQL_VALUES(333, 555, 789, 50505050));

    // FloatingPointTypes
    // ---------------------------------------------------------------------------------
    // SomeDouble
    // 1.1
    // 2.2
    // 3.3
    // 4.4
    // 5.5
    db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(1.1));
    db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(2.2));
    db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(3.3));
    db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(4.4));
    db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(5.5));

    // StringTypes
    // ---------------------------------------------------------------------------------
    // SomeString
    // foo
    // foo
    // bar
    // baz
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("foo"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("foo"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("bar"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("baz"));

    // Test SQL queries for integer types.
    int32_t i32;
    int64_t i64;
    uint32_t u32;
    uint64_t u64;

    auto query = db_mgr.createQuery("IntegerTypes");

    // Each successful call to result_set.getNextRecord() populates these variables.
    query->select("SomeInt32", i32);
    query->select("SomeInt64", i64);
    query->select("SomeUInt32", u32);
    query->select("SomeUInt64", u64);

    // SELECT COUNT(Id) should return 3 records.
    EXPECT_EQUAL(query->count(), 3);
    auto result_set1 = query->getResultSet();

    // Iterate over the records one at a time and verify the data.
    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_EQUAL(i32, 111);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_EQUAL(i32, 222);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 444);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_EQUAL(i32, 333);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    // We should have read all the records.
    EXPECT_FALSE(result_set1.getNextRecord());

    // Reset the iterator and make sure it can iterate again from the start.
    result_set1.reset();
    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_TRUE(result_set1.getNextRecord());
    EXPECT_FALSE(result_set1.getNextRecord());

    // Add WHERE constraints, rerun the query, and check the results.
    query->addConstraintForInt("SomeInt32", simdb::Constraints::NOT_EQUAL, 111);
    auto result_set2 = query->getResultSet();

    EXPECT_TRUE(result_set2.getNextRecord());
    EXPECT_EQUAL(i32, 222);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 444);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set2.getNextRecord());
    EXPECT_EQUAL(i32, 333);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_FALSE(result_set2.getNextRecord());

    query->addConstraintForInt("SomeUInt32", simdb::Constraints::EQUAL, 789);
    auto result_set3 = query->getResultSet();

    EXPECT_TRUE(result_set3.getNextRecord());
    EXPECT_EQUAL(i32, 333);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_FALSE(result_set3.getNextRecord());

    // Remove WHERE constraints, add limit, rerun query.
    query->resetConstraints();
    query->setLimit(2);
    auto result_set4 = query->getResultSet();

    EXPECT_TRUE(result_set4.getNextRecord());
    EXPECT_EQUAL(i32, 111);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set4.getNextRecord());
    EXPECT_EQUAL(i32, 222);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 444);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_FALSE(result_set4.getNextRecord());

    // Add ORDER BY clauses, rerun query.
    query->resetLimit();
    query->orderBy("SomeUInt32", simdb::QueryOrder::DESC);
    query->orderBy("SomeInt32", simdb::QueryOrder::ASC);
    auto result_set5 = query->getResultSet();

    EXPECT_TRUE(result_set5.getNextRecord());
    EXPECT_EQUAL(i32, 111);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set5.getNextRecord());
    EXPECT_EQUAL(i32, 333);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 789);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_TRUE(result_set5.getNextRecord());
    EXPECT_EQUAL(i32, 222);
    EXPECT_EQUAL(i64, 555);
    EXPECT_EQUAL(u32, 444);
    EXPECT_EQUAL(u64, 50505050);

    EXPECT_FALSE(result_set5.getNextRecord());
}
