/*
 \brief Tests for SimDB collections feature (collecting groups of stats
 *      from all over a simulator).
 */

#include "simdb/collection/Scalars.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <array>

TEST_INIT;

using CounterCollectionT = simdb::StatCollection<uint32_t>;

template <typename DataT>
using RandStatCollectionT = simdb::StatCollection<DataT>;

/// Example class for counter values, which are common in certain simulators.
class Counter
{
public:
    /// Increment the count.
    void operator++()
    {
        ++count_;
    }

    /// Get the count.
    uint32_t getValue() const
    {
        return count_;
    }

private:
    uint32_t count_ = 0;
};

/// Example unit which issues instructions.
class Execute
{
public:
    /// Issue an instruction.
    void issue()
    {
        ++num_insts_issued_;
    }

    /// Get the number of issued instructions.
    uint32_t getNumIssued() const
    {
        return num_insts_issued_.getValue();
    }

private:
    Counter num_insts_issued_;
};

/// Example unit which retires instructions.
class Retire
{
public:
    /// Retire an instruction.
    void retire()
    {
        ++num_insts_retired_;
    }

    /// Get the number of retired instructions.
    uint32_t getNumRetired() const
    {
        return num_insts_retired_.getValue();
    }

private:
    Counter num_insts_retired_;
};

/// Example simulator with Execute->Retire units. We will configure a collection
/// for the issued/retired counters.
class Sim
{
public:
    Sim(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
    }

    void runSimulation()
    {
        configCollection_();

        // Issue an instruction for 5 time steps. Do not retire anything at this
        // time to simulate a processing / pipeline delay.
        step_(true, false);
        step_(true, false);
        step_(true, false);
        step_(true, false);
        step_(true, false);

        // For the next 5 time steps, issue a new instruction and retire one old instruction.
        step_(true, true);
        step_(true, true);
        step_(true, true);
        step_(true, true);
        step_(true, true);

        // For the last 5 time steps, retire the instructions.
        step_(false, true);
        step_(false, true);
        step_(false, true);
        step_(false, true);
        step_(false, true);

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

    void verifyDataWithSqlQuery()
    {
        {
            auto query = db_mgr_->createQuery("Collections");

            std::string name;
            query->select("Name", name);

            std::string data_type;
            query->select("DataType", data_type);

            int is_container;
            query->select("IsContainer", is_container);

            auto result_set = query->getResultSet();

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "InstCounts");
            EXPECT_EQUAL(data_type, "uint32_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt8s");
            EXPECT_EQUAL(data_type, "int8_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt16s");
            EXPECT_EQUAL(data_type, "int16_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt32s");
            EXPECT_EQUAL(data_type, "int32_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt64s");
            EXPECT_EQUAL(data_type, "int64_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt8s");
            EXPECT_EQUAL(data_type, "uint8_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt16s");
            EXPECT_EQUAL(data_type, "uint16_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt32s");
            EXPECT_EQUAL(data_type, "uint32_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt64s");
            EXPECT_EQUAL(data_type, "uint64_t");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandFloats");
            EXPECT_EQUAL(data_type, "float");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandDoubles");
            EXPECT_EQUAL(data_type, "double");
            EXPECT_EQUAL(is_container, 0);

            EXPECT_FALSE(result_set.getNextRecord());
        }

        {
            auto query = db_mgr_->createQuery("CollectionPaths");

            int collection_id;
            query->select("CollectionID", collection_id);

            std::string stat_path;
            query->select("StatPath", stat_path);

            auto result_set = query->getResultSet();

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(collection_id, 1);
            EXPECT_EQUAL(stat_path, "stats.num_insts_issued");

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(collection_id, 1);
            EXPECT_EQUAL(stat_path, "stats.num_insts_retired");

            validateStatPaths_(result_set, collection_id, 2, "stats.rand_int8s.bin", stat_path, rand_int8s_);
            validateStatPaths_(result_set, collection_id, 3, "stats.rand_int16s.bin", stat_path, rand_int16s_);
            validateStatPaths_(result_set, collection_id, 4, "stats.rand_int32s.bin", stat_path, rand_int32s_);
            validateStatPaths_(result_set, collection_id, 5, "stats.rand_int64s.bin", stat_path, rand_int64s_);
            validateStatPaths_(result_set, collection_id, 6, "stats.rand_uint8s.bin", stat_path, rand_uint8s_);
            validateStatPaths_(result_set, collection_id, 7, "stats.rand_uint16s.bin", stat_path, rand_uint16s_);
            validateStatPaths_(result_set, collection_id, 8, "stats.rand_uint32s.bin", stat_path, rand_uint32s_);
            validateStatPaths_(result_set, collection_id, 9, "stats.rand_uint64s.bin", stat_path, rand_uint64s_);
            validateStatPaths_(result_set, collection_id, 10, "stats.rand_floats.bin", stat_path, rand_floats_);
            validateStatPaths_(result_set, collection_id, 11, "stats.rand_doubles.bin", stat_path, rand_doubles_);

            EXPECT_FALSE(result_set.getNextRecord());
        }

        {
            auto query = db_mgr_->createQuery("CollectionData");

            double time_val;
            query->select("TimeVal", time_val);

            // TODO: Verify the data after automatic compress/decompress
            // feature is added.

            for (auto id : {1,2}) {
                double expected_time_val = 1;
                query->resetConstraints();
                query->addConstraintForInt("CollectionID", simdb::Constraints::EQUAL, id);
                {
                    auto result_set = query->getResultSet();

                    for (size_t idx = 0; idx < 15; ++idx) {
                        EXPECT_TRUE(result_set.getNextRecord());
                        EXPECT_EQUAL(time_val, expected_time_val);
                        ++expected_time_val;
                    }

                    EXPECT_FALSE(result_set.getNextRecord());
                }
            }
        }
    }

private:
    void configCollection_()
    {
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<CounterCollectionT> ctr_collection(new CounterCollectionT("InstCounts"));

        ctr_collection->addStat("stats.num_insts_issued", std::bind(&Execute::getNumIssued, &execute_));
        ctr_collection->addStat("stats.num_insts_retired", std::bind(&Retire::getNumRetired, &retire_));

        collection_mgr->addCollection(std::move(ctr_collection));

        addCollection_<int8_t>(rand_int8s_, "stats.rand_int8s.bin", "RandInt8s", collection_mgr);
        addCollection_<int16_t>(rand_int16s_, "stats.rand_int16s.bin", "RandInt16s", collection_mgr);
        addCollection_<int32_t>(rand_int32s_, "stats.rand_int32s.bin", "RandInt32s", collection_mgr);
        addCollection_<int64_t>(rand_int64s_, "stats.rand_int64s.bin", "RandInt64s", collection_mgr);
        addCollection_<uint8_t>(rand_uint8s_, "stats.rand_uint8s.bin", "RandUInt8s", collection_mgr);
        addCollection_<uint16_t>(rand_uint16s_, "stats.rand_uint16s.bin", "RandUInt16s", collection_mgr);
        addCollection_<uint32_t>(rand_uint32s_, "stats.rand_uint32s.bin", "RandUInt32s", collection_mgr);
        addCollection_<uint64_t>(rand_uint64s_, "stats.rand_uint64s.bin", "RandUInt64s", collection_mgr);
        addCollection_<float>(rand_floats_, "stats.rand_floats.bin", "RandFloats", collection_mgr);
        addCollection_<double>(rand_doubles_, "stats.rand_doubles.bin", "RandDoubles", collection_mgr);

        db_mgr_->finalizeCollections();
    }

    template <typename DataT>
    void addCollection_(const std::array<DataT, 10> & array, const std::string& stat_path_prefix, const std::string& collection_name, simdb::Collections* collection_mgr) {
        std::unique_ptr<RandStatCollectionT<DataT>> collection(new RandStatCollectionT<DataT>(collection_name));

        for (size_t idx = 0; idx < array.size(); ++idx) {
            const auto name = stat_path_prefix + std::to_string(idx);
            collection->addStat(name, &array[idx]);
        }

        collection_mgr->addCollection(std::move(collection));
    }

    void step_(bool issue, bool retire)
    {
        ++time_;

        if (issue) {
            execute_.issue();
        }

        if (retire) {
            retire_.retire();
        }

        assignRandomVals_<int8_t>(rand_int8s_);
        assignRandomVals_<int16_t>(rand_int16s_);
        assignRandomVals_<int32_t>(rand_int32s_);
        assignRandomVals_<int64_t>(rand_int64s_);
        assignRandomVals_<uint8_t>(rand_uint8s_);
        assignRandomVals_<uint16_t>(rand_uint16s_);
        assignRandomVals_<uint32_t>(rand_uint32s_);
        assignRandomVals_<uint64_t>(rand_uint64s_);
        assignRandomVals_<float>(rand_floats_);
        assignRandomVals_<double>(rand_doubles_);

        // Collect as normal.
        EXPECT_NOTHROW(db_mgr_->getCollectionMgr()->collectAll());

        // Ensure that collecting again at any timestamp that is not monotonically
        // increasing throws an exception.
        EXPECT_THROW(db_mgr_->getCollectionMgr()->collectAll());
    }

    template <typename DataT>
    typename std::enable_if<std::is_integral<DataT>::value, void>::type
    assignRandomVals_(std::array<DataT, 10> &array)
    {
        for (auto& bin : array) {
            bin = rand() % 10;
        }
    }

    template <typename DataT>
    typename std::enable_if<std::is_floating_point<DataT>::value, void>::type
    assignRandomVals_(std::array<DataT, 10> &array)
    {
        for (auto& bin : array) {
            bin = rand() % 10 * 3.14;
        }
    }

    template <typename DataT>
    void validateStatPaths_(simdb::SqlResultIterator& result_set, int& actual_collection_id, const int expected_collection_id, const std::string& stat_path_prefix, std::string& actual_stat_path, const std::array<DataT, 10>& array)
    {
        for (size_t idx = 0; idx < array.size(); ++idx) {
            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(actual_collection_id, expected_collection_id);
            EXPECT_EQUAL(actual_stat_path, stat_path_prefix + std::to_string(idx));
        }
    }

    simdb::DatabaseManager* db_mgr_;
    Execute execute_;
    Retire retire_;
    std::array<int8_t, 10> rand_int8s_;
    std::array<int16_t, 10> rand_int16s_;
    std::array<int32_t, 10> rand_int32s_;
    std::array<int64_t, 10> rand_int64s_;
    std::array<uint8_t, 10> rand_uint8s_;
    std::array<uint16_t, 10> rand_uint16s_;
    std::array<uint32_t, 10> rand_uint32s_;
    std::array<uint64_t, 10> rand_uint64s_;
    std::array<float, 10> rand_floats_;
    std::array<double, 10> rand_doubles_;
    uint64_t time_ = 0;
};

void runNegativeTests()
{
    simdb::Schema schema;

    simdb::DatabaseManager db_mgr("negative.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    // Pretend we have some member variables for collection.
    uint64_t dummy_time = 0; 
    uint32_t dummy_data = 0;

    auto collection_mgr = db_mgr.getCollectionMgr();
    collection_mgr->useTimestampsFrom(&dummy_time);

    std::unique_ptr<CounterCollectionT> collection1(new CounterCollectionT("InstCounts"));
    std::unique_ptr<CounterCollectionT> collection2(new CounterCollectionT("InstCounts"));

    // Hang onto the collection raw pointer so we can attempt bogus API calls on it after move().
    auto collection1_ptr = collection1.get();

    // Should throw due to the stat path not being usable from python.
    EXPECT_THROW(collection1->addStat("123_invalid_python", &dummy_data));

    // Should not throw. Normal use.
    EXPECT_NOTHROW(collection1->addStat("valid_python", &dummy_data));

    // Should throw since "valid_python" is already a stat in this collection.
    EXPECT_THROW(collection1->addStat("valid_python", &dummy_data));

    // Should not throw. Normal use.
    EXPECT_NOTHROW(collection_mgr->addCollection(std::move(collection1)));

    // Should throw since we can't collect before finalizing the collections.
    EXPECT_THROW(collection_mgr->collectAll());

    // Should not throw. Normal use.
    EXPECT_NOTHROW(db_mgr.finalizeCollections());

    // Should throw since we already finalized the collection.
    EXPECT_THROW(db_mgr.finalizeCollections());

    // Should throw even with a valid stat path, since we already finalized the collections.
    EXPECT_THROW(collection1_ptr->addStat("another_valid_python", &dummy_data));

    // Should throw since we already added a collection with the same name as this one.
    EXPECT_THROW(collection_mgr->addCollection(std::move(collection2)));

    db_mgr.closeDatabase();
}

int main()
{
    DB_INIT;

    // Note that we only care about the collection data and have
    // no need for any other tables, aside from the tables that the
    // DatabaseManager adds automatically to support this feature.
    simdb::Schema schema;

    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    runNegativeTests();

    Sim sim(&db_mgr);
    sim.runSimulation();
    sim.verifyDataWithSqlQuery();
    db_mgr.closeDatabase();

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}
