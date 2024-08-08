/*
 \brief Tests for SimDB constellations feature (collecting groups of stats
 *      from all over a simulator).
 */

#include "simdb/sqlite/Constellation.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <array>

TEST_INIT;

/// This constellation uses uncompressed uint32_t counter values.
using CounterConstellationT = simdb::Constellation<uint32_t, simdb::CompressionModes::UNCOMPRESSED>;

/// This constellation uses compressed double random stat values.
template <typename DataT>
using RandStatConstellationT = simdb::Constellation<DataT, simdb::CompressionModes::COMPRESSED>;

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

/// Example simulator with Execute->Retire units. We will configure a constellation
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
        configConstellations_();

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
            auto query = db_mgr_->createQuery("Constellations");

            std::string name;
            query->select("Name", name);

            std::string data_type;
            query->select("DataType", data_type);

            int compressed;
            query->select("Compressed", compressed);

            auto result_set = query->getResultSet();

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "InstCounts");
            EXPECT_EQUAL(data_type, "uint32_t");
            EXPECT_EQUAL(compressed, 0);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt8s");
            EXPECT_EQUAL(data_type, "int8_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt16s");
            EXPECT_EQUAL(data_type, "int16_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt32s");
            EXPECT_EQUAL(data_type, "int32_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandInt64s");
            EXPECT_EQUAL(data_type, "int64_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt8s");
            EXPECT_EQUAL(data_type, "uint8_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt16s");
            EXPECT_EQUAL(data_type, "uint16_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt32s");
            EXPECT_EQUAL(data_type, "uint32_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandUInt64s");
            EXPECT_EQUAL(data_type, "uint64_t");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandFloats");
            EXPECT_EQUAL(data_type, "float");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(name, "RandDoubles");
            EXPECT_EQUAL(data_type, "double");
            EXPECT_EQUAL(compressed, 1);

            EXPECT_FALSE(result_set.getNextRecord());
        }

        {
            auto query = db_mgr_->createQuery("ConstellationPaths");

            int constellation_id;
            query->select("ConstellationID", constellation_id);

            std::string stat_path;
            query->select("StatPath", stat_path);

            auto result_set = query->getResultSet();

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(constellation_id, 1);
            EXPECT_EQUAL(stat_path, "stats.num_insts_issued");

            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(constellation_id, 1);
            EXPECT_EQUAL(stat_path, "stats.num_insts_retired");

            validateStatPaths_(result_set, constellation_id, 2, "stats.rand_int8s.bin", stat_path, rand_int8s_);
            validateStatPaths_(result_set, constellation_id, 3, "stats.rand_int16s.bin", stat_path, rand_int16s_);
            validateStatPaths_(result_set, constellation_id, 4, "stats.rand_int32s.bin", stat_path, rand_int32s_);
            validateStatPaths_(result_set, constellation_id, 5, "stats.rand_int64s.bin", stat_path, rand_int64s_);
            validateStatPaths_(result_set, constellation_id, 6, "stats.rand_uint8s.bin", stat_path, rand_uint8s_);
            validateStatPaths_(result_set, constellation_id, 7, "stats.rand_uint16s.bin", stat_path, rand_uint16s_);
            validateStatPaths_(result_set, constellation_id, 8, "stats.rand_uint32s.bin", stat_path, rand_uint32s_);
            validateStatPaths_(result_set, constellation_id, 9, "stats.rand_uint64s.bin", stat_path, rand_uint64s_);
            validateStatPaths_(result_set, constellation_id, 10, "stats.rand_floats.bin", stat_path, rand_floats_);
            validateStatPaths_(result_set, constellation_id, 11, "stats.rand_doubles.bin", stat_path, rand_doubles_);

            EXPECT_FALSE(result_set.getNextRecord());
        }

        {
            auto query = db_mgr_->createQuery("ConstellationData");

            double time_val;
            query->select("TimeVal", time_val);

            // TODO: Verify the data after automatic compress/decompress
            // feature is added.

            for (auto id : {1,2}) {
                double expected_time_val = 1;
                query->resetConstraints();
                query->addConstraintForInt("ConstellationID", simdb::Constraints::EQUAL, id);
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
    void configConstellations_()
    {
        auto constellation_mgr = db_mgr_->getConstellationMgr();
        constellation_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<CounterConstellationT> ctr_constellation(new CounterConstellationT("InstCounts"));

        ctr_constellation->addStat("stats.num_insts_issued", std::bind(&Execute::getNumIssued, &execute_));
        ctr_constellation->addStat("stats.num_insts_retired", std::bind(&Retire::getNumRetired, &retire_));

        constellation_mgr->addConstellation(std::move(ctr_constellation));

        addConstellation_<int8_t>(rand_int8s_, "stats.rand_int8s.bin", "RandInt8s", constellation_mgr);
        addConstellation_<int16_t>(rand_int16s_, "stats.rand_int16s.bin", "RandInt16s", constellation_mgr);
        addConstellation_<int32_t>(rand_int32s_, "stats.rand_int32s.bin", "RandInt32s", constellation_mgr);
        addConstellation_<int64_t>(rand_int64s_, "stats.rand_int64s.bin", "RandInt64s", constellation_mgr);
        addConstellation_<uint8_t>(rand_uint8s_, "stats.rand_uint8s.bin", "RandUInt8s", constellation_mgr);
        addConstellation_<uint16_t>(rand_uint16s_, "stats.rand_uint16s.bin", "RandUInt16s", constellation_mgr);
        addConstellation_<uint32_t>(rand_uint32s_, "stats.rand_uint32s.bin", "RandUInt32s", constellation_mgr);
        addConstellation_<uint64_t>(rand_uint64s_, "stats.rand_uint64s.bin", "RandUInt64s", constellation_mgr);
        addConstellation_<float>(rand_floats_, "stats.rand_floats.bin", "RandFloats", constellation_mgr);
        addConstellation_<double>(rand_doubles_, "stats.rand_doubles.bin", "RandDoubles", constellation_mgr);

        db_mgr_->finalizeConstellations();
    }

    template <typename DataT>
    void addConstellation_(const std::array<DataT, 10> & array, const std::string& stat_path_prefix, const std::string& constellation_name, simdb::Constellations* constellation_mgr) {
        std::unique_ptr<RandStatConstellationT<DataT>> constellation(new RandStatConstellationT<DataT>(constellation_name));

        for (size_t idx = 0; idx < array.size(); ++idx) {
            const auto name = stat_path_prefix + std::to_string(idx);
            constellation->addStat(name, &array[idx]);
        }

        constellation_mgr->addConstellation(std::move(constellation));
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

        db_mgr_->getConstellationMgr()->collectConstellations();
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
    void validateStatPaths_(simdb::SqlResultIterator& result_set, int& actual_constellation_id, const int expected_constellation_id, const std::string& stat_path_prefix, std::string& actual_stat_path, const std::array<DataT, 10>& array)
    {
        for (size_t idx = 0; idx < array.size(); ++idx) {
            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(actual_constellation_id, expected_constellation_id);
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

    auto constellation_mgr = db_mgr.getConstellationMgr();
    constellation_mgr->useTimestampsFrom(&dummy_time);

    std::unique_ptr<CounterConstellationT> constellation1(new CounterConstellationT("InstCounts"));
    std::unique_ptr<CounterConstellationT> constellation2(new CounterConstellationT("InstCounts"));

    // Hang onto the constellation raw pointer so we can attempt bogus API calls on it after move().
    auto constellation1_ptr = constellation1.get();

    // Should throw due to the stat path not being usable from python.
    EXPECT_THROW(constellation1->addStat("123_invalid_python", &dummy_data));

    // Should not throw. Normal use.
    EXPECT_NOTHROW(constellation1->addStat("valid_python", &dummy_data));

    // Should throw since "valid_python" is already a stat in this constellation.
    EXPECT_THROW(constellation1->addStat("valid_python", &dummy_data));

    // Should not throw. Normal use.
    EXPECT_NOTHROW(constellation_mgr->addConstellation(std::move(constellation1)));

    // Should throw since we can't collect before finalizing the constellations.
    EXPECT_THROW(constellation_mgr->collectConstellations());

    // Should not throw. Normal use.
    EXPECT_NOTHROW(db_mgr.finalizeConstellations());

    // Should throw since we already finalized the constellations.
    EXPECT_THROW(db_mgr.finalizeConstellations());

    // Should throw even with a valid stat path, since we already finalized the constellations.
    EXPECT_THROW(constellation1_ptr->addStat("another_valid_python", &dummy_data));

    // Should throw since we already added a constellation with the same name as this one.
    EXPECT_THROW(constellation_mgr->addConstellation(std::move(constellation2)));

    db_mgr.closeDatabase();
}

int main()
{
    DB_INIT;

    // Note that we only care about the constellation data and have
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
