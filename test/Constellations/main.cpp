/*
 \brief Tests for SimDB constellations feature (collecting groups of stats
 *      from all over a simulator).
 */

#include "simdb/sqlite/Constellation.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <array>

TEST_INIT;

/// This constellation uses async mode with uncompressed uint32_t counter values and uint32_t time values.
using CounterConstellationT = simdb::Constellation<uint32_t, uint32_t, simdb::AsyncModes::ASYNC, simdb::CompressionModes::UNCOMPRESSED>;

/// This constellation uses async mode with compressed double random stat values and uint32_t time values.
using RandStatConstellationT = simdb::Constellation<double, uint32_t, simdb::AsyncModes::ASYNC, simdb::CompressionModes::COMPRESSED>;

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

    void run()
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
    }

private:
    void configConstellations_()
    {
        auto constellation_mgr = db_mgr_->getConstellationMgr();

        std::unique_ptr<CounterConstellationT> ctr_constellation(new CounterConstellationT("InstCounts", &time_));

        ctr_constellation->addStat("stats.num_insts_issued", std::bind(&Execute::getNumIssued, &execute_));
        ctr_constellation->addStat("stats.num_insts_retired", std::bind(&Retire::getNumRetired, &retire_));

        constellation_mgr->addConstellation(std::move(ctr_constellation));

        std::unique_ptr<RandStatConstellationT> rnd_constellation(new RandStatConstellationT("RandStats", &time_));

        for (size_t idx = 0; idx < rand_stats_.size(); ++idx) {
            const auto name = "stats.rand" + std::to_string(idx);
            rnd_constellation->addStat(name, &rand_stats_[idx]);
        }

        constellation_mgr->addConstellation(std::move(rnd_constellation));

        constellation_mgr->finalizeConstellations();
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

        for (auto& val : rand_stats_) {
            val = rand() % 30 * 3.14;
        }

        db_mgr_->getConstellationMgr()->collectConstellations();
    }

    simdb::DatabaseManager* db_mgr_;
    Execute execute_;
    Retire retire_;
    std::array<double, 100> rand_stats_;
    uint32_t time_ = 0;
};

int main()
{
    DB_INIT;

    // Note that we only care about the constellation data and have
    // no need for any other tables, aside from the tables that the
    // DatabaseManager adds automatically to support this feature.
    simdb::Schema schema;

    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    Sim sim(&db_mgr);
    sim.run();

    db_mgr.getConnection()->getTaskQueue()->stopThread();

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}

// validate incoming stat names
// validate with Query
// python module
