/*
 \brief Tests for SimDB self-profiling feature to help users understand AsyncTaskQueue and safeTransaction().
 */

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

struct SimControls
{
    size_t NUM_METADATA_ROWS = 10;
    size_t NUM_SIMLOOP_TIME_STEPS = 10;
    size_t NUM_SIMLOOP_SLEEP_MS = 10;

    void fullTest()
    {
        NUM_METADATA_ROWS = 100;
        NUM_SIMLOOP_TIME_STEPS = 1000;
    }
};

SimControls sim_controls;

class Sim
{
public:
    Sim(const std::string& db_path)
        : db_mgr_(db_path)
    {
        simdb::Schema schema;
        using dt = simdb::SqlDataType;
        
        schema.addTable("Metadata")
            .addColumn("Name", dt::string_t)
            .addColumn("Number", dt::int32_t);

        schema.addTable("Stats")
            .addColumn("Time", dt::int32_t)
            .addColumn("Data", dt::blob_t);

        // Pass in TRUE to enable profiling.
        db_mgr_.createDatabaseFromSchema(schema, true);
    }

    void setup(const bool in_transaction)
    {
        db_mgr_.enterSimPhase(simdb::SimPhase::SETUP);

        // Write a bunch of metadata. This is most commonly done on the main thread,
        // not by a simdb::WorkerTask subclass. We typically send off only the high-
        // volumn data (usually blobs) to the worker thread during the sim loop.

        auto write_metadata = [&]() {
            for (size_t idx = 0; idx < sim_controls.NUM_METADATA_ROWS; ++idx) {
                db_mgr_.INSERT(SQL_TABLE("Metadata"), SQL_COLUMNS("Name","Number"), SQL_VALUES("HelloWorld", 333));
            }
            return true;
        };

        // The performance difference between putting a lot of work inside safeTransaction()
        // and not doing so can be large.
        if (in_transaction) {
            db_mgr_.safeTransaction(write_metadata);
        } else {
            write_metadata();
        }
    }

    void run(const bool async)
    {
        db_mgr_.enterSimPhase(simdb::SimPhase::SIMLOOP);

        time_ = 0;
        while (time_++ < sim_controls.NUM_SIMLOOP_TIME_STEPS) {
            std::unique_ptr<simdb::WorkerTask> task(new StatsWriter(&db_mgr_, time_));

            // There is no good way to put database work inside a safeTransaction()
            // in the sim loop. You cannot realistically put the entire simulation
            // inside one transaction. So our choices are to either write the data
            // now, or send it to the worker thread for background processing. The
            // worker thread approach can be MUCH faster.
            if (async) {
                db_mgr_.getConnection()->getTaskQueue()->addTask(std::move(task));
            } else {
                task->completeTask();
            }

            // Sleep for a little bit to make this simulator more realistic.
            std::this_thread::sleep_for(std::chrono::milliseconds(sim_controls.NUM_SIMLOOP_SLEEP_MS));
        }

        if (async) {
            db_mgr_.getConnection()->getTaskQueue()->stopThread();
        }
    }

    void teardown(const bool in_transaction)
    {
        db_mgr_.enterSimPhase(simdb::SimPhase::TEARDOWN);

        // Update all the records we created during the SETUP phase.
        auto update_metadata = [&]() {
            auto query = db_mgr_.createQuery("Metadata");

            int id;
            query->select("Id", id);

            auto result_set = query->getResultSet();
            while (result_set.getNextRecord()) {
                auto record = db_mgr_.getRecord("Metadata", id);
                record->setPropertyString("Name","GoodbyeWorld");
                record->setPropertyInt32("Number", 444);
            }
        };

        if (in_transaction) {
            db_mgr_.safeTransaction([&]() {
                update_metadata();
                return true;
            });
        } else {
            update_metadata();
        }

        // Done with the database.
        db_mgr_.closeDatabase();
    }

    void writeProfile(const std::string& title)
    {
        db_mgr_.writeProfileReport(std::cout, title);
    }

private:
    class StatsWriter : public simdb::WorkerTask
    {
    public:
        StatsWriter(simdb::DatabaseManager* db_mgr, const uint32_t time)
            : db_mgr_(db_mgr)
            , time_(time)
        {}

        void completeTask() override
        {
            // Use a static vector to remove the performance overhead of allocation.
            static const std::vector<double> stats(1000, 3.14);

            db_mgr_->INSERT(SQL_TABLE("Stats"), SQL_COLUMNS("Time","Data"), SQL_VALUES(time_, stats));
        }

    private:
        simdb::DatabaseManager* db_mgr_;
        uint32_t time_;
    };

    simdb::DatabaseManager db_mgr_;
    uint32_t time_;
};

int main(int argc, char** argv)
{
    DB_INIT;

    if (argc > 1 && std::string(argv[1]) == "full") {
        sim_controls.fullTest();
    }

    // First start by misusing SimDB and purposefully avoid the highest-impact
    // performance utilities: safeTransaction() and AsyncTaskQueue.
    Sim slow_sim("slow.db");
    slow_sim.setup(false);    // Not in safeTransaction()
    slow_sim.run(false);      // Not using AsyncTaskQueue
    slow_sim.teardown(false); // Not in safeTransaction()

    // Now run another simulation using SimDB the way it is intended for
    // best performance.
    Sim fast_sim("fast.db");
    fast_sim.setup(true);     // Use safeTransaction()
    fast_sim.run(true);       // Use AsyncTaskQueue
    fast_sim.teardown(true);  // Use safeTransaction()

    // Write the performance results to stdout.
    std::cout << std::endl;
    slow_sim.writeProfile("SLOW SIMULATION");

    std::cout << std::endl;
    fast_sim.writeProfile("FAST SIMULATION");
}
