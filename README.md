# Introduction to SimDB
This is a C++11 SQLite database module which intends to address common use cases encountered in simulation systems:

* [Sparta](https://github.com/sparcians/map)
* [SystemC](https://github.com/accellera-official/systemc)
* [gem5](https://github.com/gem5/gem5)
* etc.

SimDB features include:

* Ease of use over sqlite3 C API
* Background worker thread to perform tasks asynchronously
* Automatic system-wide data collection with a single API call in the sim loop
* Front-end viewer to analyze collected data

# Code examples

## Database creation

    using dt = simdb::SqlDataType;

    simdb::Schema schema;

    schema.addTable("SimHierarchy")
        .addColumn("ParentNodeID", dt::int32_t)
        .addColumn("NodeName", dt::string_t)
        .createIndexOn("ParentNodeID");

    schema.addTable("SnapshotData")
        .addColumn("Cycle", dt::uint64_t)
        .addColumn("SimHierNodeID", dt::int32_t)
        .addColumn("RawData", dt::blob_t)
        .createCompoundIndexOn(SQL_COLUMNS("Cycle", "SimHierNodeID"));

    simdb::DatabaseManager db_mgr("sim.db");
    db_mgr.createDatabaseFromSchema(schema);

## INSERT

    // struct SimHierNode {
    //     std::string name;
    //     std::vector<std::unique_ptr<SimHierNode>> children;
    //     int db_id;
    // };

    void writeHierarchy(simdb::DatabaseManager& db_mgr, SimHierNode& node, int parent_db_id = 0)
    {
        auto record = db_mgr.INSERT(SQL_TABLE("SimHierarchy"),
                                    SQL_COLUMNS("ParentNodeID", "NodeName"),
                                    SQL_VALUES(parent_db_id, node.name));

        node.db_id = record->getID();

        for (auto& child : node.children) {
            writeHierarchy(db_mgr, *child, record->getId());
        }
    }

## SELECT

    // SELECT Cycle,RawData FROM SnapshotData WHERE Cycle>50 AND Cycle<=100 AND SimHierNodeID=309

    auto query = db_mgr.createQuery("SnapshotData");

    uint64_t cycle;
    query->select("Cycle", cycle);

    std::vector<double> raw_data;
    query->select("RawData", raw_data);

    query->addConstraintForInt("Cycle", simdb::Constraints::GREATER, 50);
    query->addConstraintForInt("Cycle", simdb::Constraints::LESS_EQUAL, 100);
    query->addConstraintForInt("SimHierNodeID", simdb::Constraints::EQUAL, 309);

    auto result_set = query->getResultSet();
    while (result_set.getNextRecord()) {
        std::cout << "Found " << raw_data.size() << " stats at cycle " << cycle << "\n";
    }

## UPDATE

    // UPDATE SimHierarchy NodeName="root" WHERE Id=1
    auto record = db_mgr.getRecord("SimHierarchy", 1);
    record->setPropertyString("NodeName", "root");
    
## Async mode

Perform high-volume data writes on the worker thread. This is highly
encouraged for maximum performance.

    // class SimLoopStatsWriter : public simdb::WorkerTask
    // {
    // public:
    //     SimLoopStatsWriter(simdb::DatabaseManager& db_mgr,
    //                        const uint64_t cycle,
    //                        const SimHierNode& node,
    //                        const std::vector<double>& stats)
    //         : db_mgr_(db_mgr)
    //         , cycle_(cycle)
    //         , node_id_(node.db_id)
    //         , stats_(stats)
    //     {}
    // 
    //     void completeTask() override {
    //         db_mgr_.INSERT(SQL_TABLE("SnapshotData"),
    //                        SQL_COLUMNS("Cycle", "SimHierNodeID", "RawData"),
    //                        SQL_VALUES(cycle_, node_id_, stats_));
    //     }
    // 
    // private:
    //     simdb::DatabaseManager& db_mgr_;
    //     const uint64_t cycle_;
    //     const int node_id_;
    //     const std::vector<double> stats_;
    // };

    auto task_queue = db_mgr.getConnection()->getTaskQueue();

    while (Scheduler::instance()->step()) {
        std::unique_ptr<simdb::WorkerTask> task(new SimLoopStatsWriter(...));
        task_queue->addTask(std::move(task)));
    }

    db_mgr.closeDatabase();

## Automatic collection

One of SimDB's most powerful features is automatic data collection.
* POD types, enums, strings
* Structs
* Containers of structs (by value, pointer, or smart pointer)

A simple example is provided below for brevity. See the detailed examples in the test
directories (test/Collection/**).

    std::string getHierLocation(const SimHierNode* hier_node)
    {
        std::string loc;
        // Walk up the hierarchy to get a dot-delimited location e.g. "root.core0.lsu.stats.foo"
        return loc;
    }

    struct NodeStat {
        SimHierNode* hier_node;
        double val;
    };

    // Setup the collection
    auto collection_mgr = db_mgr.getCollectionMgr();

    // Use T* backpointers or a std::function<T()> to capture the collection time values.
    // Both integral (discrete time) and floating-point (continuous time) are supported.
    // Here we assume we have a uint64_t member variable called "cycle_" holding the current tick.
    collection_mgr->useTimestampsFrom(&cycle_);

    using StatCollection = simdb::StatCollection<double>;
    std::unique_ptr<StatCollection> collection(new StatCollection("AllStats"));

    // Assume we have many NodeStat structs all over the simulator.
    for (const NodeStat& node_stat : all_node_stats) {
        SimHierNode* hier_node = node_stat.hier_node;
        std::string loc = getHierLocation(hier_node);
        collection->addStat(loc, &node_stat.val);
    }

    collection_mgr->addCollection(std::move(collection));
    db_mgr.finalizeCollections();

// Later in the sim loop...

    void Scheduler::run()
    {
        while (++time_ < 1000) {
            step();
            db_mgr_.getCollectionMgr()->collectAll();
        }
    }

# Python Deserializer

You can use the simdb_collections.py module to write your own post-processing
tools, such as stats CSV reports.

    # Create a CSV report of all stats, with this format:
    #   Time,1,2,3,4,5
    #   root.core0.lsu.stats.foo,33,22,55,44,22
    #   root.core0.fpu.stats.bar,33,22,55,44,22
    #   ...

    from simdb_collections import Collections

    collections = Collections('sim.db')
    data_json = collections.Unpack()
    
    with open('stats.csv', 'w') as fout:
        time_vals = data_json['TimeVals']
        stats_json = data_json['DataVals']

        fout.write('Time,')
        fout.write(','.join([str(t) for t in time_vals]))
        fout.write('\n')

        for stat_path, stat_vals in stats_json.items():
            fout.write(stat_path + ',')
            fout.write(','.join([str(val) for val in stat_vals]))
            fout.write('\n')

# Argos viewer

SimDB provides a collection viewer called "Argos" which can be launched with this command:

    python3 python/argos.py --database <path/to/your/sim.db>

All Argos documentation is found in the 'python' subdirectory README.

# Dependencies

The following dependencies must be installed to use SimDB (sudo apt install, conda, etc.)

* sqlite3
* zlib
* rapidjson
* wxPython (for Argos viewer only)