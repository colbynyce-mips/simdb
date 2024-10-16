// <DatabaseManager> -*- C++ -*-

#pragma once

#include "simdb3/schema/SchemaDef.hpp"
#include "simdb3/collection/CollectionBase.hpp"
#include "simdb3/sqlite/SQLiteConnection.hpp"
#include "simdb3/sqlite/SQLiteQuery.hpp"
#include "simdb3/sqlite/SQLiteTable.hpp"
#include "simdb3/utils/PerfDiagnostics.hpp"

namespace simdb3
{

/*!
 * \class DatabaseManager
 *
 * \brief This class is the primary "entry point" into SimDB connections,
 *        including instantiating schemas, creating records, querying
 *        records, and accessing the underlying SQLiteConnection.
 */
class DatabaseManager
{
public:
    /// \brief Construct a DatabaseManager with a database directory and a filename.
    /// \param db_file Name of the database file, typically with .db extension
    /// \param force_new_file Force the <db_file> to be overwritten if it exists.
    ///                       If the file already existed and this flag is false, 
    ///                       then you will not be able to call createDatabaseFromSchema()
    ///                       or appendSchema(). The schema is considered read-only for
    ///                       previously existing database files.
    DatabaseManager(const std::string& db_file = "sim.db", const bool force_new_file = false)
        : db_file_(db_file)
    {
        std::ifstream fin(db_file);
        if (fin.good()) {
            if (force_new_file) {
                fin.close();
                const auto cmd = "rm -f " + db_file;
                auto rc = system(cmd.c_str());
                (void)rc;
            } else {
                if (!connectToExistingDatabase_(db_file)) {
                    throw DBException("Unable to connect to database file: ") << db_file;
                }
                append_schema_allowed_ = false;
            }
        }
    }

    /// You must explicitly call closeDatabase() prior to deleting
    /// the DatabaseManager to close the sqlite3 connection and to
    /// stop the AsyncTaskQueue thread if it is still running.
    ///
    /// Also write performance diagnostics to stdout if profiling was
    /// enabled, and writeProfileReport() was never explicitly
    /// called.
    ~DatabaseManager()
    {
        if (db_conn_) {
            std::cout << "You must call DatabaseManager::closeDatabase() "
                      << "before it goes out of scope!" << std::endl;
            std::terminate();
        }

        if (perf_diagnostics_ && !perf_diagnostics_->reportWritten()) {
            perf_diagnostics_->writeReport(std::cout);
        }
    }

    /// \brief  Using a Schema object for your database, construct
    ///         the physical database file and open the connection.
    ///
    /// \param profile Pass in TRUE if you want to enable SimDB
    ///                performance diagnostics. You will be responsible
    ///                for calling enterSimPhase() at the appropriate times
    ///                if you want the resulting report to separate DB usage
    ///                based on setup, simloop, and teardown phases.
    ///
    /// \throws This will throw an exception for DatabaseManager's
    ///         whose connection was initialized with a previously
    ///         existing file.
    ///
    /// \return Returns true if successful, false otherwise.
    bool createDatabaseFromSchema(const Schema& schema, const bool profile = false)
    {
        if (!append_schema_allowed_) {
            throw DBException("Cannot alter schema if you created a DatabaseManager with an existing file.");
        }

        db_conn_.reset(new SQLiteConnection(this));
        schema_ = schema;

        assertNoDatabaseConnectionOpen_();
        createDatabaseFile_();

        db_conn_->realizeSchema(schema_);

        if (db_conn_->isValid() && profile) {
            perf_diagnostics_.reset(new PerfDiagnostics);
            db_conn_->enableProfiling(perf_diagnostics_.get());
        }

        return db_conn_->isValid();
    }

    /// \brief   After calling createDatabaseFromSchema(), you may
    ///          add additional tables with this method.
    ///
    /// \throws  This will throw an exception if the schema was invalid
    ///          for any reason.
    ///
    /// \throws  This will throw an exception for DatabaseManager's
    ///          whose connection was initialized with a previously
    ///          existing file.
    ///
    /// \return  Returns true if successful, false otherwise.
    bool appendSchema(const Schema& schema)
    {
        if (!db_conn_) {
            return false;
        } else if (!db_conn_->isValid()) {
            throw DBException("Attempt to append schema tables to a DatabaseManager that does not have a valid database connection");
        }

        if (!append_schema_allowed_) {
            throw DBException("Cannot alter schema if you created a DatabaseManager with an existing file.");
        }

        db_conn_->realizeSchema(schema);
        schema_.appendSchema(schema);
        return true;
    }

    /// Get the full database file path.
    const std::string& getDatabaseFilePath() const
    {
        return db_filepath_;
    }

    /// Get the SQLiteTransaction for safeTransaction() as well
    /// as access to the async task queue (worker thread task manager).
    SQLiteTransaction* getConnection() const
    {
        return db_conn_.get();
    }

    /// Get access to the collections for this database.
    Collections* getCollectionMgr()
    {
        if (!db_conn_) {
            return nullptr;
        }

        if (!collections_) {
            collections_.reset(new Collections(this, db_conn_.get()));
        }

        return collections_.get();
    }

    /// Call this one time after all collections have been configured.
    void finalizeCollections()
    {
        if (!collections_) {
            return;
        }

        Schema schema;
        collections_->defineSchema(schema);
        appendSchema(schema);

        collections_->finalizeCollections_();

        std::string time_type;
        if (collections_->timestamp_->getDataType() == SqlDataType::double_t) {
            time_type = "REAL";
        } else {
            time_type = "INT";
        }

        INSERT(SQL_TABLE("CollectionGlobals"), SQL_COLUMNS("TimeType", "Heartbeat"), SQL_VALUES(time_type, (int)collections_->getHeartbeat()));
    }

    /// Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc& func) const
    {
        db_conn_->safeTransaction(func);
    }

    /// \brief  Perform INSERT operation on this database.
    ///
    /// \note   The way to call this method is:
    ///         db_mgr.INSERT(SQL_TABLE("TableName"),
    ///                       SQL_COLUMNS("ColA", "ColB"),
    ///                       SQL_VALUES(3.14, "foo"));
    ///
    /// \note   You may also provide ValueContainerBase subclasses in the SQL_VALUES.
    ///
    /// \return SqlRecord which wraps the table and the ID of its record.
    std::unique_ptr<SqlRecord> INSERT(SqlTable&& table, SqlColumns&& cols, SqlValues&& vals)
    {
        std::unique_ptr<SqlRecord> record;

        db_conn_->safeTransaction([&](){
            std::ostringstream oss;
            oss << "INSERT INTO " << table.getName();
            cols.writeColsForINSERT(oss);
            vals.writeValsForINSERT(oss);

            std::string cmd = oss.str();
            auto stmt = db_conn_->prepareStatement(cmd);
            vals.bindValsForINSERT(stmt);

            auto rc = SQLiteReturnCode(sqlite3_step(stmt));
            if (rc != SQLITE_DONE) {
                throw DBException("Could not perform INSERT. Error: ") << sqlite3_errmsg(db_conn_->getDatabase());
            }

            auto db_id = db_conn_->getLastInsertRowId();
            record.reset(new SqlRecord(table.getName(), db_id, db_conn_->getDatabase(), db_conn_.get()));
            return true;
        });

        return record;
    }

    /// This INSERT() overload is to be used for tables that were defined with
    /// at least one default value for its column(s).
    std::unique_ptr<SqlRecord> INSERT(SqlTable&& table)
    {
        std::unique_ptr<SqlRecord> record;

        db_conn_->safeTransaction([&](){
            const std::string cmd = "INSERT INTO " + table.getName() + " DEFAULT VALUES";
            auto stmt = db_conn_->prepareStatement(cmd);

            auto rc = SQLiteReturnCode(sqlite3_step(stmt));
            if (rc != SQLITE_DONE) {
                throw DBException("Could not perform INSERT. Error: ") << sqlite3_errmsg(db_conn_->getDatabase());
            }

            auto db_id = db_conn_->getLastInsertRowId();
            record.reset(new SqlRecord(table.getName(), db_id, db_conn_->getDatabase(), db_conn_.get()));
            return true;
        });

        return record;
    }

    /// \brief  Get a SqlRecord from a database ID for the given table.
    ///
    /// \return Returns the record wrapper if found, or nullptr if not.
    std::unique_ptr<SqlRecord> findRecord(const char* table_name, const int db_id) const
    {
        return findRecord_(table_name, db_id, false);
    }

    /// \brief  Get a SqlRecord from a database ID for the given table.
    ///
    /// \throws Throws an exception if this database ID is not found in the given table.
    std::unique_ptr<SqlRecord> getRecord(const char* table_name, const int db_id) const
    {
        return findRecord_(table_name, db_id, true);
    }

    /// \brief  Delete one record from the given table with the given ID.
    ///
    /// \return Returns true if successful, false otherwise.
    bool removeRecordFromTable(const char* table_name, const int db_id)
    {
        db_conn_->safeTransaction([&]() {
            std::ostringstream oss;
            oss << "DELETE FROM " << table_name << " WHERE Id=" << db_id;
            const auto cmd = oss.str();

            auto rc = SQLiteReturnCode(sqlite3_exec(db_conn_->getDatabase(), cmd.c_str(), nullptr, nullptr, nullptr));
            if (rc) {
                throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
            }

            return true;
        });

        return sqlite3_changes(db_conn_->getDatabase()) == 1;
    }

    /// \brief  Delete every record from the given table.
    ///
    /// \return Returns the total number of deleted records.
    uint32_t removeAllRecordsFromTable(const char* table_name)
    {
        db_conn_->safeTransaction([&]() {
            std::ostringstream oss;
            oss << "DELETE FROM " << table_name;
            const auto cmd = oss.str();

            auto rc = SQLiteReturnCode(sqlite3_exec(db_conn_->getDatabase(), cmd.c_str(), nullptr, nullptr, nullptr));
            if (rc) {
                throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
            }

            return true;
        });

        return sqlite3_changes(db_conn_->getDatabase());
    }

    /// \brief  Issue "DELETE FROM TableName" to clear out the given table.
    ///
    /// \return Returns the total number of deleted records across all tables.
    uint32_t removeAllRecordsFromAllTables()
    {
        uint32_t count = 0;

        db_conn_->safeTransaction([&]() {
            const char* cmd = "SELECT name FROM sqlite_master WHERE type='table'";
            auto stmt = db_conn_->prepareStatement(cmd);

            while (true) {
                auto rc = SQLiteReturnCode(sqlite3_step(stmt));
                if (rc != SQLITE_ROW) {
                    break;
                }

                auto table_name = sqlite3_column_text(stmt, 0);
                count += removeAllRecordsFromTable((const char*)table_name);
            }

            return true;
        });

        return count;
    }

    /// Get a query object to issue SELECT statements with constraints.
    std::unique_ptr<SqlQuery> createQuery(const char* table_name)
    {
        return std::unique_ptr<SqlQuery>(new SqlQuery(table_name, db_conn_->getDatabase()));
    }

    /// Close the sqlite3 connection and stop the AsyncTaskQueue thread
    /// if it is still running.
    void closeDatabase()
    {
        if (db_conn_) {
            db_conn_->getTaskQueue()->stopThread();
            db_conn_.reset();
        }

        if (perf_diagnostics_) {
            perf_diagnostics_->onCloseDatabase();
        }
    }

    /// \brief Write the current performance diagnostics to file.
    /// \param filename Name of the report file, or "" to print to stdout.
    /// \param title Title of the report.
    bool writeProfileReport(std::ostream& os, const std::string& title = "") const
    {
        if (!perf_diagnostics_) {
            return false;
        }

        perf_diagnostics_->writeReport(os, title);
        return true;
    }

    /// To support accurate SimDB self-profiling, update the simulation
    /// phase at the appropriate times (SETUP->SIMLOOP->TEARDOWN). This
    /// is used to write profile data separated by phase. 
    void enterSimPhase(const SimPhase phase)
    {
        if (perf_diagnostics_) {
            perf_diagnostics_->enterSimPhase(phase);
        }
    }

private:
    /// \brief  Open a database connection to an existing database file.
    ///
    /// \param db_fpath Full path to the database including the ".db"
    ///                 extension, e.g. "/path/to/my/dir/statistics.db"
    ///
    /// \note   The 'db_fpath' is typically one that was given to us
    ///         from a previous call to getDatabaseFilePath()
    ///
    /// \return Returns true if successful, false otherwise.
    bool connectToExistingDatabase_(const std::string& db_fpath)
    {
        assertNoDatabaseConnectionOpen_();
        db_conn_.reset(new SQLiteConnection(this));

        if (db_conn_->openDbFile_(db_fpath).empty()) {
            db_conn_.reset();
            db_filepath_.clear();
            return false;
        }

        db_filepath_ = db_conn_->getDatabaseFilePath();
        append_schema_allowed_ = false;
        return true;
    }

    /// Open the given database file.
    bool createDatabaseFile_()
    {
        if (!db_conn_) {
            return false;
        }

        auto db_filename = db_conn_->openDbFile_(db_file_);
        if (!db_filename.empty()) {
            //File opened without issues. Store the full DB filename.
            db_filepath_ = db_filename;
            return true;
        }

        return false;
    }

    /// This class does not currently allow one DatabaseManager
    /// to be simultaneously connected to multiple databases.
    void assertNoDatabaseConnectionOpen_() const
    {
        if (!db_conn_) {
            return;
        }

        if (db_conn_->isValid()) {
            throw DBException("A database connection has already been "
                              "made for this DatabaseManager");
        }
    }

    /// Get a SqlRecord from a database ID for the given table.
    std::unique_ptr<SqlRecord> findRecord_(const char* table_name, const int db_id, const bool must_exist) const
    {
        std::ostringstream oss;
        oss << "SELECT * FROM " << table_name << " WHERE Id=" << db_id;
        const auto cmd = oss.str();

        auto stmt = SQLitePreparedStatement(db_conn_->getDatabase(), cmd);
        auto rc = SQLiteReturnCode(sqlite3_step(stmt));

        if (must_exist && rc == SQLITE_DONE) {
            throw DBException("Record not found with ID ") << db_id << " in table " << table_name;
        } else if (rc == SQLITE_DONE) {
            return nullptr;
        } else if (rc == SQLITE_ROW) {
            return std::unique_ptr<SqlRecord>(new SqlRecord(table_name, db_id, db_conn_->getDatabase(), db_conn_.get()));
        } else {
            throw DBException("Internal error has occured: ") << sqlite3_errmsg(db_conn_->getDatabase());
        }
    }

    /// Database connection.
    std::shared_ptr<SQLiteConnection> db_conn_;

    /// Schema for this database as given to createDatabaseFromSchema()
    /// and optionally appendSchema().
    Schema schema_;

    /// Name of the database file.
    const std::string db_file_;

    /// Full database file name, including the database path
    /// and file extension
    std::string db_filepath_;

    /// Flag saying whether or not the schema can be altered.
    /// We do not allow schemas to be altered for DatabaseManager's
    /// that were initialized with a previously existing file.
    bool append_schema_allowed_ = true;

    /// Hold onto all the user-configured collections.
    std::unique_ptr<Collections> collections_;

    /// Self-profiler to help users maximize performance of SimDB
    /// with its intended use.
    std::unique_ptr<PerfDiagnostics> perf_diagnostics_;
};

/// \class CollectableSerializer
/// \brief Writes collection data on the worker thread.
class CollectableSerializer : public WorkerTask
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    CollectableSerializer(
        DatabaseManager* db_mgr, const TimestampBase* timestamp, const std::vector<char>& data, bool compressed)
        : db_mgr_(db_mgr)
        , timestamp_binder_(timestamp->createBinder())
        , data_(data)
        , compressed_(compressed)
        , unserialized_map_(StringMap::instance()->getUnserializedMap())
    {
        StringMap::instance()->clearUnserializedMap();
    }

    /// Asynchronously write the collection data to the database.
    void completeTask() override
    {
        auto record = db_mgr_->INSERT(SQL_TABLE("CollectionData"),
                                      SQL_COLUMNS("TimeVal", "DataVals", "IsCompressed"),
                                      SQL_VALUES(timestamp_binder_, data_, compressed_));

        for (const auto& kvp : unserialized_map_) {
            db_mgr_->INSERT(SQL_TABLE("StringMap"),
                            SQL_COLUMNS("IntVal", "String"),
                            SQL_VALUES(kvp.first, kvp.second));
        }
    }

    /// Get the size of this task in bytes. This is used to track the total
    /// size of the queue in the AsyncTaskQueue.
    size_t sizeInBytes() const override
    {
        return data_.size();
    }

private:
    /// DatabaseManager used for INSERT().
    DatabaseManager* db_mgr_;

    /// Timestamp binder holding the timestamp value at the time of construction.
    ValueContainerBasePtr timestamp_binder_;

    /// Data values.
    std::vector<char> data_;

    /// Whether the data is compressed or not.
    const bool compressed_;

    /// Map of uint32_t->string pairs that need to be written to the database.
    StringMap::unserialized_string_map_t unserialized_map_;
};

/// One-time finalization of all collections. Called by friend class DatabaseManager.
inline void Collections::finalizeCollections_()
{
    db_conn_->safeTransaction([&]() {
        auto root = createElementTree_();
        for (auto& collection : collections_) {
            collection->finalize(db_mgr_, root.get(), pipeline_heartbeat_);
            root.reset();
        }

        for (const auto& kvp : clk_periods_) {
            db_mgr_->INSERT(SQL_TABLE("Clocks"), SQL_COLUMNS("Name", "Period"), SQL_VALUES(kvp.first, kvp.second));
        }

        for (const auto& kvp : clks_by_location_) {
            db_mgr_->INSERT(SQL_TABLE("ElementClocks"), SQL_COLUMNS("SimPath", "ClockName"), SQL_VALUES(kvp.first, kvp.second));
        }

        return true;
    });
}

/// Called manually during simulation to trigger automatic collection of all collections.
inline void Collections::collectAll()
{
    if (!timestamp_->ensureTimeHasAdvanced()) {
        throw DBException("Cannot perform  - time has not advanced");
    }

    CollectionBuffer buffer(all_collection_data_);
    for (auto& collection : collections_) {
        collection->collect(buffer);
    }

    timestamp_->captureCurrentTime();

    size_t task_count = 0;
    if (compression_level_ > 0) {
        compressDataVec(all_collection_data_, all_compressed_data_, compression_level_);

        std::unique_ptr<WorkerTask> task(new CollectableSerializer(
            db_mgr_, timestamp_.get(), all_compressed_data_, true));

        task_count = db_mgr_->getConnection()->getTaskQueue()->addTask(std::move(task));
    } else {
        std::unique_ptr<WorkerTask> task(new CollectableSerializer(
            db_mgr_, timestamp_.get(), all_collection_data_, false));

        task_count = db_mgr_->getConnection()->getTaskQueue()->addTask(std::move(task));
    }

    if (num_tasks_highwater_mark_ == 0) {
        // Start with a highwater mark of 5 so we do not inadvertently lower the compression
        // level too soon. We want to give the worker thread a chance to catch up.
        num_tasks_highwater_mark_ = 5;
    } else if (task_count > num_tasks_highwater_mark_ && compression_level_ > 0) {
        num_tasks_highwater_mark_ = task_count * 2;
        ++num_times_highwater_mark_exceeded_;
        if (num_times_highwater_mark_exceeded_ >= 3) {
            if (compression_level_ > 1) {
                std::cout << "SimDB collections worker thread is falling behind. Lowering compression level to "
                          << compression_level_ - 1 << std::endl;
            } else {
                std::cout << "SimDB collections worker thread is falling behind. Disabling compression." << std::endl;
            }
            --compression_level_;
            num_times_highwater_mark_exceeded_ = 0;
            num_tasks_highwater_mark_ = 0;
        }
    } else if (task_count > num_tasks_highwater_mark_) {
        auto task_queue = db_mgr_->getConnection()->getTaskQueue();
        if (task_queue->enableAutoFlush(100 * 1024 * 1024)) {
            std::cout << "SimDB collections is already at the fastest compression level, but the "
                      << "worker thread is still not able to keep up. The worker queue will be flushed "
                      << "whenever the backlog consumes more than 100MB." << std::endl;
        }
    }
}

} // namespace simdb3
