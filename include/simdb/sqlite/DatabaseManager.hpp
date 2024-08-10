// <DatabaseManager> -*- C++ -*-

#pragma once

#include "simdb/schema/Schema.hpp"
#include "simdb/collection/CollectionBase.hpp"
#include "simdb/sqlite/SQLiteConnection.hpp"
#include "simdb/sqlite/SQLiteQuery.hpp"
#include "simdb/sqlite/SQLiteTable.hpp"
#include "simdb/utils/PerfDiagnostics.hpp"

namespace simdb
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
        if (collections_->timestamp_->getDataType() == ColumnDataType::double_t) {
            time_type = "REAL";
        } else {
            time_type = "INT";
        }

        INSERT(SQL_TABLE("CollectionGlobals"), SQL_COLUMNS("TimeType"), SQL_VALUES(time_type));
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

} // namespace simdb
