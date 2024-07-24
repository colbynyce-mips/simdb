// <DatabaseManager> -*- C++ -*-

#pragma once

#include "simdb/schema/Schema.hpp"
#include "simdb/sqlite/SQLiteConnection.hpp"
#include "simdb/sqlite/SQLiteTable.hpp"
#include "simdb/utils/uuids.hpp"
#include "simdb_fwd.hpp"

#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>

namespace simdb {

/*!
 * \brief Database manager class. Used in order to create
 * databases with a user-specified schema, and connect to
 * existing databases that were previously created with
 * another DatabaseManager.
 */
class DatabaseManager
{
public:
    //! Construct an DatabaseManager. This does not open any
    //! database connection or create any database just yet.
    //! The database path that you pass in is wherever you
    //! want the database to ultimately live.
    DatabaseManager(const std::string & db_dir = ".")
        : db_dir_(db_dir)
    {
    }

    //! Using a Schema object for your database, construct
    //! the physical database file and open the connection.
    //!
    //! Returns true if successful, false otherwise.
    bool createDatabaseFromSchema(Schema & schema)
    {
        schema.finalizeSchema_();
        db_conn_.reset(new SQLiteConnection(this));
        schema_ = schema;

        openDatabaseWithoutSchema_();
        db_conn_->realizeSchema(schema_, *this);
        return db_conn_->isValid();
    }

    //! After calling createDatabaseFromSchema(), you may
    //! add additional tables with this method. If a table
    //! has the same name as an existing table in this database,
    //! all of the table columns need to match exactly as
    //! well, or this method will throw. If the columns
    //! match however, the table will be ignored as it
    //! already exists in the schema.
    //!
    //! Returns true if the provided schema's tables were
    //! successfuly added to this DatabaseManager's schema,
    //! otherwise returns false.
    bool appendSchema(Schema & schema)
    {
        if (!db_conn_) {
            return false;
        } else if (!db_conn_->isValid()) {
            throw DBException("Attempt to append schema tables to ")
                << "an DatabaseManager that does not have a valid "
                << "database connection";
        }

        schema.finalizeSchema_();
        db_conn_->realizeSchema(schema, *this);
        schema_ += schema;
        return true;
    }

    //! Open a database connection to an existing database
    //! file. The 'db_file' that you pass in should be the
    //! full database path, including the file name and
    //! extension. For example, "/path/to/my/dir/statistics.db"
    //!
    //! This 'db_file' is typically one that was given to you
    //! from a previous call to getDatabaseFullFilename()
    //!
    //! Returns true if successful, false otherwise.
    bool connectToExistingDatabase(const std::string & db_file)
    {
        assertNoDatabaseConnectionOpen_();
        db_conn_.reset(new SQLiteConnection(this));

        if (!db_conn_->connectToExistingDatabase(db_file)) {
            db_conn_.reset();
            db_full_filename_.clear();
            return false;
        }

        db_full_filename_ = db_conn_->getDatabaseFullFilename();
        return true;
    }

    //! Get the full database file name, including its path and
    //! file extension. If the database has not been opened or
    //! created yet, this will just return the database path.
    const std::string & getDatabaseFullFilename() const
    {
        return db_full_filename_;
    }

    //! Get the internal database proxy. Will return nullptr
    //! if no database connection has been made yet.
    SQLiteConnection * getConnection() const
    {
        return db_conn_.get();
    }

    //! Open database connections will be closed when the
    //! destructor is called.
    ~DatabaseManager() = default;

    //! Get the schema this DatabaseManager is using.
    Schema & getSchema()
    {
        return schema_;
    }

    //! Get the schema this DatabaseManager is using.
    const Schema & getSchema() const
    {
        return schema_;
    }

    //! Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc & func) const
    {
        db_conn_->safeTransaction(func);
    }

    //! Perform INSERT operation on this database. The way to
    //! call this method is:
    //!
    //! db_mgr.INSERT(SQL_TABLE("TableName"),
    //!               SQL_COLUMNS("ColA", "ColB"),
    //!               SQL_VALUES(3.14, "foo"));
    //!
    //! The returned value is the database ID of the created record.
    std::unique_ptr<SqlRecord> INSERT(SqlTable &&table, SqlColumns &&cols, SqlValues &&vals)
    {
        std::ostringstream oss;
        oss << "INSERT INTO " << table.getName();
        cols.writeColsForINSERT(oss);
        vals.writeValsForINSERT(oss);

        std::string cmd = oss.str();
        sqlite3_stmt * stmt = db_conn_->prepareStatement(cmd);
        vals.bindValsForINSERT(stmt);

        auto rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            throw DBException("Could not perform INSERT. Error code: ") << rc;
        }

        auto db_id = db_conn_->getLastInsertRowId();
        return std::unique_ptr<SqlRecord>(new SqlRecord(table.getName(), db_id, db_conn_->getDatabase()));
    }

    //! This is similar to the above INSERT() method, and will result in a
    //! record created with default values. These values are either explicitly
    //! set with setDefaultValue() during schema creation, or they are uninitialized
    //! and may be garbage and unsafe to read.
    std::unique_ptr<SqlRecord> INSERT(SqlTable &&table)
    {
        const std::string cmd = "INSERT INTO " + table.getName() + " DEFAULT VALUES";
        sqlite3_stmt * stmt = db_conn_->prepareStatement(cmd);

        auto rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            throw DBException("Could not perform INSERT. Error code: ") << rc;
        }

        auto db_id = db_conn_->getLastInsertRowId();
        return std::unique_ptr<SqlRecord>(new SqlRecord(table.getName(), db_id, db_conn_->getDatabase()));
    }

    // xxx yyy colby nyce todo
    // give an overload which doesn't take SQL_COLUMNS

private:
    //! Open the given database file. If the connection is
    //! successful, this file will be the DatabaseManager's
    //! "db_full_filename_" value.
    bool openDbFile_(const std::string & db_file,
                     const bool create_file)
    {
        if (!db_conn_) {
            return false;
        }

        auto db_filename = db_conn_->openDbFile_(db_dir_, db_file, create_file);
        if (!db_filename.empty()) {
            //File opened without issues. Store the full DB filename.
            db_full_filename_ = db_filename;
            return true;
        }

        return false;
    }

    //! Try to just open an empty database file. This is
    //! similar to fopen().
    void openDatabaseWithoutSchema_()
    {
        assertNoDatabaseConnectionOpen_();
        auto db_file = generateUUID() + ".db";
        openDbFile_(db_file, true);
    }

    //! This class does not currently allow one DatabaseManager
    //! to be simultaneously connected to multiple databases.
    void assertNoDatabaseConnectionOpen_() const
    {
        if (!db_conn_) {
            return;
        }

        if (db_conn_->isValid()) {
            throw DBException(
                "A database connection has already been "
                "made for this DatabaseManager");
        }
    }

    //! Physical database proxy. Commands (INSERT, UPDATE, etc.)
    //! are executed against this proxy, not against the lower-
    //! level database APIs directly.
    std::shared_ptr<SQLiteConnection> db_conn_;

    //! Copy of the schema that was given to the DatabaseManager's
    //! createDatabaseFromSchema() method.
    Schema schema_;

    //! Location where this database lives, e.g. the tempdir
    const std::string db_dir_;

    //! Full database file name, including the database path
    //! and file extension
    std::string db_full_filename_;
};

} // namespace simdb
