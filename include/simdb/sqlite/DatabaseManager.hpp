// <DatabaseManager> -*- C++ -*-

#pragma once

#include "simdb/schema/Schema.hpp"
#include "simdb/sqlite/Connection.hpp"
#include "simdb/utils/uuids.hpp"
#include "simdb_fwd.hpp"

#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
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
        , task_queue_(nullptr)
    {
    }

    //! Using a Schema object for your database, construct
    //! the physical database file and open the connection.
    //!
    //! Returns true if successful, false otherwise.
    bool createDatabaseFromSchema(Schema & schema)
    {
        schema.finalizeSchema_();
        db_conn_.reset(new SQLiteConnection);
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
        db_conn_.reset(new SQLiteConnection);

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
    const SQLiteConnection * getConnection() const
    {
        return db_conn_.get();
    }

    //! Get this database connection's task queue. This
    //! object can be used to schedule database work to
    //! be executed on a background thread. This never
    //! returns null.
    AsyncTaskQueue * getTaskQueue() const {
        return task_queue_.get();
    }

    //! Open database connections will be closed when the
    //! destructor is called.
    ~DatabaseManager() = default;

    //! All API calls to DatabaseManager, ObjectRef, and the
    //! other database classes will be executed inside "safe
    //! transactions" for exception safety and for better
    //! performance. Failed database writes/reads will be
    //! retried until successful. This will also improve
    //! performance - especially for DB writes - if you
    //! have several operations that you need to perform
    //! on the database, for example:
    //!
    //! \code
    //!     ObjectRef new_customer(...)
    //!     new_customer.setPropertyString("First", "Bob")
    //!     new_customer.setPropertyString("Last", "Smith")
    //!     new_customer.setPropertyInt32("Age", 41)
    //! \endcode
    //!
    //! That would normally be three individual transactions.
    //! But if you do this instead (assuming you have an
    //! DatabaseManager 'obj_mgr' nearby):
    //!
    //! \code
    //!     obj_mgr.safeTransaction([&]() {
    //!         ObjectRef new_customer(...)
    //!         new_customer.setPropertyString("First", "Bob")
    //!         new_customer.setPropertyString("Last", "Smith")
    //!         new_customer.setPropertyInt32("Age", 41)
    //!     });
    //! \endcode
    //!
    //! That actually ends up being just *one* database
    //! transaction. Not only is this faster (in some
    //! scenarios it can be a very significant performance
    //! boost) but all three of these individual setProperty()
    //! calls would either be committed to the database, or
    //! they wouldn't, maybe due to an exception. But the
    //! "new_customer" object would not have the "First"
    //! property written to the database, while the "Last"
    //! and "Age" properties were left unwritten. "Half-
    //! written" database objects could result in difficult
    //! bugs to track down, or leave your data in an
    //! inconsistent state.
    typedef std::function<void()> TransactionFunc;
    void safeTransaction(TransactionFunc transaction) const
    {
        //There are "normal" or "acceptable" SQLite errors that
        //we trap: SQLITE_BUSY (the database file is locked), and
        //SQLITE_LOCKED (a table in the database is locked). These
        //can occur when SQLite is used in concurrent systems, and
        //are not necessarily "real" errors.
        //
        //If these *specific* types of errors occur, we will catch
        //them and keep retrying the transaction until successful.
        //This is part of what is meant by a "safe" transaction.
        //Database transactions will not fail due to concurrent
        //access errors that are not always obvious from a SPARTA
        //user/developer's perspective.

        while (true) {
            try {
                //More thought needs to go into thread safety of the
                //database writes/reads. Let's be super lazy and grab
                //a mutex right here for the time being.
                std::lock_guard<std::recursive_mutex> lock(mutex_);

                //Check to see if we are already in a transaction, in which
                //case we simply call the transaction function. We cannot
                //call "BEGIN TRANSACTION" recursively.
                if (is_in_transaction_) {
                    transaction();
                } else {
                    ScopedTransaction scoped_transaction(db_conn_.get(), transaction, is_in_transaction_);
                    (void)scoped_transaction;
                }

                //We got this far without an exception, which means
                //that the proxy's commitAtomicTransaction() method
                //has been called (if it supports atomic transactions).
                break;

            //Retry transaction due to database access errors
            } catch (const DBAccessException & ex) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                continue;
            }

            //Note that other std::exceptions are still being thrown,
            //and may abort the simulation
        }
    }

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

    //! Task queue associated with this database connection.
    //! It is instantiated from our constructor, but won't
    //! have any effect unless its addWorkerTask() method
    //! is called. That method starts a background thread
    //! to begin consuming work packets.
    std::shared_ptr<AsyncTaskQueue> task_queue_;

    //! Copy of the schema that was given to the DatabaseManager's
    //! createDatabaseFromSchema() method.
    Schema schema_;

    //! Location where this database lives, e.g. the tempdir
    const std::string db_dir_;

    //! Full database file name, including the database path
    //! and file extension
    std::string db_full_filename_;

    //! Flag used in RAII safeTransaction() calls. This is
    //! needed to we know whether to tell SQL to "BEGIN
    //! TRANSACTION" or not (i.e. if we're already in the
    //! middle of another safeTransaction).
    //! 
    //! This allows users to freely do something like this:
    //! 
    //!     obj_mgr_.safeTransaction([&]() {
    //!         writeReportHeader_(report);
    //!     });
    //! 
    //! Even if their writeReportHeader_() code does the
    //! same thing:
    //! 
    //!     void CSV::writeReportHeader_(sparta::Report * r) {
    //!         obj_mgr_.safeTransaction([&]() {
    //!             writeReportName_(r);
    //!             writeSimulationMetadata_(sim_);
    //!         });
    //!     }
    mutable bool is_in_transaction_ = false;

    //! Mutex for thread-safe reentrant safeTransaction's.
    mutable std::recursive_mutex mutex_;

    //! RAII used for BEGIN/COMMIT TRANSACTION calls to make safeTransaction
    //! more performant.
    struct ScopedTransaction {
        ScopedTransaction(const SQLiteConnection * db_conn,
                        DatabaseManager::TransactionFunc & transaction,
                        bool & in_transaction_flag) :
            db_conn_(db_conn),
            transaction_(transaction),
            in_transaction_flag_(in_transaction_flag)
        {
            in_transaction_flag_ = true;
            db_conn_->eval("BEGIN TRANSACTION");
            transaction_();
        }

        ~ScopedTransaction()
        {
            db_conn_->eval("COMMIT TRANSACTION");
            in_transaction_flag_ = false;
        }

    private:
        //! Open database connection
        const SQLiteConnection * db_conn_ = nullptr;

        //! The caller's function they want inside BEGIN/COMMIT TRANSACTION
        DatabaseManager::TransactionFunc & transaction_;

        //! The caller's "in transaction flag" - in case they
        //! need to know whether *their code* is already in
        //! an ongoing transaction:
        //!
        //!   void MyObj::callSomeSQL(DbConnProxy * db_conn) {
        //!       if (!already_in_transaction_) {
        //!           ScopedTransaction(db_conn,
        //!               [&](){ eval_sql(db_conn, "INSERT INTO Customers ..."); },
        //!               already_in_transaction_);
        //!
        //!           //Now call another method which MIGHT
        //!           //call this "callSomeSQL()" method again:
        //!           callFooBarFunction_();
        //!       } else {
        //!           eval_sql(db_conn, "INSERT INTO Customers ...");
        //!       }
        //!   }
        //!
        //! The use of this flag lets functions like MyObj::callSomeSQL()
        //! be safely called recursively. Without it, "BEGIN TRANSACTION"
        //! could get called a second time like this:
        //!
        //!      BEGIN TRANSACTION
        //!      INSERT INTO Customers ...
        //!      BEGIN TRANSACTION            <-- SQLite will error!
        //!                           (was expecting COMMIT TRANSACTION before
        //!                                    seeing this again)
        bool & in_transaction_flag_;
    };
};

} // namespace simdb
