#pragma once

#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb/sqlite/Constraints.hpp"
#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/utils/Stringifiers.hpp"
#include "simdb/utils/MathUtils.hpp"
#include "simdb_fwd.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <sqlite3.h>

namespace simdb {

//! Callback which gets invoked during SELECT queries that involve
//! floating point comparisons with a supplied tolerance.
void fuzzyMatch(sqlite3_context * context,
                int, sqlite3_value ** argv)
{
    const double column_value = sqlite3_value_double(argv[0]);
    const double target_value = sqlite3_value_double(argv[1]);
    const int constraint = sqlite3_value_int(argv[2]);
    static constexpr double tolerance = std::numeric_limits<double>::epsilon();

    if (constraint >= static_cast<int>(SetConstraints::IN_SET)) {
        throw DBException("Invalid constraint in fuzzyMatch(). Should be Constraints enum.");
    }

    const Constraints e_constraint = static_cast<Constraints>(constraint);

    auto set_is_match = [context](const bool match) {
        sqlite3_result_int(context, match ? 1 : 0);
    };

    auto check_equal = [=](const bool should_be_equal) {
        const bool approx_equal = utils::approximatelyEqual(column_value, target_value, tolerance);
        if (approx_equal == should_be_equal) {
            set_is_match(true);
        } else {
            set_is_match(false);
        }
    };

    switch (e_constraint) {
        case Constraints::EQUAL: {
            check_equal(true);
            break;
        }
        case Constraints::NOT_EQUAL: {
            check_equal(false);
            break;
        }
        case Constraints::LESS: {
            set_is_match(column_value < target_value);
            break;
        }
        case Constraints::LESS_EQUAL: {
            if (column_value < target_value) {
                set_is_match(true);
                break;
            } else {
                check_equal(true);
            }
            break;
        }
        case Constraints::GREATER: {
            set_is_match(column_value > target_value);
            break;
        }
        case Constraints::GREATER_EQUAL: {
            if (column_value > target_value) {
                set_is_match(true);
            } else {
                check_equal(true);
            }
            break;
        }
    }
}

/*!
 * \brief This class handles the SQLite database operations.
 */
class SQLiteConnection : public SQLiteTransaction
{
public:
    SQLiteConnection(DatabaseManager * db_mgr)
        : task_queue_(new AsyncTaskQueue(this, db_mgr))
        , db_mgr_(db_mgr)
    {
    }

    ~SQLiteConnection()
    {
        if (db_conn_) {
            sqlite3_close(db_conn_);
        }
    }

    //! Turn a Schema object into an actual database connection.
    void realizeSchema(
        const Schema & schema,
        const DatabaseManager & obj_mgr)
    {
        for (const auto & table : schema) {
            //First create the table and its columns
            std::ostringstream oss;
            oss << "CREATE TABLE " << table.getName() << "(";

            //All tables have an auto-incrementing primary key
            oss << "Id INTEGER PRIMARY KEY AUTOINCREMENT";
            if (!table.hasColumns()) {
                //A table without any columns would be somewhat
                //odd, but that's what the user's schema specified.
                //It is not invalid SQL, so we do not throw.
                oss << ");";
            } else {
                //Fill in the rest of the CREATE TABLE command:
                //CREATE TABLE Id INTEGER PRIMARY KEY AUTOINCREMENT First TEXT, ...
                //                                                  ---------------
                oss << ", " << getColumnsSqlCommand_(table) << ");";
            }

            //Create the table in the database
            eval(oss.str());

            //Now create any table indexes, for example:
            //    CREATE INDEX customer_fullname ON Customers (First,Last)
            //    CREATE INDEX county_population ON Counties (CountyName,Population)
            //    ...
            makeIndexesForTable_(table);
        }
    }

    //! Override the default way DatabaseManager gets the table
    //! names. It defaults to whatever Table objects were in the
    //! Schema object it was originally given, but we may want to
    //! override it so we can make some tables private/unqueryable
    //! for internal use only.
    void getTableNames(
        std::unordered_set<std::string> & table_names)
    {
        //Helper object that will get called once for each
        //matching record in the SELECT statement.
        struct TableNames {
            TableNames(std::unordered_set<std::string> & names) :
                tbl_names(names)
            {}

            int addTableName(int argc, char ** argv, char **) {
                //We got another table name. Add it to the set.
                //*BUT* skip over any tables that are prefixed
                //with "sqlite_". Those are all reserved for the
                //library, and aren't really ours.
                assert(argc == 1);
                if (std::string(argv[0]).find("sqlite_") != 0) {
                    tbl_names.insert(argv[0]);
                }
               return 0;
            }
            std::unordered_set<std::string> & tbl_names;
        };

        TableNames select_callback_obj(table_names);

        //The TableNames object above has a reference to the table_names
        //output argument. Running the following SELECT statement will
        //populate this variable.
        evalSelect("SELECT name FROM sqlite_master WHERE type='table'",
                   +[](void * callback_obj, int argc, char ** argv, char ** col_names) {
                       return static_cast<TableNames*>(callback_obj)->
                           addTableName(argc, argv, col_names);
                   },
                   &select_callback_obj);
    }

    //! Try to open a connection to an existing database file.
    bool connectToExistingDatabase(const std::string & db_file)
    {
        return (!openDbFile_(".", db_file, false).empty());
    }

    //! Get the full database filename being used. This includes
    //! the database path, stem, and extension. Returns empty if
    //! no connection is open.
    std::string getDatabaseFullFilename() const
    {
        return db_full_filename_;
    }

    //! Is this connection still alive and well?
    bool isValid() const
    {
        return (db_conn_ != nullptr);
    }

    //! Execute the provided statement against the database
    //! connection. This will validate the command, and throw
    //! if this command is disallowed.
    void eval(const std::string & command) const
    {
        eval_(command, db_conn_, nullptr, nullptr);
    }

    //! Execute a SELECT statement against the database
    //! connection. The provided callback will be invoked
    //! once for each record found. Example:
    //!
    //!    struct CallbackHandler {
    //!        int handle(int argc, char ** argv, char ** col_names) {
    //!            ...
    //!            return 0;
    //!        }
    //!    };
    //!
    //!    CallbackHandler handler;
    //!
    //!    db_proxy->evalSelect(
    //!        "SELECT First,Last FROM Customers",
    //!        +[](void * handler, int argc, char ** argv, char ** col_names) {
    //!            return (CallbackHandler*)(handler)->handle(argc, argv, col_names);
    //!        },
    //!        &handler);
    //!
    //! See TransactionUtils.h for more details about the callback
    //! arguments that are passed to your SELECT handler.
    void evalSelect(
        const std::string & command,
        int (*callback)(void *, int, char **, char **),
        void * callback_obj) const
    {
        eval_(command, db_conn_, callback, callback_obj);
    }

    //! Turn the given command into an SQL prepared statement.
    sqlite3_stmt * prepareStatement(const std::string & command)
    {
        sqlite3_stmt * stmt = nullptr;
        if (sqlite3_prepare_v2(db_conn_, command.c_str(), -1, &stmt, 0)) {
            throw DBException("Malformed SQL command: ") << command;
        }
        return stmt;
    }

    //! Get the database ID of the last INSERT statement.
    int getLastInsertRowId() const
    {
        return sqlite3_last_insert_rowid(db_conn_);
    }

    //! Get direct access to the underlying SQLite database.
    sqlite3 * getDatabase() const
    {
        return db_conn_;
    }

    //! Get this database connection's task queue. This
    //! object can be used to schedule database work to
    //! be executed on a background thread. This never
    //! returns null.
    AsyncTaskQueue *getTaskQueue() const
    {
        return task_queue_.get();
    }

    //! Get the database manager this connection is
    //! associated with.
    DatabaseManager *getDatabaseManager() const
    {
        return db_mgr_;
    }

private:
    //! This proxy is intended to be used directly with
    //! the core SimDB classes.
    friend class DatabaseManager;

    //! Issue BEGIN TRANSACTION
    void beginTransaction() const override
    {
        eval("BEGIN TRANSACTION");
    }

    //! Issue COMMIT TRANSACTION
    void endTransaction() const override
    {
        eval("COMMIT TRANSACTION");
    }

    //! First-time database file open.
    std::string openDbFile_(
        const std::string & db_dir,
        const std::string & db_file,
        const bool create_file)
    {
        db_full_filename_ = resolveDbFilename_(db_dir, db_file);
        if (db_full_filename_.empty()) {
            if (create_file) {
                db_full_filename_ = db_dir + "/" + db_file;
            } else {
                throw DBException("Could not find database file: '")
                    << db_dir << "/" << db_file;
            }
        }

        const int db_open_flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
        sqlite3 * sqlite_conn = nullptr;
        auto err_code = sqlite3_open_v2(db_full_filename_.c_str(),
                                        &sqlite_conn, db_open_flags, 0);

        //Inability to even open the database file may mean that
        //we don't have write permissions in this directory or
        //something like that. We should throw until we understand
        //better how else we can get bad file opens.
        if (err_code != SQLITE_OK) {
            throw DBException(
                "Unable to connect to the database file: ") << db_file;
        }

        //SQLite isn't the only implementation that SimDB supports.
        //The sqlite3_open_v2() function can still return a non-null
        //handle for a file that is NOT even SQLite. Let's try to make
        //a simple database query to verify the file is actually SQLite.
        if (!validateConnectionIsSQLite_(sqlite_conn)) {
            sqlite3_close(sqlite_conn);
            sqlite_conn = nullptr;
        }

        db_conn_ = sqlite_conn;
        if (db_conn_) {
            sqlite3_create_function(db_conn_, "fuzzyMatch", 3,
                                    SQLITE_UTF8, nullptr,
                                    &fuzzyMatch,
                                    nullptr, nullptr);
        }
        return (db_conn_ != nullptr ? db_full_filename_ : "");
    }

    //! Create a prepared statement for the provided command.
    //! The specific pointer type of the output void** is tied
    //! to the database tech being used. This is intended to
    //! be implementation detail, so this method is accessible
    //! to friends only.
    void prepareStatement_(
        const std::string & command,
        void ** statement) const
    {
        if (!db_conn_ || !statement) {
            return;
        }

        sqlite3_stmt * stmt = nullptr;
        if (sqlite3_prepare_v2(db_conn_, command.c_str(), -1, &stmt, 0)) {
            throw DBException("Malformed SQL command: '") << command << "'";
        }

        if (stmt) {
            *statement = stmt;
        }
    }

    //! All SQL commands (both reads and writes) end up here. The only
    //! difference between a read and a write is if the two callback
    //! inputs are null or not.
    void eval_(const std::string & command,
               sqlite3 * db_conn,
               int (*callback)(void *, int, char **, char **),
               void * callback_obj) const
    {
        char * err = 0;
        const int res = sqlite3_exec(db_conn,
                                     command.c_str(),
                                     callback,
                                     callback_obj,
                                     &err);
        if (res != SQLITE_OK) {
            switch (res) {
                case SQLITE_BUSY:
                    throw SqlFileLockedException();
                case SQLITE_LOCKED:
                    throw SqlTableLockedException();
                default:
                    break;
            }

            std::string err_str;
            if (err) {
                //If our char* has an error message in it,
                //we will add it to the exception.
                err_str = err;
                sqlite3_free(err);
            } else {
                //Otherwise, just add the SQLite error code.
                //Users can look up the meaning of the code
                //in sqlite3.h
                std::ostringstream oss;
                oss << res << " (see sqlite3.h for error code definitions)";
                err_str = oss.str();
            }
            err_str += " (failed SQL command was '" + command + "')";
            throw DBException(err_str);
        }
    }

    //! Loop over a Table's columns one by one, and create
    //! a SQL statement that can be used with CREATE TABLE.
    //! Column names, data types, and value defaults are
    //! used here. Example SQL might look like this:
    //! 
    //!   First TEXT, Last TEXT, Age INT, Balance FLOAT DEFAULT 50.00
    //! 
    std::string getColumnsSqlCommand_(const Table & table) const
    {
        std::ostringstream oss;
        for (const auto & column : table) {
            oss << column->getName() << " " << column->getDataType();
            if (column->hasDefaultValue()) {
                oss << " DEFAULT " << column->getDefaultValueAsString();
            }
            oss << ", ";
        }
        std::string command = oss.str();

        //Trim the trailing ", "
        if (command.back() == ' ') { command.pop_back(); }
        if (command.back() == ',') { command.pop_back(); }

        return command;
    }

    //! Create indexes for a given Table, depending on how the
    //! user set up the Column indexes (indexed by itself, vs.
    //! indexed together with other columns)
    void makeIndexesForTable_(const Table & table) const
    {
        if (!table.hasColumns()) {
            return;
        }

        for (const auto & column : table) {
            if (column->isIndexed()) {
                makeIndexesForColumnInTable_(table, *column);
            }
        }
    }

    //! Execute index creation statements like:
    //! 
    //!     "CREATE INDEX Customers_Last ON Customers(Last)"
    //!         ^^ indexes Customers table by Last column only
    //! 
    //!     "CREATE INDEX Customers_Last ON Customers(First,Last)"
    //!         ^^ multi-column index on the Customers table by First+Last columns
    //! 
    void makeIndexesForColumnInTable_(const Table & table,
                                      const Column & column) const
    {
        std::ostringstream oss;
        oss << " CREATE INDEX " << table.getName() << "_" << column.getName()
            << " ON " << table.getName()
            << " (" << makePropertyIndexesStr_(column) << ")";
        eval(oss.str());
    }

    //! For the CREATE INDEX statements, this helper makes a comma-
    //! separated string of Column names like "First,Last"
    std::string makePropertyIndexesStr_(const Column & column) const
    {
        std::ostringstream oss;
        for (const auto & indexed_property : column.getIndexedProperties()) {
            oss << indexed_property->getName() << ",";
        }
        std::string indexes_str = oss.str();
        if (indexes_str.back() == ',') {
            indexes_str.pop_back();
        }
        return indexes_str;
    }

    //See if there is an existing file by the name <dir/file>
    //and return it. If not, return just <file> if it exists.
    //Return "" if no such file could be found.
    std::string resolveDbFilename_(
        const std::string & db_dir,
        const std::string & db_file) const
    {
        std::ifstream fin(db_dir + "/" + db_file);
        if (fin) {
            return db_dir + "/" + db_file;
        }

        fin.open(db_file);
        if (fin) {
            return db_file;
        }
        return "";
    }

    //! Attempt to run an SQL command against our open connection.
    //! A file may have been given to us that was a different format,
    //! such as HDF5.
    bool validateConnectionIsSQLite_(sqlite3 * db_conn) const
    {
        struct FindHelper {
            bool did_find_any = false;
        };

        FindHelper finder;
        const std::string command =
            "SELECT name FROM sqlite_master WHERE type='table'";

        try {
            eval_(command, db_conn,
                  +[](void * callback_obj, int, char **, char **)
                  {
                    static_cast<FindHelper*>(callback_obj)->did_find_any = true;
                    return SQLITE_OK;
                  },
                  &finder);
            return true;
        } catch (...) {
        }
        return false;
    }

    //! Physical database connection
    sqlite3 * db_conn_ = nullptr;

    //! Filename of the database in use
    std::string db_full_filename_;

    //! Task queue associated with this database connection.
    //! It is instantiated from our constructor, but won't
    //! have any effect unless its addWorkerTask() method
    //! is called. That method starts a background thread
    //! to begin consuming work packets.
    std::shared_ptr<AsyncTaskQueue> task_queue_;

    //! DatabaseManager associated with this database connection.
    DatabaseManager *db_mgr_ = nullptr;
};

} // namespace simdb
