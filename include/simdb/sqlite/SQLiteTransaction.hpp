#pragma once

#include "simdb/Errors.hpp"

#include <chrono>
#include <functional>
#include <mutex>
#include <thread>

namespace simdb
{

typedef std::function<void()> TransactionFunc;

class SQLiteTransaction
{
public:
    virtual ~SQLiteTransaction() = default;

    virtual void beginTransaction() const = 0;

    virtual void endTransaction() const = 0;

    //! Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc& transaction) const
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
                    ScopedTransaction scoped_transaction(this, transaction, is_in_transaction_);
                    (void)scoped_transaction;
                }

                //We got this far without an exception, which means
                //that the proxy's commitAtomicTransaction() method
                //has been called (if it supports atomic transactions).
                break;

                //Retry transaction due to database access errors
            } catch (const DBAccessException& ex) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                continue;
            }

            //Note that other std::exceptions are still being thrown,
            //and may abort the simulation
        }
    }

private:
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
        ScopedTransaction(const SQLiteTransaction* db_conn,
                          const TransactionFunc& transaction,
                          bool& in_transaction_flag)
            : db_conn_(db_conn)
            , transaction_(transaction)
            , in_transaction_flag_(in_transaction_flag)
        {
            in_transaction_flag_ = true;
            db_conn_->beginTransaction();
            transaction_();
        }

        ~ScopedTransaction()
        {
            db_conn_->endTransaction();
            in_transaction_flag_ = false;
        }

    private:
        //! Open database connection
        const SQLiteTransaction* db_conn_ = nullptr;

        //! The caller's function they want inside BEGIN/COMMIT TRANSACTION
        const TransactionFunc& transaction_;

        //! The caller's "in transaction flag" - in case they
        //! need to know whether *their code* is already in
        //! an ongoing transaction. This protects against
        //! recursive calls to BEGIN TRANSACTION, which is
        //! disallowed.
        bool& in_transaction_flag_;
    };
};

} // namespace simdb
