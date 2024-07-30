// <SQLiteTransaction> -*- C++ -*-

#pragma once

#include "simdb/Errors.hpp"

#include <sqlite3.h>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>

namespace simdb
{

class AsyncTaskQueue;

typedef std::function<void()> TransactionFunc;

class SQLiteReturnCode
{
public:
    explicit SQLiteReturnCode(const int rc)
        : rc_(rc)
    {
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            throw SafeTransactionSilentException();
        }
    }

    operator int() const
    {
        return rc_;
    }

    operator bool() const
    {
        return rc_ != SQLITE_OK;
    }

    bool operator==(const int rc)
    {
        return rc_ == rc;
    }

    bool operator!=(const int rc)
    {
        return rc_ != rc;
    }

private:
    const int rc_;
};

inline std::ostream& operator<<(std::ostream& os, const SQLiteReturnCode& rc)
{
    os << (int)rc;
    return os;
}

class SQLiteTransaction
{
public:
    virtual ~SQLiteTransaction() = default;

    virtual void beginTransaction() = 0;

    virtual void endTransaction() = 0;

    /// Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc& transaction)
    {
        while (true) {
            try {
                std::lock_guard<std::recursive_mutex> lock(mutex_);

                // Check to see if we are already in a transaction, in which
                // case we simply call the transaction function. We cannot
                // call "BEGIN TRANSACTION" recursively.
                if (in_transaction_flag_) {
                    transaction();
                } else {
                    ScopedTransaction scoped_transaction(this, transaction, in_transaction_flag_);
                    (void)scoped_transaction;
                }

                // We got this far without an exception, which means
                // that the transaction is committed.
                break;
            } catch (const SafeTransactionSilentException&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        }
    }

    /// Get this database connection's task queue. This
    /// object can be used to schedule database work to
    /// be executed on a background thread.
    virtual AsyncTaskQueue* getTaskQueue() const = 0;

private:
    /// \brief Flag used in RAII safeTransaction() calls. This is
    ///        needed to we know whether to tell SQL to "BEGIN
    ///        TRANSACTION" or not (i.e. if we're already in the
    ///        middle of another safeTransaction).
    ///
    /// This allows users to freely do something like this:
    ///
    /// \code
    ///     db_mgr_->safeTransaction([&]() {
    ///         doFoo();
    ///         doBar();
    ///     });
    /// \endcode
    ///
    /// Even if doFoo() and doBar() do the same thing:
    ///
    /// \code
    ///     void MyClass::doFoo() {
    ///         db_mgr_->safeTransaction([&](){
    ///             ...
    ///         });
    ///     }
    ///
    ///     void MyClass::doBar() {
    ///         db_mgr_->safeTransaction([&](){
    ///             ...
    ///         });
    ///     }
    /// \endcode
    bool in_transaction_flag_ = false;

    /// Mutex for thread-safe reentrant safeTransaction's.
    std::recursive_mutex mutex_;

    /// RAII used for BEGIN/COMMIT TRANSACTION calls. Ensures that
    /// these calls always occur in pairs.
    struct ScopedTransaction {
        /// Issues BEGIN TRANSACTION
        ScopedTransaction(SQLiteTransaction* db_conn, const TransactionFunc& transaction, bool& in_transaction_flag)
            : db_conn_(db_conn)
            , transaction_(transaction)
            , in_transaction_flag_(in_transaction_flag)
        {
            in_transaction_flag_ = true;
            db_conn_->beginTransaction();
            transaction_();
        }

        /// Issues COMMIT TRANSACTION
        ~ScopedTransaction()
        {
            db_conn_->endTransaction();
            in_transaction_flag_ = false;
        }

    private:
        /// Open database connection
        SQLiteTransaction* db_conn_ = nullptr;

        /// The caller's function they want inside BEGIN/COMMIT TRANSACTION
        const TransactionFunc& transaction_;

        /// Reference to SQLiteTransaction::in_transaction_flag_
        bool& in_transaction_flag_;
    };
};

} // namespace simdb
