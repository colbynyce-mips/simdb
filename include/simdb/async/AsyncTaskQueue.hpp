// <AsyncTaskQueue> -*- C++ -*-

#pragma once

#include "simdb/async/TimerThread.hpp"
#include "simdb/async/ConcurrentQueue.hpp"

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace simdb {

/*!
 * \brief Base class used for all tasks that are given
 * to the worker task queue. The completeTask() method
 * will be called when this task's turn is up on the
 * worker thread.
 */
class WorkerTask
{
public:
    virtual ~WorkerTask() = default;
    virtual void completeTask() = 0;
};

/*!
 * \brief Specialized worker task used in order to break
 * out of the consumer thread without synchronously asking
 * it to do so.
 */
class WorkerInterrupt : public WorkerTask
{
protected:
    //! WorkerTask implementation
    void completeTask() override {
        throw InterruptException();
    }
};

class AsyncTaskQueue
{
public:
    //! Construct a task evaluator that will execute every
    //! 'interval_seconds' that you specify.
    explicit AsyncTaskQueue(SQLiteConnection *db_conn, double interval_seconds = 0.1)
        : db_conn_(db_conn)
        , timed_eval_(interval_seconds, this)
    {}

    ~AsyncTaskQueue() {
        timed_eval_.stop();
    }

    //! Add a task you wish to evaluate off the main thread
    void addTask(std::unique_ptr<WorkerTask> task) {
        concurrent_queue_.emplace(task.release());

        if (!timed_eval_.isRunning()) {
            timed_eval_.start();
        }
    }

    //! Evaluate every task that has been queued up. This is
    //! typically called by a worker thread, but may be called
    //! from the main thread at synchronization points like
    //! simulation pause/stop.
    void flushQueue() {
        safeTransaction([&]() {
            std::unique_ptr<WorkerTask> task;
            while (concurrent_queue_.try_pop(task)) {
                try {
                    task->completeTask();
                } catch (const InterruptException &) {
                    break;
                }
            }
        });
    }

    //! Wait for the worker queue to be flushed / consumed,
    //! and stop the consumer thread.
    //!
    //! \warning DO NOT call this method from any WorkerTask
    //! subclass' completeTask() method. If the completeTask()
    //! method was being invoked from this controller's own
    //! consumer thread (which is usually the case), this
    //! method will hang. It is safest to call this method
    //! from code that you know is always on the main thread,
    //! for example in setup or teardown / post-processing
    //! code in a simulation.
    void stopThread() {
        //Put a special interrupt packet in the queue. This
        //does nothing but throw an interrupt exception when
        //its turn is up.
        std::unique_ptr<WorkerTask> interrupt(new WorkerInterrupt);
        addTask(std::move(interrupt));

        //Join the thread and just wait forever... (until the
        //exception is thrown).
        timed_eval_.stop();
    }

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
                    ScopedTransaction scoped_transaction(db_conn_, transaction, is_in_transaction_);
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

private:
    //! TimerThread implementation. Called at regular
    //! intervals on a worker thread.
    class TimedEval : public TimerThread
    {
    public:
        TimedEval(const double interval_seconds,
                  AsyncTaskQueue * task_eval) :
            TimerThread(TimerThread::Interval::FIXED_RATE,
                        interval_seconds),
            task_eval_(task_eval)
        {}

    private:
        void execute_() override {
            task_eval_->flushQueue();
        }

        AsyncTaskQueue *const task_eval_;
    };

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
                          AsyncTaskQueue::TransactionFunc & transaction,
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
        AsyncTaskQueue::TransactionFunc & transaction_;

        //! The caller's "in transaction flag" - in case they
        //! need to know whether *their code* is already in
        //! an ongoing transaction. This protects against
        //! recursive calls to BEGIN TRANSACTION, which is
        //! disallowed.
        bool & in_transaction_flag_;
    };

    SQLiteConnection * db_conn_ = nullptr;
    ConcurrentQueue<std::unique_ptr<WorkerTask>> concurrent_queue_;
    TimedEval timed_eval_;
};

} // namespace simdb
