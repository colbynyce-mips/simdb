// <AsyncTaskQueue> -*- C++ -*-

#pragma once

#include "simdb/async/ConcurrentQueue.hpp"
#include "simdb/async/TimerThread.hpp"
#include "simdb/sqlite/SQLiteTransaction.hpp"

#include <iostream>
#include <memory>
#include <vector>

namespace simdb
{

class DatabaseManager;

/*!
 * \class WorkerTask
 *
 * \brief Base class used for all tasks that are given
 *        to the AsyncTaskQueue.
 */
class WorkerTask
{
public:
    /// Destructor
    virtual ~WorkerTask() = default;

    /// Called when this task's turn is up on the worker thread.
    virtual void completeTask() = 0;
};

/*!
 * \class WorkerInterrupt
 *
 * \brief Specialized worker task used in order to break
 *        out of the consumer thread.
 */
class WorkerInterrupt : public WorkerTask
{
protected:
    /// Throw a special exception that informs the worker
    /// thread's infinite consumer loop to finish.
    void completeTask() override
    {
        throw InterruptException();
    }
};

/*!
 * \class AsyncTaskQueue
 *
 * \brief This class is used for processing tasks on a background thread.
 */
class AsyncTaskQueue
{
public:
    /// Stop the timer thread on destruction if it is still running.
    ~AsyncTaskQueue()
    {
        timed_eval_.stop();
    }

    /// Get the database connection associated with this task queue.
    SQLiteTransaction* getConnection() const
    {
        return db_conn_;
    }

    /// Add a task you wish to evaluate on the worker thread.
    void addTask(std::unique_ptr<WorkerTask> task)
    {
        concurrent_queue_.emplace(task.release());

        if (!timed_eval_.isRunning()) {
            timed_eval_.start();
        }
    }

    /// \brief Evaluate every task that has been queued up.
    ///
    /// This is typically called by the worker thread, but may be called
    /// from the main thread at synchronization points like simulation
    /// pause/stop or prior to reading data from the database.
    void flushQueue()
    {
        safeTransaction([&]() {
            std::unique_ptr<WorkerTask> task;
            while (concurrent_queue_.try_pop(task)) {
                try {
                    task->completeTask();
                } catch (const InterruptException&) {
                    break;
                }
            }
        });
    }

    /// \brief   Wait for the worker queue to be flushed / consumed,
    ///          and stop the consumer thread.
    ///
    /// \warning DO NOT call this method from any WorkerTask
    ///          subclass' completeTask() method or the thread join
    ///          will hang.
    void stopThread()
    {
        // Put a special interrupt packet in the queue. This
        // does nothing but throw an interrupt exception when
        // its turn is up.
        std::unique_ptr<WorkerTask> interrupt(new WorkerInterrupt);
        addTask(std::move(interrupt));

        // Join the thread and wait until the exception is thrown.
        timed_eval_.stop();
    }

    /// \brief Safely execute the given functor in an atomic transaction.
    ///
    /// A "safe" transaction is atomic in the sense that it is guaranteed
    /// to execute even if the SQLite database or one of its tables is locked.
    /// It will simply be retried over and over until successful. But note that
    /// exceptions that occur for other reasons (not due to the database being
    /// locked) will still be thrown.
    void safeTransaction(const TransactionFunc& func) const
    {
        db_conn_->safeTransaction(func);
    }

private:
    /// \brief Private constructor. Access via SQLiteConnection::getTaskQueue().
    ///
    /// \param db_conn Database connection. Used for safeTransaction().
    ///
    /// \param interval_seconds Fixed-space interval between consecutive atomic commits.
    AsyncTaskQueue(SQLiteTransaction* db_conn, const double interval_seconds = 0.1)
        : db_conn_(db_conn)
        , timed_eval_(interval_seconds, this)
    {
    }

    /// \class TimedEval
    ///
    /// \brief TimerThread subclass which invokes the AsyncTaskQueue::flushQueue()
    ///        at regular intervals on the worker thread.
    class TimedEval : public TimerThread
    {
    public:
        /// \brief Construction.
        ///
        /// \param interval_seconds Fixed-space interval between consecutive atomic commits.
        ///
        /// \param task_queue AsyncTaskQueue that will get its flushQueue() method invoked periodically.
        TimedEval(const double interval_seconds, AsyncTaskQueue* task_queue)
            : TimerThread(interval_seconds)
            , task_queue_(task_queue)
        {
        }

    private:
        /// Called at regular intervals on the worker thread.
        void execute_() override
        {
            task_queue_->flushQueue();
        }

        AsyncTaskQueue* const task_queue_;
    };

    /// Database connection used for safeTransaction().
    SQLiteTransaction* db_conn_ = nullptr;

    /// Thread-safe queue of tasks to be executed on the worker thread.
    ConcurrentQueue<std::unique_ptr<WorkerTask>> concurrent_queue_;

    /// Background thread for fixed-rate periodic queue flushes.
    TimedEval timed_eval_;

    /// Creating one of these directly is only for SQLiteConnection.
    /// Access via SQLiteConnection::getTaskQueue().
    friend class SQLiteConnection;
};

} // namespace simdb
