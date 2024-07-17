// <AsyncTaskQueue> -*- C++ -*-

#pragma once

#include "simdb/async/TimerThread.hpp"
#include "simdb/async/ConcurrentQueue.hpp"
#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb_fwd.hpp"

#include <iostream>
#include <memory>
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
    AsyncTaskQueue(SQLiteTransaction *db_conn, DatabaseManager *db_mgr, double interval_seconds = 0.1)
        : db_conn_(db_conn)
        , db_mgr_(db_mgr)
        , timed_eval_(interval_seconds, this)
    {}

    ~AsyncTaskQueue() {
        timed_eval_.stop();
    }

    //! Get the database connection associated with this task queue.
    SQLiteTransaction *getConnection() const
    {
        return db_conn_;
    }

    //! Get the database manager associated with this task queue.
    DatabaseManager *getDatabaseManager() const
    {
        return db_mgr_;
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

    //! Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc & func) const
    {
        db_conn_->safeTransaction(func);
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

    SQLiteTransaction * db_conn_ = nullptr;
    DatabaseManager * db_mgr_ = nullptr;
    ConcurrentQueue<std::unique_ptr<WorkerTask>> concurrent_queue_;
    TimedEval timed_eval_;
};

} // namespace simdb
