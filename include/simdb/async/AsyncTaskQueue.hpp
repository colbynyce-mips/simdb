// <AsyncTaskQueue> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskThread.hpp"
#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb/utils/RunningMean.hpp"

#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
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

    /// \brief Called when this task's turn is up on the worker thread.
    virtual void completeTask() = 0;

    /// \brief Get the size of this task in bytes. This is used
    ///        to track the total size of the queue in the AsyncTaskQueue.
    virtual size_t sizeInBytes() const
    {
        return 0;
    }
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
 * \class ConcurrentQueue<T>
 *
 * \brief Thread-safe wrapper around std::queue
 */
template <typename T>
class ConcurrentQueue
{
public:
    /// Push an item to the back of the queue.
    void push(const T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace(std::move(item));
    }

    /// \brief Construct an item on the back of the queue.
    ///
    /// \param args Forwarding arguments for the <T> constructor.
    template <typename... Args>
    void emplace(Args&&... args)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace(std::forward<Args>(args)...);
    }

    /// \brief Get the item at the front of the queue.
    ///
    /// \param item Output argument for the popped item.
    ///
    /// \return Returns true if successful, or false if there
    ///         was no data in the queue.
    bool try_pop(T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (queue_.empty()) {
            return false;
        }
        std::swap(item, queue_.front());
        queue_.pop();
        return true;
    }

    /// Get the number of items in this queue.
    size_t size() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.size();
    }

    /// Check for empty
    bool empty() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.empty();
    }

private:
    /// Mutex for thread safety.
    mutable std::mutex mutex_;

    /// FIFO queue.
    std::queue<T> queue_;
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

    /// Add a task you wish to evaluate on the worker thread. Returns
    /// the number of tasks currently in the queue.
    size_t addTask(std::unique_ptr<WorkerTask> task)
    {
        if (new_task_destination_) {
            new_task_destination_(std::move(task));
            return concurrent_queue_.size();
        }

        queue_size_bytes_ += task->sizeInBytes();
        concurrent_queue_.emplace(task.release());

        if (!timed_eval_.isRunning()) {
            if (threadRunning()) {
                throw DBException("Must call DatabaseManager::closeDatabase() " \
                                  "before opening another connection!");
            }
            timed_eval_.start();
            threadRunning() = true;
        }

        if (auto_flush_mb_ > 0 && queue_size_bytes_ >= auto_flush_mb_ * 1024 * 1024) {
            std::cout << "Auto-flushing " << queue_size_bytes_ / 1024 / 1024 << " MB to the database" << std::endl;
            flushQueue();
        }

        return concurrent_queue_.size();
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
            bool wrote_to_db = false;
            while (concurrent_queue_.try_pop(task)) {
                try {
                    auto begin = std::chrono::high_resolution_clock::now();
                    task->completeTask();
                    wrote_to_db = true;
                    auto end = std::chrono::high_resolution_clock::now();

                    // Get number of seconds and add it to the running mean
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
                    auto seconds = static_cast<double>(duration.count()) / 1e6;
                    task_processing_time_.add(seconds);
                } catch (const InterruptException&) {
                    break;
                }
            }
            queue_size_bytes_ = 0;
            return wrote_to_db;
        });
    }

    /// \brief Tell this task queue to automatically flush the queue
    ///        to the database every time the queue reaches the given
    ///        size in MB.
    bool enableAutoFlush(const size_t flush_MB)
    {
        if (auto_flush_mb_ != flush_MB) {
            auto_flush_mb_ = flush_MB;
            return true;
        }

        return false;
    }

    /// \brief   Reroute all future tasks from calls to addTask() to the provided
    ///          object's addTask() method. The signature of that method must be
    ///          "void addTask(std::unique_ptr<simdb::WorkerTask> task)".
    template <typename T>
    void rerouteNewTasksTo(T& destination)
    {
        if (new_task_destination_) {
            throw DBException("Cannot call rerouteNewTasksTo() since we are already rerouting tasks! ")
                << "You must call rerouteNewTasksTo(nullptr) first.";
        }
        new_task_destination_ = std::bind(&T::addTask, &destination, std::placeholders::_1);
    }

    /// \brief   Overload for rerouteNewTasksTo() so users can stop rerouting tasks
    ///          and go back to the usual execution.
    void rerouteNewTasksTo(std::nullptr_t)
    {
        new_task_destination_ = nullptr;
    }

    /// Get an estimate of the remaining processing time for all tasks (seconds).
    double getEstimatedRemainingProcTime() const
    {
        return task_processing_time_.mean() * concurrent_queue_.size();
    }

    /// \brief   Wait for the worker queue to be flushed / consumed,
    ///          and stop the consumer thread.
    ///
    /// \warning DO NOT call this method from any WorkerTask
    ///          subclass' completeTask() method or the thread join
    ///          will hang.
    void stopThread()
    {
        if (!timed_eval_.isRunning()) {
            return;
        }

        // Put a special interrupt packet in the queue. This
        // does nothing but throw an interrupt exception when
        // its turn is up.
        std::unique_ptr<WorkerTask> interrupt(new WorkerInterrupt);
        addTask(std::move(interrupt));
        flushQueue();

        // Join the thread and wait until the exception is thrown.
        timed_eval_.stop();
        threadRunning() = false;
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
    /// \brief AsyncTaskThread subclass which invokes the AsyncTaskQueue::flushQueue()
    ///        at regular intervals on the worker thread.
    class TimedEval : public AsyncTaskThread
    {
    public:
        /// \brief Construction.
        ///
        /// \param interval_seconds Fixed-space interval between consecutive atomic commits.
        ///
        /// \param task_queue AsyncTaskQueue that will get its flushQueue() method invoked periodically.
        TimedEval(const double interval_seconds, AsyncTaskQueue* task_queue)
            : AsyncTaskThread(interval_seconds)
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

    // Keep track of how many AsyncTaskQueues (DatabaseManagers) are
    // live and restrict it to at most one worker thread.
    static bool& threadRunning() {
        static bool running = false;
        return running;
    }

    /// Database connection used for safeTransaction().
    SQLiteTransaction* db_conn_ = nullptr;

    /// Thread-safe queue of tasks to be executed on the worker thread.
    ConcurrentQueue<std::unique_ptr<WorkerTask>> concurrent_queue_;

    /// Background thread for fixed-rate periodic queue flushes.
    TimedEval timed_eval_;

    /// Destination for rerouted tasks.
    std::function<void(std::unique_ptr<WorkerTask>)> new_task_destination_;

    /// Size in MB at which to automatically flush the queue to the database.
    size_t auto_flush_mb_ = 0;

    /// Approximate size of the queue in bytes.
    size_t queue_size_bytes_ = 0;

    /// Keep track of the running mean time for the completeTask() methods.
    RunningAverage task_processing_time_;

    /// Creating one of these directly is only for SQLiteConnection.
    /// Access via SQLiteConnection::getTaskQueue().
    friend class SQLiteConnection;
};

/*!
 * \class AllOrNothing
 * \brief This is intended to be used in an RAII fashion where you need ALL
 *        tasks that are added to the AsyncTaskQueue while the AllOrNothing 
 *        is in scope to be guaranteed to get inside the same safeTransaction(),
 *        or NONE of them do.
 * 
 * This protects against the following scenario in asynchronous SimDB:
 * 
 *    void sendDataToDatabase()
 *    {
 *        std::unique_ptr<simdb::WorkerTask> task1(...);
 *        task_queue_->addTask(std::move(task1));
 * 
 *        // Imagine the safeTransaction() now runs on the worker thread
 *        // and successfully completes.
 * 
 *        std::unique_ptr<simdb::WorkerTask> task2(...);
 *        task_queue_->addTask(std::move(task2));
 * 
 *        // Now imagine that the second task is pending in the queue,
 *        // but safeTransaction() hasn't run again just yet. Now we hit
 *        // a segfault.
 *        int *ptr = new int[5];
 *        std::vector<int> data = {1,2,3,4,5};
 *        memcpy(ptr, data.data(), sizeof(int) * 50);
 * 
 *        // Buffer overrun!! Say the above code crashes the program. SQLite
 *        // can make gaurantees about everything inside a safeTransaction()
 *        // getting into the database or not at all. But from a user's point
 *        // of view, there can be use cases where task1 getting into the DB
 *        // but NOT task2 is effectively an invalid database.
 *    }
 * 
 *    // Rewritten to make task1 and task2 BOTH get into the DB or NONE of them do.
 *    void sendDataToDatabase()
 *    {
 *        // Use RAII.
 *        simdb::AllOrNothing all_or_nothing(task_queue_);
 * 
 *        std::unique_ptr<simdb::WorkerTask> task1(...);
 *        task_queue_->addTask(std::move(task1));
 * 
 *        std::unique_ptr<simdb::WorkerTask> task2(...);
 *        task_queue_->addTask(std::move(task2));
 * 
 *        // ... whatever else ...
 *    }
 */
class AllOrNothing
{
public:
    AllOrNothing(simdb::AsyncTaskQueue* task_queue)
        : task_queue_(task_queue)
    {
        task_queue_->rerouteNewTasksTo(*this);
    }

    /// Commit every pending WorkerTask to the physical database, or commit
    /// none of them at all. They will all be evaluated in the same call to
    /// safeTransaction().
    ~AllOrNothing()
    {
        if (!pending_tasks_.empty()) {
            std::unique_ptr<simdb::WorkerTask> commit_task(new Committer(std::move(pending_tasks_)));
            task_queue_->rerouteNewTasksTo(nullptr);
            task_queue_->addTask(std::move(commit_task));
        }
    }

private:
    /// Called by AsyncTaskQueue to do the rerouting. This is private to ensure
    /// that nobody else calls this but the friend class AsyncTaskQueue, or else
    /// tasks could get added to both and the required FIFO nature of the WorkerTask
    /// completeTask() calls could become out of order.
    void addTask(std::unique_ptr<simdb::WorkerTask> task)
    {
        pending_tasks_.emplace_back(task.release());
    }

    class Committer : public simdb::WorkerTask
    {
    public:
        Committer(std::vector<std::unique_ptr<simdb::WorkerTask>>&& tasks)
            : tasks_(std::move(tasks))
        {
        }

        void completeTask() override
        {
            for (auto &task : tasks_) {
                task->completeTask();
            }
        }

    private:
        std::vector<std::unique_ptr<simdb::WorkerTask>> tasks_;
    };

    simdb::AsyncTaskQueue* task_queue_;
    std::vector<std::unique_ptr<simdb::WorkerTask>> pending_tasks_;
    friend class AsyncTaskQueue;
};

} // namespace simdb
