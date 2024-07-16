// <AsyncTaskQueue> -*- C++ -*-

#pragma once

#include "simdb/async/TimerThread.hpp"
#include "simdb/async/ConcurrentQueue.hpp"
#include "simdb/Errors.hpp"

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <functional>

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
    explicit AsyncTaskQueue(const double interval_seconds = 0.1) :
        timed_eval_(interval_seconds, this)
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
        std::unique_ptr<WorkerTask> task;
        while (concurrent_queue_.try_pop(task)) {
            try {
                task->completeTask();
            } catch (const InterruptException &) {
                break;
            }
        }
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

private:
    //! Pop the oldest task from the queue, returning true
    //! if successful, false otherwise (due to empty queue).
    bool popQueue_(std::unique_ptr<WorkerTask> & task) {
        return concurrent_queue_.try_pop(task);
    }

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

    ConcurrentQueue<std::unique_ptr<WorkerTask>> concurrent_queue_;
    TimedEval timed_eval_;
};

} // namespace simdb
