// <AsyncTaskThread> -*- C++ -*-

#pragma once

#include "simdb3/Exceptions.hpp"
#include <chrono>
#include <thread>

namespace simdb3
{

/*!
 * \class AsyncTaskThread
 *
 * \brief Thread utility used for periodic execution
 *        of asynchronous tasks.
 */
class AsyncTaskThread
{
public:
    /// \brief Construction.
    ///
    /// \param interval_seconds Fixed wall clock interval in seconds.
    AsyncTaskThread(const double interval_seconds)
        : interval_seconds_(interval_seconds)
    {
    }

    /// Destructor. When the timer goes out of scope,
    /// the execute_() callbacks will be stopped.
    virtual ~AsyncTaskThread()
    {
        stop();
    }

    /// Call this method from the main thread to start
    /// timed execution of your execute_() method.
    void start()
    {
        if (thread_ == nullptr) {
            is_running_ = true;
            thread_.reset(new std::thread([&]() { start_(); }));
        }
    }

    /// \brief   Call this method from the main thread to stop
    ///          timed execution of your execute_() method.
    ///
    /// \warning You may NOT call this method from inside your
    ///          execute_() callback, or the timer thread will
    ///          not be able to be torn down (the join will hang).
    void stop()
    {
        is_running_ = false;
        if (thread_ != nullptr) {
            thread_->join();
            thread_.reset();
        }
    }

    /// Ask if this timer is currently executing on the background thread.
    bool isRunning() const
    {
        return thread_ != nullptr;
    }

private:
    /// The timer's delayed start callback.
    void start_()
    {
        sleepUntilIntervalEnd_();
        intervalFcn_();
    }

    /// The timer's own interval callback which includes
    /// your execute_() implementation and sleep duration
    /// calculation.
    void intervalFcn_()
    {
        while (is_running_ || !first_execute_occurred_) {
            // Get the time before calling the user's code.
            const Time interval_start = getCurrentTime_();

            try {
                execute_();
                first_execute_occurred_ = true;
            } catch (const InterruptException&) {
                is_running_ = false;
                continue;
            }

            // Take the amount of time it took to execute the user's
            // code, and use that info to sleep for the amount of time
            // that puts the next call to execute_() close to the fixed
            // interval.
            auto interval_end = getCurrentTime_();
            std::chrono::duration<double> user_code_execution_time = interval_end - interval_start;

            const double num_seconds_into_this_interval = user_code_execution_time.count();
            sleepUntilIntervalEnd_(num_seconds_into_this_interval);
        }
    }

    /// Go to sleep until the current time interval has expired.
    ///
    ///     |----------------|----------------|----------------|
    ///     ^
    /// (sleeps until........^)
    ///
    ///     |----------------|----------------|----------------|
    ///                           ^
    ///                       (sleeps until...^)
    void sleepUntilIntervalEnd_(const double offset_seconds = 0)
    {
        const double sleep_seconds = interval_seconds_ - offset_seconds;
        if (sleep_seconds > 0) {
            auto sleep_ms = std::chrono::milliseconds(static_cast<uint64_t>(sleep_seconds * 1000));
            std::this_thread::sleep_for(sleep_ms);
        }
    }

    typedef std::chrono::time_point<std::chrono::high_resolution_clock> Time;

    /// Get the current time to be used in the sleep_for calculation.
    Time getCurrentTime_() const
    {
        return std::chrono::high_resolution_clock::now();
    }

    /// This method will be called at regular intervals on the worker thread.
    virtual void execute_() = 0;

    /// Number of seconds between consecutive calls to execute_()
    const double interval_seconds_;

    /// Worker thread.
    std::unique_ptr<std::thread> thread_;

    /// Flag used for the logic around starting/stopping the thread
    /// and breaking out of the infinite consumer loop.
    bool is_running_ = false;

    /// Flag used to protect against the scenario where the thread
    /// is started, tasks get added to the queue immediately, and
    /// then stop() is called before the consumer loop even has
    /// a chance to begin running on the worker thread. This is
    /// not common in practice, but could happen perhaps in very
    /// short unit tests.
    bool first_execute_occurred_ = false;
};

} // namespace simdb3
