// <PerfDiagnostics> -*- C++ -*-

#pragma once

#include "simdb3/Exceptions.hpp"

#include <array>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace simdb3 {

class PerfTimer
{
public:
    PerfTimer()
    {
        restart();
    }

    double elapsedTime() const
    {
        auto end = std::chrono::steady_clock::now();
        auto elap = (std::chrono::duration_cast<std::chrono::microseconds>(end - begin_).count()) / 1000000.0;
        return elap;
    }

    void restart()
    {
        begin_ = std::chrono::steady_clock::now();
    }

private:
    std::chrono::steady_clock::time_point begin_;
};

enum SimPhase
{
    UNSPECIFIED,
    SETUP,
    SIMLOOP,
    TEARDOWN,
    __NUM_PHASES__
};

class PerfDiagnostics
{
public:
    PerfDiagnostics(std::thread::id main_thread_id = std::this_thread::get_id())
    {
        main_thread_id_ = main_thread_id;
        main_thread_commits_.fill(0);
        worker_thread_commits_.fill(0);
    }

    void enterSimPhase(const SimPhase phase)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        switch (phase_) {
            case SimPhase::UNSPECIFIED: phase_ = phase; break;
            case SimPhase::SETUP: {
                if (phase == SimPhase::UNSPECIFIED) {
                    throw DBException("Cannot change sim phase 'backwards'");
                }
                phase_ = phase;
                break;
            }
            case SimPhase::SIMLOOP: {
                if (phase != SimPhase::SIMLOOP && phase != SimPhase::TEARDOWN) {
                    throw DBException("Cannot change sim phase 'backwards'");
                }
                phase_ = phase;
                break;
            }
            case SimPhase::TEARDOWN: {
                if (phase != SimPhase::TEARDOWN) {
                    throw DBException("Cannot change sim phase 'backwards'");
                }
                break;
            }
            case SimPhase::__NUM_PHASES__: {
                throw DBException("Invalid sim phase");
            }
        }
    }

    void onCommitTransaction()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (std::this_thread::get_id() == main_thread_id_) {
            ++main_thread_commits_[phase_];
        } else {
            ++worker_thread_commits_[phase_];
        }
    }

    void onCloseDatabase()
    {
        elap_seconds_ = timer_.elapsedTime();
    }

    void writeReport(std::ostream& os, const std::string& title = "") noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);

        os << "**************** SimDB performance report *****************\n";
        if (!title.empty()) {
            os << title << "\n\n";
        } else {
            os << "\n";
        }

        if (phase_ == SimPhase::UNSPECIFIED) {
            const size_t main_thread_commits = main_thread_commits_[SimPhase::UNSPECIFIED];
            const size_t worker_thread_commits = worker_thread_commits_[SimPhase::UNSPECIFIED];

            os << "Elapsed time," << getFormattedElapTime_() << "\n";
            os << "Main thread commits,Worker thread commits\n";
            os << main_thread_commits << "," << worker_thread_commits << "\n";
        } else {
            const size_t setup_main_thread_commits = main_thread_commits_[SimPhase::UNSPECIFIED] + main_thread_commits_[SimPhase::SETUP];
            const size_t setup_worker_thread_commits = worker_thread_commits_[SimPhase::UNSPECIFIED] + worker_thread_commits_[SimPhase::SETUP];
            const size_t simloop_main_thread_commits = main_thread_commits_[SimPhase::SIMLOOP];
            const size_t simloop_worker_thread_commits = worker_thread_commits_[SimPhase::SIMLOOP];
            const size_t teardown_main_thread_commits = main_thread_commits_[SimPhase::TEARDOWN];
            const size_t teardown_worker_thread_commits = worker_thread_commits_[SimPhase::TEARDOWN];

            os << "Elapsed time," << getFormattedElapTime_() << "\n";
            os << "Sim phase,Main thread commits,Worker thread commits\n";
            os << "SETUP," << setup_main_thread_commits << "," << setup_worker_thread_commits << "\n";

            if (phase_ == SimPhase::SIMLOOP || phase_ == SimPhase::TEARDOWN) {
                os << "SIMLOOP," << simloop_main_thread_commits << "," << simloop_worker_thread_commits << "\n";
            }
            
            if (phase_ == SimPhase::TEARDOWN) {
                os << "TEARDOWN," <<  teardown_main_thread_commits << "," << teardown_worker_thread_commits << "\n";
            }
        }

        os << "***********************************************************" << std::endl;
        report_written_ = true;
    }

    bool reportWritten() const noexcept
    {
        return report_written_;
    }

private:
    std::string getFormattedElapTime_() const
    {
        auto ss = elap_seconds_;

        auto hh = ss / (60 * 60);
        ss -= hh * (60 * 60);

        auto mm = ss / 60;
        ss -= mm * 60;

        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(2) << hh << ':' << std::setw(2) << mm << ':' << std::setw(2) << ss;
        return oss.str();
    }

    std::thread::id main_thread_id_;
    SimPhase phase_ = SimPhase::UNSPECIFIED;
    std::array<size_t, SimPhase::__NUM_PHASES__> main_thread_commits_;
    std::array<size_t, SimPhase::__NUM_PHASES__> worker_thread_commits_;
    uint64_t elap_seconds_ = 0;
    bool report_written_ = false;
    std::mutex mutex_;
    PerfTimer timer_;
};

} // namespace simdb3
