// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/serialize/Serialize.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Ping.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "simdb/utils/StringMap.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace simdb
{

struct PipelineStagePayload
{
    std::vector<char> data;
    bool compressed = false;
    uint64_t tick = 0;
    DatabaseManager* db_mgr = nullptr;
};

/// Every stage in a pipeline has its own thread and its own processing
/// queue. The pipeline will try to keep these queues balanced in terms
/// of work load.
class PipelineStage
{
public:
    virtual ~PipelineStage() = default;

    void setNextStage(PipelineStage* next_stage)
    {
        next_stage_ = next_stage;
    }

    void push(PipelineStagePayload&& payload)
    {
        queue_.emplace(std::move(payload));
        start_();
    }

    size_t count() const
    {
        return queue_.size();
    }

    virtual void postSim()
    {
        is_running_ = false;
    }

    void teardown()
    {
        stop_();
    }

    /// This method is used by the Pipeline to determine the relative
    /// amount of work/compression each stage is performing.
    virtual double getEstimatedRemainingProcTime() const = 0;

private:
    void start_()
    {
        if (!is_running_ && !thread_)
        {
            is_running_ = true;
            thread_ = std::make_unique<std::thread>(std::bind(&PipelineStage::consume_, this));
        }
    }

    void stop_()
    {
        is_running_ = false;
        if (thread_)
        {
            thread_->join();
        }
    }

    void consume_()
    {
        while (is_running_)
        {
            PipelineStagePayload payload;
            if (!queue_.try_pop(payload))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            if (next_stage_)
            {
                processPipelineStage_(payload);
                next_stage_->push(std::move(payload));
            }
            else
            {
                processPipelineStage_(std::move(payload));
            }
        }
    }

    virtual void processPipelineStage_(PipelineStagePayload& data) = 0;

    virtual void processPipelineStage_(PipelineStagePayload&& data) = 0;

    bool is_running_ = false;
    std::unique_ptr<std::thread> thread_;
    ConcurrentQueue<PipelineStagePayload> queue_;
    PipelineStage* next_stage_ = nullptr;
};

/// This class is responsible for handling compression as well as SQLite
/// database writes for all collected data. It is used by the CollectionMgr
/// in every call to sweep().
///
/// As packets come in for processing, we will dynamically choose which thread
/// to add the packet to:
///
///     Thread A: Heavier compression only (higher compression level, slower algo)
///     Thread B: Faster/no compression, plus a database write
///
/// The pipeline will try to keep these two threads balanced in terms of work
/// load. If one thread is getting too far ahead of the other, we will dial
/// up/down the compression levels for the two threads to even them out.
class Pipeline
{
public:
    Pipeline(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
        stage1_.setNextStage(&stage2_);
    }

    void push(std::vector<char>&& bytes, uint64_t tick);

    void postSim()
    {
        stage1_.postSim();
        stage2_.postSim();
        stage1_.teardown();
        stage2_.teardown();
    }

private:
    class CompressionStage : public PipelineStage
    {
    public:
        void setDefaultCompressionLevel(int level)
        {
            assert(level >= 0);
            default_compression_level_ = level;
        }

        double getEstimatedRemainingProcTime() const override
        {
            if (!default_compression_level_)
            {
                return 0;
            }

            return count() * compression_time_.mean();
        }

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override
        {
            if (default_compression_level_)
            {
                auto begin = std::chrono::high_resolution_clock::now();
                compressDataVec(payload.data, compressed_bytes_, default_compression_level_);
                std::swap(payload.data, compressed_bytes_);
                payload.compressed = true;
                auto end = std::chrono::high_resolution_clock::now();

                // Get number of seconds and add it to the running mean
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
                auto seconds = static_cast<double>(duration.count()) / 1e6;
                compression_time_.add(seconds);
            }
        }

        void processPipelineStage_(PipelineStagePayload&& payload) override
        {
            throw std::runtime_error("Should not be called - I can't take ownership!");
        }

        std::vector<char> compressed_bytes_;
        int default_compression_level_ = 6;
        RunningAverage compression_time_;
    };

    class CompressionWithDatabaseWriteStage : public PipelineStage
    {
    public:
        void setDefaultCompressionLevel(int level)
        {
            assert(level >= 0);
            default_compression_level_ = level;
        }

        double getEstimatedRemainingProcTime() const override
        {
            double est_time = 0;
            if (default_compression_level_)
            {
                est_time += count() * compression_time_.mean();
            }
            est_time += count() * write_time_.mean();
            return est_time;
        }

        void postSim() override
        {
            ping_.postSim();
            PipelineStage::postSim();
        }

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override
        {
            throw std::runtime_error("Should not be called - I have to take ownership!");
        }

        void processPipelineStage_(PipelineStagePayload&& payload) override
        {
            if (!payload.compressed && default_compression_level_)
            {
                auto begin = std::chrono::high_resolution_clock::now();
                compressDataVec(payload.data, compressed_bytes_, default_compression_level_);
                std::swap(payload.data, compressed_bytes_);
                payload.compressed = true;
                auto end = std::chrono::high_resolution_clock::now();

                // Get number of seconds and add it to the running mean
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
                auto seconds = static_cast<double>(duration.count()) / 1e6;
                compression_time_.add(seconds);
            }

            sendToDatabase_(std::move(payload));
        }

        void sendToDatabase_(PipelineStagePayload&& payload);

        std::vector<char> compressed_bytes_;
        int default_compression_level_ = 1;
        RunningAverage compression_time_;
        RunningAverage write_time_;
        ConcurrentQueue<PipelineStagePayload> ready_queue_;
        Ping ping_;
    };

    DatabaseManager* db_mgr_;
    CompressionStage stage1_;
    CompressionWithDatabaseWriteStage stage2_;
};

inline void Pipeline::push(std::vector<char>&& bytes, uint64_t tick)
{
    PipelineStagePayload payload;
    payload.data = std::move(bytes);
    payload.tick = tick;
    payload.db_mgr = db_mgr_;

    // Perform load balancing between the two stages. Both stages
    // can be configured to compress data or not, and only the
    // second stage writes to the database. This simple algo
    // chooses the best time/space tradeoff for most use cases.

    auto stage1_proc_time = stage1_.getEstimatedRemainingProcTime();
    auto stage2_proc_time = stage2_.getEstimatedRemainingProcTime();

    auto total_proc_time = stage1_proc_time + stage2_proc_time;
    auto stage1_pct_proc_time = stage1_proc_time / total_proc_time * 100;

    if (stage1_pct_proc_time < 25)
    {
        stage1_.setDefaultCompressionLevel(6);
        stage2_.setDefaultCompressionLevel(1);
    }
    else if (stage1_pct_proc_time < 50)
    {
        stage1_.setDefaultCompressionLevel(3);
        stage2_.setDefaultCompressionLevel(1);
    }
    else if (stage1_pct_proc_time < 75)
    {
        stage1_.setDefaultCompressionLevel(1);
        stage2_.setDefaultCompressionLevel(3);
    }
    else
    {
        stage1_.setDefaultCompressionLevel(1);
        stage2_.setDefaultCompressionLevel(6);
    }

    if (stage1_pct_proc_time < 50)
    {
        stage1_.push(std::move(payload));
    }
    else
    {
        stage2_.push(std::move(payload));
    }
}

} // namespace simdb
