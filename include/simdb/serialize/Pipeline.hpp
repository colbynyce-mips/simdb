// <Pipeline.hpp> -*- C++ -*-

/**
 * The SimDB pipeline is responsible for handling compression as well as SQLite
 * database writes for all collected data. It is used by the CollectionMgr
 * in every call to sweep().
 *
 * The Pipeline class manages a series of stages, each with its own thread and
 * processing queue. The pipeline attempts to balance the workload across these
 * stages dynamically.
 *
 * The pipeline consists of two main stages:
 * - CompressionStage: Handles data compression.
 * - CompressionWithDatabaseWriteStage: Handles data compression and writes the
 *   compressed data to the database.
 *
 * As packets come in for processing, the pipeline dynamically chooses which
 * thread to add the packet to:
 * - Thread A: Heavier compression only (higher compression level, slower algorithm).
 * - Thread B: Faster/no compression, plus a database write.
 *
 * The pipeline will try to keep these two threads balanced in terms of workload.
 * If one thread is getting too far ahead of the other, the compression levels
 * for the two threads will be adjusted to even them out.
 *
 * @note The pipeline stages communicate via a push mechanism, where each stage
 *       pushes its processed payload to the next stage.
 */

#pragma once

#include "simdb/serialize/Serialize.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentPriorityQueue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Ping.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "simdb/utils/StringMap.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace simdb
{

/// Packet sent through each pipeline stage for incremental processing
/// on a background thread.
struct PipelineStagePayload
{
    std::vector<char> data;
    bool compressed = false;
    uint64_t tick = 0;
    uint64_t payload_id;
    DatabaseManager* db_mgr = nullptr;

    PipelineStagePayload(uint64_t tick, uint64_t payload_id)
        : tick(tick)
        , payload_id(payload_id)
    {
    }

    PipelineStagePayload() : payload_id(0) {}

    PipelineStagePayload& operator=(const PipelineStagePayload& rhs) = default;
};

} // namespace simdb

namespace std
{
template <>
inline bool greater<simdb::PipelineStagePayload>::operator()(const simdb::PipelineStagePayload& lhs,
                                                             const simdb::PipelineStagePayload& rhs) const
{
    return lhs.payload_id > rhs.payload_id;
}
} // namespace std

namespace simdb
{

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
        if (next_stage_)
        {
            next_stage_->postSim();
        }
    }

    void teardown()
    {
        PipelineStage* stage = this;
        while (stage)
        {
            stage->is_running_ = false;
            stage = stage->next_stage_;
        }

        stop_();
        if (next_stage_)
        {
            next_stage_->teardown();
        }
    }

    /// This method is used by the Pipeline to determine the relative
    /// amount of work/compression each stage is performing.
    virtual double getEstimatedRemainingProcTime() const = 0;

protected:
    /// Stop the infinite consume_() loop.
    virtual void stop_()
    {
        is_running_ = false;
        if (thread_)
        {
            thread_->join();
        }
    }

private:
    /// Start the infinite consume_() loop.
    void start_()
    {
        if (!is_running_ && !thread_)
        {
            is_running_ = true;
            thread_ = std::make_unique<std::thread>(std::bind(&PipelineStage::consume_, this));
        }
    }

    /// Run infinite loop to consume data from the queue. All of this occurs
    /// on this pipeline stage's own background thread.
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

    /// Run this stage's processing. This method is called when there is
    /// one or more downstream stages (only work on <data> but cannot take
    /// ownership).
    virtual void processPipelineStage_(PipelineStagePayload& data) = 0;

    /// Run this stages's processing. This method is called when there are
    /// no downstream stages (work on <data> and take ownership).
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

    void teardown()
    {
        // Note that we only have to call these methods on stage1 since
        // it forwards these calls to the next stage.
        stage1_.postSim();
        stage1_.teardown();
    }

private:
    /// This is the first of the two pipeline stages. This stage is responsible
    /// for compressing data only. Typically we will use a higher level of
    /// compression in this stage than in the second stage.
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
        /// Compress the data without taking ownership (the second stage will take ownership).
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
        RunningMean compression_time_;
    };

    /// This is the second of the two pipeline stages. This stage is responsible for
    /// compressing data and writing it to the database. Typically we will use a
    /// lower level of compression in this stage than in the first stage since we
    /// also have to perform the database write.
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

    private:
        void stop_() override
        {
            ping_.teardown();
            PipelineStage::stop_();
        }

        void processPipelineStage_(PipelineStagePayload& payload) override
        {
            throw std::runtime_error("Should not be called - I have to take ownership!");
        }

        /// Compress the data and write it to the database.
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

        /// Append the data to the internal ConcurrentQueue and process this
        /// queue asynchronously (every 1 second). We use the Ping helper class
        /// to drive the once-a-second processing.
        void sendToDatabase_(PipelineStagePayload&& payload);

        /// Each call to sendToDatabase_() puts the payload into our priority queue.
        /// We then move items out of the staging queue into the flush queue when
        /// the payloads are ready. "Ready" means that the payloads are naturally
        /// in the same order as when they first entered the Pipeline, even though
        /// they may arrive at this stage out of order.
        void flushStagingQueue_()
        {
            PipelineStagePayload payload;
            while (staging_queue_.try_pop(payload))
            {
                if (payload.payload_id == next_payload_id_)
                {
                    flush_queue_.push(std::move(payload));
                    ++next_payload_id_;
                }
                else
                {
                    staging_queue_.emplace(std::move(payload));
                    break;
                }
            }
        }

        std::vector<char> compressed_bytes_;
        int default_compression_level_ = 1;
        RunningMean compression_time_;
        RunningMean write_time_;
        ConcurrentPriorityQueue<PipelineStagePayload, std::greater<PipelineStagePayload>> staging_queue_;
        ConcurrentQueue<PipelineStagePayload> flush_queue_;
        std::atomic<uint64_t> next_payload_id_{1};
        Ping ping_;
    };

    static uint64_t getPayloadID_()
    {
        // Note that a payload ID of 0 is invalid
        static std::atomic<uint64_t> payload_id(1);
        return payload_id++;
    }

    DatabaseManager* db_mgr_;
    CompressionStage stage1_;
    CompressionWithDatabaseWriteStage stage2_;
};

inline void Pipeline::push(std::vector<char>&& bytes, uint64_t tick)
{
    PipelineStagePayload payload(tick, getPayloadID_());
    payload.data = std::move(bytes);
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
