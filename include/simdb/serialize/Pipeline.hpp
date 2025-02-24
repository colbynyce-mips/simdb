// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/Compress.hpp"
#include "simdb/serialize/Serialize.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "simdb/utils/StringMap.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Ping.hpp"

#include <chrono>
#include <thread>
#include <mutex>
#include <queue>
#include <memory>

namespace simdb {

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

    void push(PipelineStagePayload&& payload);

    size_t count() const;

    virtual void postSim();

    void teardown();

    /// This method is used by the Pipeline to determine the relative
    /// amount of work/compression each stage is performing.
    virtual double getEstimatedRemainingProcTime() const = 0;

private:
    void start_();

    void stop_();

    void consume_();

    virtual void processPipelineStage_(PipelineStagePayload& data) = 0;

    virtual void processPipelineStage_(PipelineStagePayload&& data) = 0;

    bool is_running_ = false;
    bool can_restart_ = true;
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

    void postSim();

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
            if (!default_compression_level_) {
                return 0;
            }

            return count() * compression_time_.mean();
        }

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override;

        void processPipelineStage_(PipelineStagePayload&& payload) override {
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

        double getEstimatedRemainingProcTime() const override;

        void postSim() override
        {
            ping_.postSim();
            PipelineStage::postSim();
        }

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override {
            throw std::runtime_error("Should not be called - I have to take ownership!");
        }

        void processPipelineStage_(PipelineStagePayload&& payload) override;

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

} // namespace simdb
