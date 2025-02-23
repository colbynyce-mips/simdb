// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/Compress.hpp"
#include "simdb/serialize/Serialize.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "simdb/utils/StringMap.hpp"
#include "simdb/async/AsyncTaskQueue.hpp"

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

class CollectionPointDataWriter : public WorkerTask
{
public:
    CollectionPointDataWriter(DatabaseManager* db_mgr, const std::vector<char>& data, int64_t tick, bool compressed)
        : db_mgr_(db_mgr)
        , data_(data)
        , tick_(tick)
        , compressed_(compressed)
        , unserialized_map_(StringMap::instance()->getUnserializedMap())
    {
        StringMap::instance()->clearUnserializedMap();
    }

private:
    void completeTask() override;

    DatabaseManager* db_mgr_;
    std::vector<char> data_;
    int64_t tick_;
    bool compressed_;
    StringMap::unserialized_string_map_t unserialized_map_;
};

/// Every stage in a pipeline has its own thread and its own processing
/// queue. The pipeline will try to keep these queues balanced in terms
/// of work load.
class PipelineStage
{
public:
    PipelineStage();

    virtual ~PipelineStage() = default;

    void setNextStage(PipelineStage* next_stage)
    {
        next_stage_ = next_stage;
    }

    void push(PipelineStagePayload&& payload);

    size_t count() const;

    void flush(DatabaseManager* db_mgr);

    void teardown();

    /// This method is used by the Pipeline to determine the relative
    /// amount of work/compression each stage is performing.
    virtual double getEstimatedRemainingProcTime(DatabaseManager* db_mgr) const = 0;

protected:
    virtual void flush_(DatabaseManager*);

private:
    void start_();

    void stop_();

    void consume_();

    virtual void processPipelineStage_(PipelineStagePayload& data) = 0;

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

    void postSim(DatabaseManager* db_mgr);

private:
    class CompressionStage : public PipelineStage
    {
    public:
        void setDefaultCompressionLevel(int level)
        {
            assert(level >= 0);
            default_compression_level_ = level;
        }

        double getEstimatedRemainingProcTime(DatabaseManager*) const override
        {
            if (!default_compression_level_) {
                return 0;
            }

            return count() * compression_time_.mean();
        }

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override;

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

        double getEstimatedRemainingProcTime(DatabaseManager* db_mgr) const override;

    private:
        void processPipelineStage_(PipelineStagePayload& payload) override;

        void sendToDatabase_(PipelineStagePayload& payload) const;

        void flush_(DatabaseManager* db_mgr) override;

        std::vector<char> compressed_bytes_;
        int default_compression_level_ = 1;
        RunningAverage compression_time_;
    };

    DatabaseManager* db_mgr_;
    CompressionStage stage1_;
    CompressionWithDatabaseWriteStage stage2_;
};

} // namespace simdb
