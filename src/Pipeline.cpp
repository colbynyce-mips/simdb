// <Pipeline.cpp> -*- C++ -*-

#include "simdb/serialize/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb {

void PipelineStage::push(PipelineStagePayload&& payload)
{
    queue_.emplace(std::move(payload));
    start_();
}

size_t PipelineStage::count() const
{
    return queue_.size();
}

void PipelineStage::postSim()
{
    is_running_ = false;
}

void PipelineStage::teardown()
{
    stop_();
}

void PipelineStage::start_()
{
    if (!is_running_ && !thread_) {
        is_running_ = true;
        thread_ = std::make_unique<std::thread>(std::bind(&PipelineStage::consume_, this));
    }
}

void PipelineStage::stop_()
{
    is_running_ = false;
    if (thread_) {
        thread_->join();
    }
}

void PipelineStage::consume_()
{
    while (is_running_) {
        PipelineStagePayload payload;
        if (!queue_.try_pop(payload)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        if (next_stage_) {
            processPipelineStage_(payload);
            next_stage_->push(std::move(payload));
        } else {
            processPipelineStage_(std::move(payload));
        }
    }
}

void Pipeline::push(std::vector<char>&& bytes, uint64_t tick)
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

    if (stage1_pct_proc_time < 25) {
        stage1_.setDefaultCompressionLevel(6);
        stage2_.setDefaultCompressionLevel(1);
    } else if (stage1_pct_proc_time < 50) {
        stage1_.setDefaultCompressionLevel(3);
        stage2_.setDefaultCompressionLevel(1);
    } else if (stage1_pct_proc_time < 75) {
        stage1_.setDefaultCompressionLevel(1);
        stage2_.setDefaultCompressionLevel(3);
    } else {
        stage1_.setDefaultCompressionLevel(1);
        stage2_.setDefaultCompressionLevel(6);
    }

    if (stage1_pct_proc_time < 50) {
        stage1_.push(std::move(payload));
    } else {
        stage2_.push(std::move(payload));
    }
}

void Pipeline::postSim()
{
    stage1_.postSim();
    stage2_.postSim();
    stage1_.teardown();
    stage2_.teardown();
}

void Pipeline::CompressionStage::processPipelineStage_(PipelineStagePayload& payload)
{
    if (default_compression_level_) {
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

double Pipeline::CompressionWithDatabaseWriteStage::getEstimatedRemainingProcTime() const
{
    double est_time = 0;
    if (default_compression_level_) {
        est_time += count() * compression_time_.mean();
    }
    est_time += count() * write_time_.mean();
    return est_time;
}

void Pipeline::CompressionWithDatabaseWriteStage::processPipelineStage_(PipelineStagePayload&& payload)
{
    if (!payload.compressed && default_compression_level_) {
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

    ready_queue_.emplace(std::move(payload));

    if (ping_) {
        auto db_mgr = payload.db_mgr;
        db_mgr->safeTransaction([&](){
            PipelineStagePayload payload;
            while (ready_queue_.try_pop(payload)) {
                const auto& data = payload.data;
                const auto tick = payload.tick;
                const auto compressed = payload.compressed;

                auto begin = std::chrono::high_resolution_clock::now();

                db_mgr->INSERT(SQL_TABLE("CollectionRecords"),
                               SQL_COLUMNS("Tick", "Data", "IsCompressed"),
                               SQL_VALUES(tick, data, (int)compressed));

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
                auto seconds = static_cast<double>(duration.count()) / 1e6;
                write_time_.add(seconds);
            }

            StringMap::instance()->clearUnserializedMap();

            // Note that we don't add this to the write_time_ running mean calculation
            // since this map is going to shrink to nothing over time (basically it is
            // amortized for real use cases).
            for (const auto& kvp : StringMap::instance()->getUnserializedMap()) {
                db_mgr->INSERT(SQL_TABLE("StringMap"),
                               SQL_COLUMNS("IntVal", "String"),
                               SQL_VALUES(kvp.first, kvp.second));
            }

            return true;
        });
    }
}

} // namespace simdb
