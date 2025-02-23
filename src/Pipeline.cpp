// <Pipeline.cpp> -*- C++ -*-

#include "simdb/serialize/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb {

PipelineStage::PipelineStage()
    : is_running_(true)
    , thread_(std::bind(&PipelineStage::consume_, this))
{
}

void PipelineStage::push(PipelineStagePayload&& payload)
{
    queue_.emplace(std::move(payload));
}

size_t PipelineStage::count() const
{
    return queue_.size();
}

void PipelineStage::flush(DatabaseManager* db_mgr)
{
    flush_(db_mgr);
    if (next_stage_) {
        next_stage_->flush(db_mgr);
    }
}

void PipelineStage::teardown()
{
    stop_();
    if (next_stage_) {
        next_stage_->teardown();
    }
}

void PipelineStage::flush_(DatabaseManager*)
{
    while (is_running_ && !queue_.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void PipelineStage::stop_()
{
    is_running_ = false;
    if (thread_.joinable()) {
        thread_.join();
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

        processPipelineStage_(payload);
        if (next_stage_) {
            next_stage_->push(std::move(payload));
        }
    }
}

void Pipeline::push(std::vector<char>&& bytes, uint64_t tick)
{
    PipelineStagePayload payload;
    payload.data = std::move(bytes);
    payload.tick = tick;
    payload.db_mgr = db_mgr_;

    auto stage1_proc_time = stage1_.getEstimatedRemainingProcTime(db_mgr_);
    auto stage2_proc_time = stage2_.getEstimatedRemainingProcTime(db_mgr_);

    if (stage1_proc_time <= stage2_proc_time) {
        stage1_.push(std::move(payload));
    } else {
        stage2_.push(std::move(payload));
    }
}

void Pipeline::postSim(DatabaseManager* db_mgr)
{
    stage1_.flush(db_mgr);
    stage1_.teardown();
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

double Pipeline::CompressionWithDatabaseWriteStage::getEstimatedRemainingProcTime(DatabaseManager* db_mgr) const
{
    double est_time = 0;
    if (default_compression_level_) {
        est_time += count() * compression_time_.mean();
    }

    est_time += db_mgr->getConnection()->getTaskQueue()->getEstimatedRemainingProcTime();
    return est_time;
}


void Pipeline::CompressionWithDatabaseWriteStage::processPipelineStage_(PipelineStagePayload& payload)
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

    sendToDatabase_(payload);
}

void Pipeline::CompressionWithDatabaseWriteStage::sendToDatabase_(PipelineStagePayload& payload) const
{
    auto db_mgr = payload.db_mgr;
    const auto & data = payload.data;
    const auto tick = payload.tick;
    const auto compressed = payload.compressed;

    auto task = std::make_unique<CollectionPointDataWriter>(db_mgr, data, tick, compressed);
    db_mgr->getConnection()->getTaskQueue()->addTask(std::move(task));
}

void Pipeline::CompressionWithDatabaseWriteStage::flush_(DatabaseManager* db_mgr)
{
    PipelineStage::flush_(db_mgr);
    db_mgr->getConnection()->getTaskQueue()->flushQueue();
}

} // namespace simdb
