// <BlobSerializer> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/sqlite/Timestamps.hpp"
#include "simdb/utils/StringMap.hpp"

namespace simdb
{

/// \class CollectableSerializer
/// \brief Writes collection data on the worker thread. This class is used for
///        scalar PODs and scalar structs. To serialize collection data for
///        iterable scalars, use the IterableStructSerializer. 
class CollectableSerializer : public WorkerTask
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    CollectableSerializer(
        DatabaseManager* db_mgr, const int collection_id, const TimestampBase* timestamp, const void* data_ptr, const size_t num_bytes)
        : db_mgr_(db_mgr)
        , collection_id_(collection_id)
        , timestamp_binder_(timestamp->createBinder())
        , unserialized_map_(StringMap::instance()->getUnserializedMap())
    {
        data_vals_.resize(num_bytes);
        memcpy(data_vals_.data(), data_ptr, num_bytes);
        StringMap::instance()->clearUnserializedMap();
    }

    /// Asynchronously write the collection data to the database.
    void completeTask() override
    {
        auto record = db_mgr_->INSERT(SQL_TABLE("CollectionData"),
                                      SQL_COLUMNS("CollectionID", "TimeVal", "DataVals"),
                                      SQL_VALUES(collection_id_, timestamp_binder_, data_vals_));

        collection_data_id_ = record->getId();

        for (const auto& kvp : unserialized_map_) {
            db_mgr_->INSERT(SQL_TABLE("StringMap"),
                            SQL_COLUMNS("IntVal", "String"),
                            SQL_VALUES(kvp.first, kvp.second));
        }
    }

protected:
    /// ID of the CollectionData record.
    int collection_data_id_ = 0;

    /// DatabaseManager used for INSERT().
    DatabaseManager* db_mgr_;

private:
    /// Primary key in the Collections table.
    const int collection_id_;

    /// Timestamp binder holding the timestamp value at the time of construction.
    ValueContainerBasePtr timestamp_binder_;

    /// Data values.
    std::vector<char> data_vals_;

    /// Map of uint32_t->string pairs that need to be written to the database.
    StringMap::unserialized_string_map_t unserialized_map_;
};

/// \class IterableStructSerializer
/// \brief Writes collection data on the worker thread. This class is used for
///        iterable structs only. To serialize collection data for scalar PODs
///        and scalar structs, use the CollectableSerializer.
class IterableStructSerializer : public CollectableSerializer
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    IterableStructSerializer(
        DatabaseManager* db_mgr, const int collection_id, const TimestampBase* timestamp, const void* data_ptr, const size_t num_bytes, const void* valid_flags_ptr, const size_t valid_flags_num_bytes)
        : CollectableSerializer(db_mgr, collection_id, timestamp, data_ptr, num_bytes)
    {
        valid_flags_.resize(valid_flags_num_bytes);
        memcpy(valid_flags_.data(), valid_flags_ptr, valid_flags_num_bytes);
    }

    /// Asynchronously write the collection data to the database.
    void completeTask() override
    {
        CollectableSerializer::completeTask();

        if (!valid_flags_.empty()) {
            db_mgr_->INSERT(SQL_TABLE("SparseValidFlags"),
                            SQL_COLUMNS("CollectionDataID", "CompressedFlags"),
                            SQL_VALUES(collection_data_id_, valid_flags_));
        }
    }

private:
    /// Compressed valid/invalid flag vector. This is used to support sparse containers.
    std::vector<char> valid_flags_;
};

} // namespace simdb
