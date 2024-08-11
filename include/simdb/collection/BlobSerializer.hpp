// <BlobSerializer> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/sqlite/Timestamps.hpp"
#include "simdb/utils/StringMap.hpp"

namespace simdb
{

/// \class CollectionSerializer
/// \brief Writes collection data on the worker thread.
class CollectionSerializer : public WorkerTask
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    CollectionSerializer(
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
        db_mgr_->INSERT(SQL_TABLE("CollectionData"),
                        SQL_COLUMNS("CollectionID", "TimeVal", "DataVals"),
                        SQL_VALUES(collection_id_, timestamp_binder_, data_vals_));

        for (const auto& kvp : unserialized_map_) {
            db_mgr_->INSERT(SQL_TABLE("StringMap"),
                            SQL_COLUMNS("IntVal", "String"),
                            SQL_VALUES(kvp.first, kvp.second));
        }
    }

private:
    /// DatabaseManager used for INSERT().
    DatabaseManager* db_mgr_;

    /// Primary key in the Collections table.
    const int collection_id_;

    /// Timestamp binder holding the timestamp value at the time of construction.
    ValueContainerBasePtr timestamp_binder_;

    /// Data values.
    std::vector<char> data_vals_;

    /// Map of uint32_t->string pairs that need to be written to the database.
    StringMap::unserialized_string_map_t unserialized_map_;
};

} // namespace simdb
