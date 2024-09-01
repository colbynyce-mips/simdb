// <BlobSerializer> -*- C++ -*-

#pragma once

#include "simdb3/async/AsyncTaskQueue.hpp"
#include "simdb3/sqlite/DatabaseManager.hpp"
#include "simdb3/sqlite/Timestamps.hpp"
#include "simdb3/utils/StringMap.hpp"
#include "simdb3/utils/Compress.hpp"

namespace simdb3
{

/// \class CollectableSerializer
/// \brief Writes collection data on the worker thread. This class is used for
///        scalar PODs and scalar structs. To serialize collection data for
///        iterable scalars, use the IterableStructSerializer. 
template <typename DataT>
class CollectableSerializer : public WorkerTask
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    CollectableSerializer(
        DatabaseManager* db_mgr, const int collection_id, const TimestampBase* timestamp, const std::vector<DataT>& data, const size_t num_elements_in_blob, const bool compress = true)
        : db_mgr_(db_mgr)
        , compress_(compress)
        , collection_id_(collection_id)
        , timestamp_binder_(timestamp->createBinder())
        , data_vals_(data)
        , num_elems_in_blob_(num_elements_in_blob)
        , unserialized_map_(StringMap::instance()->getUnserializedMap())
    {
        StringMap::instance()->clearUnserializedMap();
    }

    /// Asynchronously write the collection data to the database.
    void completeTask() override
    {
        if (compress_) {
            std::vector<char> compressed_data;
            compressDataVec(data_vals_, compressed_data);

            auto record = db_mgr_->INSERT(SQL_TABLE("CollectionData"),
                                          SQL_COLUMNS("CollectionID", "TimeVal", "DataVals", "NumElems"),
                                          SQL_VALUES(collection_id_, timestamp_binder_, compressed_data, num_elems_in_blob_));

            collection_data_id_ = record->getId();
        } else {
            auto record = db_mgr_->INSERT(SQL_TABLE("CollectionData"),
                                          SQL_COLUMNS("CollectionID", "TimeVal", "DataVals", "NumElems"),
                                          SQL_VALUES(collection_id_, timestamp_binder_, data_vals_, num_elems_in_blob_));

            collection_data_id_ = record->getId();
        }

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

    /// Should we compress on the worker thread?
    bool compress_;

private:
    /// Primary key in the Collections table.
    const int collection_id_;

    /// Timestamp binder holding the timestamp value at the time of construction.
    ValueContainerBasePtr timestamp_binder_;

    /// Data values.
    std::vector<DataT> data_vals_;

    /// Total number of elements written in this blob.
    size_t num_elems_in_blob_;

    /// Map of uint32_t->string pairs that need to be written to the database.
    StringMap::unserialized_string_map_t unserialized_map_;
};

/// \class IterableStructSerializer
/// \brief Writes collection data on the worker thread. This class is used for
///        iterable structs only. To serialize collection data for scalar PODs
///        and scalar structs, use the CollectableSerializer.
template <typename DataT=char>
class IterableStructSerializer : public CollectableSerializer<DataT>
{
public:
    /// Construct with a timestamp and the data values, whether compressed or not.
    IterableStructSerializer(
        DatabaseManager* db_mgr, const int collection_id, const TimestampBase* timestamp, const std::vector<DataT>& data, const std::vector<int>& valid_flags, const size_t num_structs_written, const bool compress = true)
        : CollectableSerializer<DataT>(db_mgr, collection_id, timestamp, data, num_structs_written, compress)
        , valid_flags_(valid_flags)
    {
    }

    /// Asynchronously write the collection data to the database.
    void completeTask() override
    {
        CollectableSerializer<DataT>::completeTask();

        if (valid_flags_.empty()) {
            return;
        }

        if (this->compress_) {
            std::vector<char> compressed_data;
            compressDataVec(valid_flags_, compressed_data);

            this->db_mgr_->INSERT(SQL_TABLE("IterableBlobMeta"),
                                  SQL_COLUMNS("CollectionDataID", "SparseValidFlags"),
                                  SQL_VALUES(this->collection_data_id_, compressed_data));
        } else {
            this->db_mgr_->INSERT(SQL_TABLE("IterableBlobMeta"),
                                  SQL_COLUMNS("CollectionDataID", "SparseValidFlags"),
                                  SQL_VALUES(this->collection_data_id_, valid_flags_));
        }
    }

private:
    /// Valid/invalid flag vector. This is used to support sparse containers.
    std::vector<int> valid_flags_;
};

} // namespace simdb3
