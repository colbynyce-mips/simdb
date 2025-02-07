// <CollectionBase> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb/sqlite/Timestamps.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include "simdb/utils/StringMap.hpp"
#include "simdb/utils/TreeBuilder.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/collection/TimeseriesCollector.hpp"
#include "simdb/collection/TimeLogger.hpp"

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <cstring>

namespace simdb
{

class DatabaseManager;

/*!
 * \class CollectionBuffer
 *
 * \brief A helper class to allow collections to write their data
 *        to a single buffer before sending it to the background
 *        thread for database insertion. We pack everything into
 *        one buffer to minimize the number of entries we have in
 *        the database, and to get maximum compression.
 */
class CollectionBuffer
{
public:
    CollectionBuffer(std::vector<char> &all_collection_data)
        : all_collection_data_(all_collection_data)
    {
        all_collection_data_.clear();
        all_collection_data_.reserve(all_collection_data_.capacity());
    }

    void writeHeader(uint16_t collection_id, uint16_t num_elems)
    {
        all_collection_data_.resize(all_collection_data_.size() + 2 * sizeof(uint16_t));
        auto dest = all_collection_data_.data() + all_collection_data_.size() - 2 * sizeof(uint16_t);
        memcpy(dest, &collection_id, sizeof(uint16_t));
        memcpy(dest + sizeof(uint16_t), &num_elems, sizeof(uint16_t));
    }

    void writeBucket(uint16_t bucket_id)
    {
        all_collection_data_.resize(all_collection_data_.size() + sizeof(uint16_t));
        auto dest = all_collection_data_.data() + all_collection_data_.size() - sizeof(uint16_t);
        memcpy(dest, &bucket_id, sizeof(uint16_t));
    }

    template <typename T>
    void writeBytes(const T* data, size_t num_bytes)
    {
        all_collection_data_.resize(all_collection_data_.size() + num_bytes);
        auto dest = all_collection_data_.data() + all_collection_data_.size() - num_bytes;
        memcpy(dest, data, num_bytes);
    }

private:
    std::vector<char> &all_collection_data_;
};

template <>
inline void CollectionBuffer::writeBytes<bool>(const bool* data, size_t num_bytes)
{
    for (size_t idx = 0; idx < num_bytes; ++idx) {
        int32_t val = data[idx] ? 1 : 0;
        writeBytes(&val, sizeof(int32_t));
    }
}

/*!
 * \class CollectionBase
 *
 * \brief Base class for all collection classes.
 */
class CollectionBase
{
public:
    /// Destructor
    virtual ~CollectionBase() = default;

    /// Get the name of this collection.
    virtual std::string getName() const = 0;

    /// Get if the given element path ("root.child1.child2") is in this collection.
    virtual bool hasElement(const std::string& element_path) const = 0;

    /// Get the element offset in the collection. This is for collections where we
    /// pack all stats of the same data type into the same collection buffer, specifically
    /// StatCollection<T> and ScalarStructCollection<T>.
    virtual int getElementOffset(const std::string& element_path) const = 0;

    /// Get the type of widget that should be displayed when the given element
    /// is dragged-and-dropped onto the Argos widget canvas.
    virtual std::string getWidgetType(const std::string& element_path) const = 0;

    /// Write metadata about this collection to the database.
    /// Returns the collection's primary key in the Collections table.
    virtual int writeCollectionMetadata(DatabaseManager* db_mgr) = 0;

    /// Set the heartbeat for this collection. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    virtual void setHeartbeat(const size_t heartbeat) = 0;

    /// If this collection is a scalar statistic (timeseries) then it will allow the
    /// TimeseriesCollector to gather the timeseries during simulation on its behalf.
    virtual bool rerouteTimeseries(TimeseriesCollector* timeseries_collector) = 0;

    /// Finalize this collection.
    virtual void finalize() = 0;

    /// Collect all values in this collection into one data vector
    /// and write the values to the database.
    virtual void collect(CollectionBuffer& buffer) = 0;

    /// Give collections a chance to write to the database after simulation.
    virtual void onPipelineCollectorClosing(DatabaseManager* db_mgr) = 0;

    /// Allow the Collections class to verify that all simulator paths
    /// across all collections are unique.
    const std::unordered_set<std::string>& getElemPaths() const
    {
        return element_paths_;
    }

    std::string getClockDomain(const std::unordered_map<std::string, std::string>& clks_by_location) const
    {
        std::string clk_name;
        for (const auto& el_path : element_paths_) {
            auto iter = clks_by_location.find(el_path);
            if (iter != clks_by_location.end()) {
                sparta_assert(clk_name.empty() || clk_name == iter->second);
                clk_name = iter->second;
            }
        }
        return clk_name;
    }

protected:
    /// Validate that the path (to a stat, struct, or container) is either a valid python 
    /// variable name, or a dot-delimited path of valid python variable names:
    ///
    ///   counter_foo              VALID
    ///   stats.counters.foo       VALID
    ///   5_counter_foo            INVALID
    ///   stats.counters?.foo      INVALID 
    void validatePath_(std::string stat_path)
    {
        if (!element_paths_.insert(stat_path).second) {
            throw DBException("Cannot add stat to collection - already have a stat with this path: ") << stat_path;
        }

        auto validate_python_var = [&](const std::string& varname) {
            if (varname.empty() || (!isalpha(varname[0]) && varname[0] != '_')) {
                return false;
            }

            for (char ch : varname) {
                if (!isalnum(ch) && ch != '_') {
                    return false;
                }
            }

            return true;
        };

        std::vector<std::string> varnames;
        const char* delim = ".";
        char* token = std::strtok(const_cast<char*>(stat_path.c_str()), delim);

        while (token) {
            varnames.push_back(token);
            token = std::strtok(nullptr, delim);
        }

        for (const auto& varname : varnames) {
            if (!validate_python_var(varname)) {
                std::ostringstream oss;
                oss << "Not a valid python variable name: " << varname;
                throw DBException(oss.str());
            }
        }

        if (finalized_) {
            throw DBException("Cannot add stat to collection after it's been finalized");
        }
    }

    /// Flag saying whether we can add more stats to this collection.
    bool finalized_ = false;

private:
    /// Quick lookup to ensure that element paths are all unique.
    std::unordered_set<std::string> element_paths_;
};

/*!
 * \class Collections
 *
 * \brief This class holds onto all user-configured collections for
 *        an easy way to trigger simulation-wide stat collection.
 */
class Collections
{
public:
    /// Construct with the DatabaseManager and SQLiteTransaction.
    Collections(DatabaseManager* db_mgr, SQLiteTransaction* db_conn)
        : db_mgr_(db_mgr)
        , db_conn_(db_conn)
    {
    }

    /// \brief  Use the given backpointer to an integral/double time value
    ///         when adding timestamps to collected collections.
    ///
    /// \throws Throws an exception if called a second time with a different
    ///         timestamp type (call 1st time with uint64_t, call 2nd time
    ///         with double, THROWS).
    template <typename TimeT>
    void useTimestampsFrom(const TimeT* back_ptr)
    {
        if (timestamp_) {
            throw DBException("Cannot change the timestamp once already set!");
        }
        timestamp_ = createTimestamp_<TimeT>(back_ptr);
    }

    /// \brief  Use the given function pointer to an integral/double time value
    ///         when adding timestamps to collected collections.
    ///
    /// \throws Throws an exception if called a second time with a different
    ///         timestamp type (call 1st time with uint64_t, call 2nd time
    ///         with double, THROWS).
    template <typename TimeT>
    void useTimestampsFrom(TimeT(*func_ptr)())
    {
        if (timestamp_) {
            throw DBException("Cannot change the timestamp once already set!");
        }
        timestamp_ = createTimestamp_<TimeT>(func_ptr);
    }

    /// \brief  Use the given function pointer to an integral/double time value
    ///         when adding timestamps to collected collections.
    ///
    /// \throws Throws an exception if called a second time with a different
    ///         timestamp type (call 1st time with uint64_t, call 2nd time
    ///         with double, THROWS).
    template <typename TimeT>
    void useTimestampsFrom(std::function<TimeT()> func_ptr)
    {
        if (timestamp_) {
            throw DBException("Cannot change the timestamp once already set!");
        }
        timestamp_ = createTimestamp_<TimeT>(func_ptr);
    }

    /// Set the heartbeat for all collections. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    void setHeartbeat(size_t heartbeat)
    {
        pipeline_heartbeat_ = heartbeat;
    }

    /// Get the heartbeat for all collections.
    size_t getHeartbeat() const
    {
        return pipeline_heartbeat_;
    }

    /// Set the compression level for all collections. This is the zlib compression
    /// level, where 0 is no compression, 1 is fastest, and 9 is best compression.
    void setCompressionLevel(int level)
    {
        compression_level_ = level;
    }

    /// Populate the schema with the appropriate tables for all the
    /// collections. Must be called after useTimestampsFrom().
    void defineSchema(Schema& schema) const
    {
        if (!timestamp_) {
            throw DBException("Must be called after useTimestampsFrom()");
        }

        using dt = SqlDataType;

        schema.addTable("CollectionGlobals")
            .addColumn("TimeType", dt::string_t)
            .addColumn("Heartbeat", dt::int32_t)
            .setColumnDefaultValue("Heartbeat", 10);

        schema.addTable("Collections")
            .addColumn("Name", dt::string_t)
            .addColumn("DataType", dt::string_t)
            .addColumn("IsContainer", dt::int32_t)
            .addColumn("IsSparse", dt::int32_t)
            .addColumn("Capacity", dt::int32_t);

        schema.addTable("CollectionData")
            .addColumn("TimeVal", timestamp_->getDataType())
            .addColumn("DataVals", dt::blob_t)
            .addColumn("IsCompressed", dt::int32_t)
            .createIndexOn("TimeVal");

        schema.addTable("TimeseriesData")
            .addColumn("ElementPath", dt::string_t)
            .addColumn("DataValsBlob", dt::blob_t)
            .addColumn("IsCompressed", dt::int32_t)
            .createIndexOn("ElementPath");

        schema.addTable("AllTimeVals")
            .addColumn("TimeValsBlob", dt::blob_t)
            .addColumn("IsCompressed", dt::int32_t);

        schema.addTable("StructFields")
            .addColumn("StructName", dt::int32_t)
            .addColumn("FieldName", dt::string_t)
            .addColumn("FieldType", dt::string_t)
            .addColumn("FormatCode", dt::int32_t)
            .addColumn("IsAutoColorizeKey", dt::int32_t)
            .addColumn("IsDisplayedByDefault", dt::int32_t)
            .setColumnDefaultValue("IsAutoColorizeKey", 0)
            .setColumnDefaultValue("IsDisplayedByDefault", 1);

        schema.addTable("EnumDefns")
            .addColumn("EnumName", dt::string_t)
            .addColumn("EnumValStr", dt::string_t)
            .addColumn("EnumValBlob", dt::blob_t)
            .addColumn("IntType", dt::string_t);

        schema.addTable("StringMap")
            .addColumn("IntVal", dt::int32_t)
            .addColumn("String", dt::string_t);

        schema.addTable("ElementTreeNodes")
            .addColumn("Name", dt::string_t)
            .addColumn("ParentID", dt::int32_t)
            .addColumn("ClockID", dt::int32_t)
            .addColumn("CollectionID", dt::int32_t)
            .addColumn("CollectionOffset", dt::int32_t)
            .addColumn("WidgetType", dt::string_t);

        schema.addTable("Clocks")
            .addColumn("Name", dt::string_t)
            .addColumn("Period", dt::int32_t);

        schema.addTable("QueueMaxSizes")
            .addColumn("CollectionID", dt::int32_t)
            .addColumn("MaxSize", dt::int32_t)
            .setColumnDefaultValue("MaxSize", -1);
    }

    /// Add a clock with the given name and period.
    void addClock(const std::string& name, const uint32_t period)
    {
        clk_periods_[name] = period;
    }

    /// Associate a clock with a specific collectable element.
    void setClock(const std::string& location, const std::string& clk_name)
    {
        if (clk_periods_.count(clk_name) == 0) {
            throw DBException("Clock not found: ") << clk_name;
        }

        clks_by_location_[location] = clk_name;
    }

    /// \brief  Add a user-configured collection.
    ///
    /// \throws Throws an exception if a collection with the same name as
    ///         this one was already added.
    void addCollection(std::unique_ptr<CollectionBase> collection)
    {
        for (const auto& my_collection : collections_) {
            if (my_collection->getName() == collection->getName()) {
                throw DBException("Collection with this name already exists: ") << collection->getName();
            }
        }

        for (const auto& path : collection->getElemPaths()) {
            if (!element_paths_.insert(path).second) {
                throw DBException("Cannot add stat to collection - already have a stat with this path: ") << path;
            }
        }

        collection->rerouteTimeseries(timeseries_collector_.get());
        collections_.emplace_back(collection.release());
    }

    /// Called manually during simulation to trigger automatic collection
    /// of all collections.
    void collectAll();

    /// Collect only those collectables that exist on the given clock.
    void collectDomain(const std::string& clk_name);

    /// One-time chance to write anything to the database after simulation.
    void onPipelineCollectorClosing()
    {
        if (timeseries_collector_) {
            timeseries_collector_->onPipelineCollectorClosing(db_mgr_);
        }

        for (auto& collection : collections_) {
            collection->onPipelineCollectorClosing(db_mgr_);
        }
    }

private:
    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr)
    {
        ScalarValueReader<TimeT> reader(back_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt32<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)())
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt32<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), TimestampPtr>::type
    createTimestamp_(std::function<TimeT()> func_ptr)
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt32<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr)
    {
        ScalarValueReader<TimeT> reader(back_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt64<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)())
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt64<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), TimestampPtr>::type
    createTimestamp_(std::function<TimeT()> func_ptr)
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampInt64<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_floating_point<TimeT>::value, TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr)
    {
        ScalarValueReader<TimeT> reader(back_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampDouble<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_floating_point<TimeT>::value, TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)())
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampDouble<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_floating_point<TimeT>::value, TimestampPtr>::type
    createTimestamp_(std::function<TimeT()> func_ptr)
    {
        ScalarValueReader<TimeT> reader(func_ptr);
        std::unique_ptr<TimeLoggerBase> time_logger(new TimeLogger<TimeT>(reader));
        timeseries_collector_.reset(new TimeseriesCollector(std::move(time_logger)));

        return std::make_shared<TimestampDouble<TimeT>>(func_ptr);
    }

    /// One-time finalization of all collections. Called by friend class DatabaseManager.
    void finalizeCollections_();

    /// One-time creation of a TreeNode data structure for all collections to be serialized
    /// to SimDB.
    std::unique_ptr<TreeNode> createElementTree_();

    /// Assign clock ID's, collection ID's, collection offsets, and widget types to all
    /// elements in the tree.
    void assignElementMetadata_(TreeNode* root);

    /// Write all element metadata to the ElementTreeNodes table.
    void serializeElementTree_(TreeNode* root, const int parent_db_id = 0);

    /// DatabaseManager. Needed so we can call finalize() and collect() on the
    /// CollectionBase objects.
    DatabaseManager* db_mgr_;

    /// SQLiteTransaction. Needed so we can put synchronously serialized collections
    /// inside BEGIN/COMMIT TRANSACTION calls for best performance.
    SQLiteTransaction* db_conn_;

    /// All user-configured collections.
    std::vector<std::unique_ptr<CollectionBase>> collections_;

    /// This is used to dynamically get the timestamp for each INSERT from either
    /// a user-provided backpointer or a function pointer that can get a timestamp
    /// in either 32/64-bit integers or as floating-point values.
    TimestampPtr timestamp_;

    /// The max number of cycles that we employ the optimization "only write to the
    /// database if the collected data is different from the last collected data".
    /// This prevents Argos from having to go back more than N cycles to find the
    /// last known value.
    size_t pipeline_heartbeat_ = 10;

    /// Quick lookup to ensure that element paths are all unique.
    std::unordered_set<std::string> element_paths_;

    /// All clocks added to the collections (name -> period).
    std::unordered_map<std::string, uint32_t> clk_periods_;

    /// All clocks associated with specific collectable elements (location -> clock name).
    std::unordered_map<std::string, std::string> clks_by_location_;

    /// Single buffer to hold onto all collected data for all collections. Held as a member
    /// variable so we can avoid re-allocating this buffer every time we collect all collections.
    std::vector<char> all_collection_data_;

    /// Single buffer to hold onto all compressed data for all collections. Held as a member
    /// variable so we can avoid re-allocating this buffer every time we collect all collections.
    std::vector<char> all_compressed_data_;

    /// All timeseries collections will be handled separately. It needs special handling to
    /// support Argos UI performance.
    std::unique_ptr<TimeseriesCollector> timeseries_collector_;

    /// Compression level. This starts out as the default compromise between speed and compression,
    /// and will gradually move towards fastest compression if the worker thread is falling behind.
    /// Note that the levels are 0-9, where 0 is no compression, 1 is fastest, and 9 is best compression.
    /// We currently do not go all the way to zero compression or the database will be too large.
    int compression_level_ = 6;

    /// Keep track of the "highwater mark" representing the number of tasks in the queue at
    /// the time of each collection. Start with a highwater mark of 5 so we do not inadvertently
    /// lower the compression level too soon. We want to give the worker thread a chance to catch up.
    size_t num_tasks_highwater_mark_ = 5;

    /// Keep track of how many times the highwater mark is exceeded. When it reaches 3, we will
    /// decrement the compression level to make it go faster and reset this count back to 0.
    size_t num_times_highwater_mark_exceeded_ = 0;

    friend class DatabaseManager;
};

} // namespace simdb
