// <CollectionBase> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb/sqlite/Timestamps.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include "simdb/utils/StringMap.hpp"
#include "simdb/utils/TreeBuilder.hpp"

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <cstring>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include "rapidjson/prettywriter.h"
#include <rapidjson/stringbuffer.h>

namespace simdb
{

class DatabaseManager;

enum class Format
{
    none = 0,
    hex = 1
};

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

    /// Write metadata about this collection to the database.
    virtual void finalize(DatabaseManager* db_mgr, TreeNode* root) = 0;

    /// Collect all values in this collection into one data vector
    /// and write the values to the database.
    virtual void collect(DatabaseManager* db_mgr, const TimestampBase* timestamp, const bool log_json = false) = 0;

    /// Allow the Collections class to verify that all simulator paths
    /// across all collections are unique.
    const std::unordered_set<std::string>& getElemPaths() const
    {
        return element_paths_;
    }

    /// For developer use only.
    virtual void addCollectedDataToJSON(rapidjson::Value& data_vals_dict, rapidjson::Document::AllocatorType& allocator) const = 0;

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
            if (varname.empty() || !isalpha(varname[0]) && varname[0] != '_') {
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
        auto new_timestamp = createTimestamp_<TimeT>(back_ptr);
        if (timestamp_ && timestamp_->getDataType() != new_timestamp->getDataType()) {
            throw DBException("Cannot change the timestamp data type!");
        }
        timestamp_ = new_timestamp;
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
        auto new_timestamp = createTimestamp_<TimeT>(func_ptr);
        if (timestamp_ && timestamp_->getDataType() != new_timestamp->getDataType()) {
            throw DBException("Cannot change the timestamp data type!");
        }
        timestamp_ = new_timestamp;
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
            .addColumn("TimeType", dt::string_t);

        schema.addTable("Collections")
            .addColumn("Name", dt::string_t)
            .addColumn("DataType", dt::string_t)
            .addColumn("IsContainer", dt::int32_t);

        schema.addTable("CollectionElems")
            .addColumn("CollectionID", dt::int32_t)
            .addColumn("SimPath", dt::string_t);

        schema.addTable("CollectionData")
            .addColumn("CollectionID", dt::int32_t)
            .addColumn("TimeVal", timestamp_->getDataType())
            .addColumn("DataVals", dt::blob_t)
            .addColumn("NumElems", dt::int32_t)
            .createCompoundIndexOn({"CollectionID", "TimeVal"});

        schema.addTable("StructFields")
            .addColumn("CollectionName", dt::string_t)
            .addColumn("FieldName", dt::string_t)
            .addColumn("FieldType", dt::string_t)
            .addColumn("FormatCode", dt::int32_t);

        schema.addTable("EnumDefns")
            .addColumn("EnumName", dt::string_t)
            .addColumn("EnumValStr", dt::string_t)
            .addColumn("EnumValBlob", dt::blob_t)
            .addColumn("IntType", dt::string_t);

        schema.addTable("StringMap")
            .addColumn("IntVal", dt::int32_t)
            .addColumn("String", dt::string_t);

        schema.addTable("ContainerMeta")
            .addColumn("PathID", dt::int32_t)
            .addColumn("Capacity", dt::int32_t)
            .addColumn("IsSparse", dt::int32_t);

        schema.addTable("IterableBlobMeta")
            .addColumn("CollectionDataID", dt::int32_t)
            .addColumn("SparseValidFlags", dt::blob_t);

        schema.addTable("FormatOpts")
            .addColumn("ScalarElemID", dt::int32_t)
            .addColumn("FormatCode", dt::int32_t);

        schema.addTable("ElementTreeNodes")
            .addColumn("Name", dt::string_t)
            .addColumn("ParentID", dt::int32_t);
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

        collections_.emplace_back(collection.release());
    }

    /// Called manually during simulation to trigger automatic collection
    /// of all collections.
    void collectAll()
    {
        if (!timestamp_->ensureTimeHasAdvanced()) {
            throw DBException("Cannot perform  - time has not advanced");
        }
        for (auto& collection : collections_) {
            collection->collect(db_mgr_, timestamp_.get(), json_logging_enabled_);
        }
        timestamp_->captureCurrentTime();
        json_timestamps_.emplace_back(timestamp_->createTimestampJsonSerializer());
    }

    /// For developer use only.
    void enableJsonLogging()
    {
        json_logging_enabled_ = true;
    }

    /// For developer use only.
    void serializeJSON(const std::string& filename, const bool pretty = false) const
    {
        if (!json_logging_enabled_) {
            return;
        }

        rapidjson::Document doc;
        rapidjson::Value data_vals_json{rapidjson::kObjectType};

        for (const auto& collection : collections_) {
            collection->addCollectedDataToJSON(data_vals_json, doc.GetAllocator());
        }

        rapidjson::Value time_vals_json{rapidjson::kArrayType};
        for (const auto& timestamp : json_timestamps_) {
            timestamp->appendTimestamp(time_vals_json, doc.GetAllocator());
        }

        doc.SetObject();
        doc.AddMember("TimeVals", time_vals_json, doc.GetAllocator());
        doc.AddMember("DataVals", data_vals_json, doc.GetAllocator());

        if (pretty) {
            rapidjson::StringBuffer buffer;
            rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
            doc.Accept(writer);

            std::ofstream fout(filename);
            fout << buffer.GetString() << std::endl;
        } else {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            doc.Accept(writer);

            std::ofstream fout(filename);
            fout << buffer.GetString() << std::endl;
        }
    }

private:
    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr) const
    {
        return std::make_shared<TimestampInt32<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)()) const
    {
        return std::make_shared<TimestampInt32<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr) const
    {
        return std::make_shared<TimestampInt64<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)()) const
    {
        return std::make_shared<TimestampInt64<TimeT>>(func_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_floating_point<TimeT>::value, TimestampPtr>::type
    createTimestamp_(const TimeT* back_ptr) const
    {
        return std::make_shared<TimestampDouble<TimeT>>(back_ptr);
    }

    template <typename TimeT>
    typename std::enable_if<std::is_floating_point<TimeT>::value, TimestampPtr>::type
    createTimestamp_(TimeT(*func_ptr)()) const
    {
        return std::make_shared<TimestampDouble<TimeT>>(func_ptr);
    }

    /// One-time finalization of all collections. Called by friend class DatabaseManager.
    void finalizeCollections_()
    {
        db_conn_->safeTransaction([&]() {
            auto root = createElementTree_();
            for (auto& collection : collections_) {
                collection->finalize(db_mgr_, root.get());
                root.reset();
            }
            return true;
        });
    }

    std::unique_ptr<TreeNode> createElementTree_()
    {
        std::vector<std::string> all_element_paths;
        for (const auto& collection : collections_) {
            for (const auto& path : collection->getElemPaths()) {
                all_element_paths.push_back(path);
            }
        }

        return buildTree(all_element_paths);
    }

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

    /// Quick lookup to ensure that element paths are all unique.
    std::unordered_set<std::string> element_paths_;

    /// Developer use only.
    bool json_logging_enabled_ = false;

    /// Developer use only.
    std::vector<std::unique_ptr<TimestampJsonSerializer>> json_timestamps_;

    friend class DatabaseManager;
};

} // namespace simdb
