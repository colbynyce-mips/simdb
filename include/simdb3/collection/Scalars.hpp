// <Scalars> -*- C++ -*-

#pragma once

#include "simdb3/async/AsyncTaskQueue.hpp"
#include "simdb3/collection/CollectionBase.hpp"
#include "simdb3/collection/BlobSerializer.hpp"
#include "simdb3/sqlite/DatabaseManager.hpp"
#include "simdb3/utils/Compress.hpp"
#include "simdb3/utils/TreeSerializer.hpp"

namespace simdb3
{

/*!
 * \class StatCollection
 *
 * \brief It is common in simulators to have many individual stats of the same 
 *        datatype that could belong to the same logical group:
 *  
 *          - all counters in the simulator (e.g. uint64_t)
 *          - histogram bins (e.g. uint32_t)
 *          - all stats for CSV reports (e.g. doubles)
 *          - etc.
 * 
 *        Using the StatCollection feature, you can gather these stats with a
 *        single API call during simulation, such as at every time step or 
 *        every clock cycle, and let SimDB automatically compress these values
 *        to save a significant amount of disk space in your database file.
 */
template <typename DataT>
class StatCollection : public CollectionBase
{
public:
    /// Construct with a name for this collection.
    StatCollection(const std::string& name)
        : name_(name)
    {
        static_assert(std::is_same<DataT, uint8_t>::value  ||
                      std::is_same<DataT, uint16_t>::value ||
                      std::is_same<DataT, uint32_t>::value ||
                      std::is_same<DataT, uint64_t>::value ||
                      std::is_same<DataT, int8_t>::value   ||
                      std::is_same<DataT, int16_t>::value  ||
                      std::is_same<DataT, int32_t>::value  ||
                      std::is_same<DataT, int64_t>::value  ||
                      std::is_same<DataT, float>::value    ||
                      std::is_same<DataT, double>::value,
                      "Invalid DataT for collection");
    }

    /// \brief   Add a stat to this collection using a backpointer to the data value.
    ///
    /// \param   stat_path Unique element path e.g. variable name like "counter_foo", or a 
    ///                    dot-delimited simulator location such as "stats.counters.foo"
    ///
    /// \param   data_ptr Backpointer to the raw data value.
    ///
    /// \warning This pointer will be read every time the collect() method is called.
    ///          You must ensure that this is a valid pointer for the life of the simulation
    ///          else your program will crash or send bogus data to the database.
    ///
    /// \throws  Throws an exception if called after finalize() or if the stat_path is not unique.
    ///          Also throws if the element path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addStat(const std::string& stat_path, const DataT* data_ptr, Format format = Format::none)
    {
        validatePath_(stat_path);

        if (finalized_) {
            throw DBException("Cannot add stat to collection after it's been finalized");
        }

        ScalarValueReader<DataT> reader(data_ptr);
        Stat<DataT> stat(stat_path, reader, format);
        stats_.emplace_back(stat);
    }

    /// \brief   Add a stat to this collection using a function pointer to get the
    ///          data value. This would most commonly be used in favor of the backpointer
    ///          API for calculated stats / evaluated expressions.
    ///
    /// \param   stat_path Unique element path e.g. variable name like "counter_foo", or a 
    ///                    dot-delimited simulator location such as "stats.counters.foo"
    ///
    /// \param   func_ptr Function pointer to get the raw data value.
    ///
    /// \throws  Throws an exception if called after finalize() or if the stat_path is not unique.
    ///          Also throws if the element path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addStat(const std::string& stat_path, std::function<DataT()> func_ptr, Format format = Format::none)
    {
        validatePath_(stat_path);

        ScalarValueReader<DataT> reader(func_ptr);
        Stat<DataT> stat(stat_path, reader, format);
        stats_.emplace_back(stat);
    }

    /// Get the name of this collection.
    std::string getName() const override
    {
        return name_;
    }

    /// \brief  Write metadata about this collection to the database.
    /// \throws Throws an exception if called more than once.
    void finalize(DatabaseManager* db_mgr, TreeNode* root) override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }

        serializeElementTree(db_mgr, root);

        std::string data_type;
        if (std::is_same<DataT, uint8_t>::value) {
            data_type = "uint8_t";
        } else if (std::is_same<DataT, uint16_t>::value) {
            data_type = "uint16_t";
        } else if (std::is_same<DataT, uint32_t>::value) {
            data_type = "uint32_t";
        } else if (std::is_same<DataT, uint64_t>::value) {
            data_type = "uint64_t";
        } else if (std::is_same<DataT, int8_t>::value) {
            data_type = "int8_t";
        } else if (std::is_same<DataT, int16_t>::value) {
            data_type = "int16_t";
        } else if (std::is_same<DataT, int32_t>::value) {
            data_type = "int32_t";
        } else if (std::is_same<DataT, int64_t>::value) {
            data_type = "int64_t";
        } else if (std::is_same<DataT, float>::value) {
            data_type = "float";
        } else if (std::is_same<DataT, double>::value) {
            data_type = "double";
        } else {
            throw DBException("Invalid DataT");
        }

        auto record = db_mgr->INSERT(SQL_TABLE("Collections"),
                                     SQL_COLUMNS("Name", "DataType", "IsContainer"),
                                     SQL_VALUES(name_, data_type, 0));

        collection_pkey_ = record->getId();

        for (const auto& stat : stats_) {
            auto record = db_mgr->INSERT(SQL_TABLE("CollectionElems"),
                                         SQL_COLUMNS("CollectionID", "SimPath"),
                                         SQL_VALUES(collection_pkey_, stat.getPath()));

            db_mgr->INSERT(SQL_TABLE("FormatOpts"),
                           SQL_COLUMNS("ScalarElemID", "FormatCode"),
                           SQL_VALUES(record->getId(), static_cast<int>(stat.getFormat())));
        }

        finalized_ = true;
    }

    /// \brief  Collect all values in this collection into one data vector
    ///         and write the values to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(DatabaseManager* db_mgr, const TimestampBase* timestamp, const bool log_json = false) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        stats_values_.reserve(stats_.size());
        stats_values_.clear();

        for (auto& stat : stats_) {
            stats_values_.push_back(stat.getValue());
            if (log_json) {
                collected_data_vals_[stat.getPath()].push_back(stat.getValue());
            }
        }

        std::unique_ptr<WorkerTask> task(new CollectableSerializer<DataT>(
            db_mgr, collection_pkey_, timestamp, stats_values_, stats_.size()));

        db_mgr->getConnection()->getTaskQueue()->addTask(std::move(task));
    }

    /// For developer use only.
    void addCollectedDataToJSON(rapidjson::Value& data_vals_dict, rapidjson::Document::AllocatorType& allocator) const override
    {
        for (const auto& kvp : collected_data_vals_) {
            rapidjson::Value data_vals{rapidjson::kArrayType};
            for (const auto val : kvp.second) {
                data_vals.PushBack(val, allocator);
            }

            rapidjson::Value elem_path_json;
            elem_path_json.SetString(kvp.first.c_str(), static_cast<rapidjson::SizeType>(kvp.first.length()), allocator);
            data_vals_dict.AddMember(elem_path_json, data_vals, allocator);
        }
    }

private:
    /// \class Stat
    /// \brief A single named statistic.
    template <typename StatT = DataT>
    class Stat
    {
    public:
        Stat(const std::string& stat_path, const ScalarValueReader<StatT>& reader, const Format format)
            : stat_path_(stat_path)
            , reader_(reader)
            , format_(format)
        {
        }

        const std::string& getPath() const
        {
            return stat_path_;
        }

        Format getFormat() const
        {
            return format_;
        }

        StatT getValue() const
        {
            return reader_.getValue();
        }

    private:
        std::string stat_path_;
        ScalarValueReader<StatT> reader_;
        Format format_;
    };

    /// Name of this collection. Serialized to the database.
    std::string name_;

    /// All the stats in this collection.
    std::vector<Stat<DataT>> stats_;

    /// All the stats' values in one vector. Held in a member variable
    /// so we do not reallocate these (potentially large) vectors with
    /// every call to collect(), which can be called very many times
    /// during the simulation.
    std::vector<DataT> stats_values_;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Hold collected data to later be serialized to disk in JSON format.
    /// This is for developer use only.
    ///
    /// {
    ///     "TimeVals": [1, 2, 3],
    ///     "DataVals": {
    ///         "stats.foo": [4.4, 5.5, 6.6]
    ///     }
    /// }
    std::unordered_map<std::string, std::vector<DataT>> collected_data_vals_;
};

} // namespace simdb3
