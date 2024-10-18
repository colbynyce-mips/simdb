// <Scalars> -*- C++ -*-

#pragma once

#include "simdb3/async/AsyncTaskQueue.hpp"
#include "simdb3/collection/CollectionBase.hpp"
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
                      std::is_same<DataT, double>::value   ||
                      std::is_same<DataT, bool>::value,
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
    void addStat(const std::string& stat_path, const DataT* data_ptr, const std::string& clk_name = "", Format format = Format::none)
    {
        validatePath_(stat_path);

        if (finalized_) {
            throw DBException("Cannot add stat to collection after it's been finalized");
        }

        ScalarValueReader<DataT> reader(data_ptr);
        Stat<DataT> stat(stat_path, reader, format);
        stats_.emplace_back(stat, clk_name);
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
    void addStat(const std::string& stat_path, std::function<DataT()> func_ptr, const std::string& clk_name = "", Format format = Format::none)
    {
        validatePath_(stat_path);

        ScalarValueReader<DataT> reader(func_ptr);
        Stat<DataT> stat(stat_path, reader, format);
        stats_.emplace_back(stat, clk_name);
    }

    /// Get the name of this collection.
    std::string getName() const override
    {
        return name_;
    }

    /// Get if the given element path ("root.child1.child2") is in this collection.
    bool hasElement(const std::string& element_path) const override
    {
        for (const auto& pair : stats_) {
            if (pair.first.getPath() == element_path) {
                return true;
            }
        }
        return false;
    }

    /// Get the element offset in the collection. This is for collections where we
    /// pack all stats of the same data type into the same collection buffer, specifically
    /// StatCollection<T> and ScalarStructCollection<T>.
    int getElementOffset(const std::string& element_path) const override
    {
        for (size_t idx = 0; idx < stats_.size(); ++idx) {
            if (stats_[idx].first.getPath() == element_path) {
                return (int)idx;
            }
        }
        return -1;
    }

    /// Get the type of widget that should be displayed when the given element
    /// is dragged-and-dropped onto the Argos widget canvas.
    std::string getWidgetType(const std::string& element_path) const override
    {
        if (hasElement(element_path)) {
            return "Timeseries";
        }
        return "";
    }

    /// Write metadata about this collection to the database.
    /// Returns the collection's primary key in the Collections table.
    int writeCollectionMetadata(DatabaseManager* db_mgr) override
    {
        if (collection_pkey_ != -1) {
            return collection_pkey_;
        }

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
        } else if (std::is_same<DataT, bool>::value) {
            data_type = "bool";
        } else {
            throw DBException("Invalid DataT");
        }

        auto record = db_mgr->INSERT(SQL_TABLE("Collections"),
                                     SQL_COLUMNS("Name", "DataType", "IsContainer", "IsSparse", "Capacity"),
                                     SQL_VALUES(name_, data_type, 0, 0, (int)stats_.size()));

        collection_pkey_ = record->getId();
        return collection_pkey_;
    }

    /// Set the heartbeat for this collection. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    void setHeartbeat(const size_t heartbeat) override
    {
        (void)heartbeat;
    }

    /// \brief  Write metadata about this collection to the database.
    /// \throws Throws an exception if called more than once.
    void finalize(DatabaseManager* db_mgr) override
    {
        (void)db_mgr;
        finalized_ = true;
    }

    /// \brief  Collect all values in this collection into one data vector
    ///         and write the values to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(CollectionBuffer& buffer) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        buffer.writeHeader(collection_pkey_, stats_.size());
        for (const auto& pair : stats_) {
            const auto& stat = pair.first;
            auto stat_value = stat.getValue();
            buffer.writeBytes(&stat_value, sizeof(DataT));
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

    /// All the stats in this collection together with their clock names.
    std::vector<std::pair<Stat<DataT>, std::string>> stats_;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;
};

} // namespace simdb3
