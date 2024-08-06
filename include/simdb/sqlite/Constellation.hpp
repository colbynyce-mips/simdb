// <Constellation> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/sqlite/ConstellationBase.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include <cstring>

namespace simdb
{

/*!
 * \class Constellation
 *
 * \brief It is common in simulators to have many individual stats of the same 
 *        datatype that could belong to the same logical group:
 *  
 *          - all counters in the simulator (e.g. uint64_t)
 *          - histogram bins (e.g. uint32_t)
 *          - all stats for CSV reports (e.g. doubles)
 *          - etc.
 * 
 *        Using the Constellation feature, you can gather these stats with a
 *        single API call during simulation, such as at every time step or 
 *        every clock cycle, and optionally let SimDB automatically compress  
 *        these values on the AsyncTaskQueue thread to save a significant  
 *        amount of disk space in your database file.
 */
template <typename DataT, typename TimeT, AsyncModes amode = AsyncModes::ASYNC, CompressionModes cmode = CompressionModes::COMPRESSED>
class Constellation : public ConstellationWithTime<TimeT>
{
public:
    /// Construct with a name and a backpointer to the time value.
    Constellation(const std::string& name, const TimeT* time_ptr)
        : ConstellationWithTime<TimeT>(time_ptr)
        , name_(name)
    {
    }

    /// Construct with a name and a function pointer to get the time value.
    Constellation(const std::string& name, std::function<TimeT()> func_ptr)
        : ConstellationWithTime<TimeT>(func_ptr)
        , name_(name)
    {
    }

    /// \brief   Add a stat to this constellation using a backpointer to the data value.
    ///
    /// \param   stat_path Unique stat path e.g. variable name like "counter_foo", or a 
    ///                    dot-delimited simulator location such as "stats.counters.foo"
    ///
    /// \param   data_ptr Backpointer to the raw data value.
    ///
    /// \warning This pointer will be read every time the collect() method is called.
    ///          You must ensure that this is a valid pointer for the life of the simulation
    ///          else your program will crash or send bogus data to the database.
    ///
    /// \throws  Throws an exception if called after finalize() or if the stat_path is not unique.
    ///          Also throws if the stat path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addStat(const std::string& stat_path, const DataT* data_ptr)
    {
        validateStatPath_(stat_path);

        if (finalized_) {
            throw DBException("Cannot add stat to constellation after it's been finalized");
        }

        if (!stat_paths_.insert(stat_path).second) {
            throw DBException("Cannot add stat to constellation - already have a stat with this path: ") << stat_path;
        }

        ScalarValueReader<DataT> reader(data_ptr);
        Stat<DataT> stat(stat_path, reader);
        stats_.emplace_back(stat);
    }

    /// \brief   Add a stat to this constellation using a function pointer to get the
    ///          data value. This would most commonly be used in favor of the backpointer
    ///          API for calculated stats / evaluated expressions.
    ///
    /// \param   stat_path Unique stat path e.g. variable name like "counter_foo", or a 
    ///                    dot-delimited simulator location such as "stats.counters.foo"
    ///
    /// \param   func_ptr Function pointer to get the raw data value.
    ///
    /// \throws  Throws an exception if called after finalize() or if the stat_path is not unique.
    ///          Also throws if the stat path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addStat(const std::string& stat_path, std::function<DataT()> func_ptr)
    {
        validateStatPath_(stat_path);

        if (finalized_) {
            throw DBException("Cannot add stat to constellation after it's been finalized");
        }

        if (!stat_paths_.insert(stat_path).second) {
            throw DBException("Cannot add stat to constellation - already have a stat with this path: ") << stat_path;
        }

        ScalarValueReader<DataT> reader(func_ptr);
        Stat<DataT> stat(stat_path, reader);
        stats_.emplace_back(stat);
    }

    /// Get the name of this constellation.
    std::string getName() const override
    {
        return name_;
    }

    /// Get the sync/async mode for this constellation.
    bool isSynchronous() const override
    {
        return (cmode == CompressionModes::COMPRESSED);
    }

    /// \brief  Write metadata about this constellation to the database.
    /// \throws Throws an exception if called more than once.
    void finalize(DatabaseManager* db_mgr) override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a constellation more than once");
        }

        std::string time_type;
        if (std::is_integral<TimeT>::value) {
            time_type = "INT";
        } else if (std::is_floating_point<TimeT>::value) {
            time_type = "REAL";
        }

        int compressed = (cmode == CompressionModes::COMPRESSED) ? 1 : 0;

        auto record = db_mgr->INSERT(SQL_TABLE("Constellations"),
                                     SQL_COLUMNS("Name", "TimeType", "Compressed"),
                                     SQL_VALUES(name_, time_type, compressed));

        constellation_pkey_ = record->getId();

        for (const auto& stat : stats_) {
            db_mgr->INSERT(SQL_TABLE("ConstellationPaths"),
                           SQL_COLUMNS("ConstellationID", "StatPath"),
                           SQL_VALUES(constellation_pkey_, stat.getPath()));
        }

        finalized_ = true;
    }

    /// \brief  Collect all values in this constellation into one data vector
    ///         and write the values to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(DatabaseManager* db_mgr) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a constellation before calling finalize()");
        }

        stats_values_.reserve(stats_.size());
        stats_values_.clear();

        for (auto& stat : stats_) {
            stats_values_.push_back(stat.getValue());
        }

        const bool compress = (cmode == CompressionModes::COMPRESSED);
        const bool async = (amode == AsyncModes::ASYNC);

        const void* data_ptr;
        size_t num_bytes;

        if (compress) {
            compressDataVec(stats_values_, stats_values_compressed_);
            data_ptr = stats_values_compressed_.data();
            num_bytes = stats_values_compressed_.size() * sizeof(char);
        } else {
            data_ptr = stats_values_.data();
            num_bytes = stats_values_.size() * sizeof(DataT);
        }

        const auto time_val = this->getCurrentTime();

        std::unique_ptr<WorkerTask> task(new ConstellationSerializer(db_mgr, constellation_pkey_, time_val, data_ptr, num_bytes));

        if (async) {
            db_mgr->getConnection()->getTaskQueue()->addTask(std::move(task));
        } else {
            task->completeTask();
        }
    }

private:
    /// Validate that the stat path is either a valid python variable name, or a
    /// dot-delimited path of valid python variable names:
    ///
    ///   counter_foo              VALID
    ///   stats.counters.foo       VALID
    ///   5_counter_foo            INVALID
    ///   stats.counters?.foo      INVALID 
    void validateStatPath_(std::string stat_path)
    {
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
                oss << "Invalid stat ID for constellation '" << name_ << "'. Not a valid python variable name: " << varname;
                throw DBException(oss.str());
            }
        }
    }

    /// \class ConstellationSerializer
    /// \brief Writes constellation data on the worker thread.
    class ConstellationSerializer : public WorkerTask
    {
    public:
        /// Construct with a timestamp and the data values, whether compressed or not.
        ConstellationSerializer(
            DatabaseManager* db_mgr, const int constellation_id, double time_val, const void* data_ptr, const size_t num_bytes)
            : db_mgr_(db_mgr)
            , constellation_id_(constellation_id)
            , time_val_(time_val)
        {
            data_vals_.resize(num_bytes);
            memcpy(data_vals_.data(), data_ptr, num_bytes);
        }

        /// Asynchronously write the constellation data to the database.
        void completeTask() override
        {
            db_mgr_->INSERT(SQL_TABLE("ConstellationData"),
                            SQL_COLUMNS("ConstellationID", "TimeVal", "DataVals"),
                            SQL_VALUES(constellation_id_, time_val_, data_vals_));
        }

    private:
        /// DatabaseManager used for INSERT().
        DatabaseManager* db_mgr_;

        /// Primary key in the Constellations table.
        const int constellation_id_;

        /// Timestamp at the time of construction.
        const double time_val_;

        /// Data values.
        std::vector<char> data_vals_;
    };

    /// \class Stat
    /// \brief A single named statistic.
    template <typename StatT = DataT>
    class Stat
    {
    public:
        Stat(const std::string& stat_path, const ScalarValueReader<StatT>& reader)
            : stat_path_(stat_path)
            , reader_(reader)
        {
        }

        const std::string& getPath() const
        {
            return stat_path_;
        }

        StatT getValue() const
        {
            return reader_.getValue();
        }

    private:
        std::string stat_path_;
        ScalarValueReader<StatT> reader_;
    };

    /// Name of this constellation. Serialized to the database.
    std::string name_;

    /// All the stats in this constellation.
    std::vector<Stat<DataT>> stats_;

    /// Quick lookup to ensure that stat ids are all unique.
    std::unordered_set<std::string> stat_paths_;

    /// All the stats' values in one vector. Held in a member variable
    /// so we do not reallocate these (potentially large) vectors with
    /// every call to collect(), which can be called very many times
    /// during the simulation.
    std::vector<DataT> stats_values_;

    /// All the stats' compressed values in one vector.
    std::vector<char> stats_values_compressed_;

    /// Flag saying whether we can add more stats to this constellation.
    bool finalized_ = false;

    /// Our primary key in the Constellations table.
    int constellation_pkey_ = -1;
};

} // namespace simdb
