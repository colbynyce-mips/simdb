#pragma once

#include "simdb/collection/TimeLogger.hpp"
#include "simdb/sqlite/ValueContainer.hpp"
#include "simdb/Exceptions.hpp"

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>

namespace simdb
{

class DatabaseManager;

enum class Format
{
    none = 0,
    hex = 1,
    boolalpha = 2
};

/// \class Stat
/// \brief A single named statistic.
template <typename StatT>
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

class TimeseriesValuesCollectorBase
{
public:
    virtual ~TimeseriesValuesCollectorBase() = default;
    virtual void collectAll() = 0;
    virtual std::unordered_map<std::string, std::vector<char>> getValuesByElemPath() const = 0;
};

template <typename T>
inline void memcpy_(std::vector<char>& raw_data, const std::vector<T>& values)
{
    raw_data.resize(values.size() * sizeof(T));
    memcpy(raw_data.data(), values.data(), raw_data.size());
}

template <>
inline void memcpy_<bool>(std::vector<char>& raw_data, const std::vector<bool>& values)
{
    raw_data.resize(values.size() * sizeof(int32_t));
    for (size_t i = 0; i < values.size(); ++i) {
        int32_t val = values[i] ? 1 : 0;
        memcpy(raw_data.data() + i * sizeof(int32_t), &val, sizeof(int32_t));
    }
}

template <typename DataT>
class TimeseriesValuesCollector : public TimeseriesValuesCollectorBase
{
public:
    TimeseriesValuesCollector(const std::vector<Stat<DataT>>& stats)
        : stats_(stats)
    {
    }

    void collectAll() override
    {
        for (const auto& stat : stats_) {
            auto value = stat.getValue();
            values_.push_back(value);
        }
    }

    std::unordered_map<std::string, std::vector<char>> getValuesByElemPath() const override
    {
        std::unordered_map<std::string, std::vector<char>> values_by_elem_path;

        for (size_t i = 0; i < stats_.size(); ++i) {
            std::vector<DataT> stat_values;
            for (size_t j = i; j < values_.size(); j += stats_.size()) {
                stat_values.push_back(values_[j]);
            }

            std::vector<char> raw_data;
            memcpy_(raw_data, stat_values);
            values_by_elem_path[stats_[i].getPath()] = raw_data;
        }

        return values_by_elem_path;
    }

private:
    std::vector<Stat<DataT>> stats_;
    std::vector<DataT> values_;
};

class TimeseriesCollector
{
public:
    TimeseriesCollector(std::unique_ptr<TimeLoggerBase> time_logger)
        : time_logger_(time_logger.release())
    {
    }

    template <typename DataT>
    void addStats(const std::vector<Stat<DataT>>& stats)
    {
        values_collectors_.emplace_back(new TimeseriesValuesCollector<DataT>(stats));
    }

    bool hasStats() const
    {
        return !values_collectors_.empty();
    }

    void enableArgosIPC(const ScalarValueReader<double>& ipc_reader)
    {
        Stat<double> ipc_stat("IPC", ipc_reader, Format::none);
        std::vector<Stat<double>> ipc_stats = {ipc_stat};
        addStats(ipc_stats);
    }

    void collectAll()
    {
        time_logger_->logTime();
        for (auto& collector : values_collectors_) {
            collector->collectAll();
        }
    }

    void onPipelineCollectorClosing(DatabaseManager* db_mgr);

private:
    TimeLoggerBase* time_logger_;
    std::vector<std::unique_ptr<TimeseriesValuesCollectorBase>> values_collectors_;
};

} // namespace simdb
