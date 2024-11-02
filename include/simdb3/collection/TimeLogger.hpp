#pragma once

#include "simdb3/sqlite/ValueContainer.hpp"

namespace simdb3
{

class TimeLoggerBase
{
public:
    virtual ~TimeLoggerBase() = default;
    virtual void logTime() = 0;
    virtual std::vector<char> getTimeValsBlob() const = 0;
};

template <typename TimeT, typename Enable=void>
struct TimeValsBlobGetter;

template <typename TimeT>
struct TimeValsBlobGetter<TimeT, std::enable_if_t<std::is_integral<TimeT>::value>>
{
    static std::vector<char> getBlob(const std::vector<TimeT>& time_vals)
    {
        std::vector<uint64_t> time_vals_64(time_vals.begin(), time_vals.end());
        std::vector<char> blob;
        blob.resize(time_vals_64.size() * sizeof(uint64_t));
        memcpy(blob.data(), time_vals_64.data(), blob.size());
        return blob;
    }
};

template <typename TimeT>
struct TimeValsBlobGetter<TimeT, std::enable_if_t<std::is_floating_point<TimeT>::value>>
{
    static std::vector<char> getBlob(const std::vector<TimeT>& time_vals)
    {
        std::vector<double> time_vals_64(time_vals.begin(), time_vals.end());
        std::vector<char> blob;
        blob.resize(time_vals_64.size() * sizeof(double));
        memcpy(blob.data(), time_vals_64.data(), blob.size());
        return blob;
    }
};

template <typename TimeT>
class TimeLogger: public TimeLoggerBase
{
public:
    TimeLogger(const ScalarValueReader<TimeT>& time_reader)
        : time_reader_(time_reader)
    {
    }

    void logTime() override
    {
        time_vals_.push_back(time_reader_.getValue());
    }

    std::vector<char> getTimeValsBlob() const
    {
        return TimeValsBlobGetter<TimeT>::getBlob(time_vals_);
    }

private:
    ScalarValueReader<TimeT> time_reader_;
    std::vector<TimeT> time_vals_;
};

} // namespace simdb3
