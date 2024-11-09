#pragma once

#include "simdb/sqlite/ValueContainer.hpp"
#include <type_traits>

namespace simdb
{

class TimeLoggerBase
{
public:
    virtual ~TimeLoggerBase() = default;
    virtual void logTime() = 0;
    virtual std::vector<char> getTimeValsBlob() const = 0;
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

    std::vector<char> getTimeValsBlob() const override
    {
        return getTimeValsBlobImpl_();
    }

private:
    template <typename time_t = TimeT>
    typename std::enable_if<std::is_integral<time_t>::value, std::vector<char>>::type
    getTimeValsBlobImpl_() const
    {
        std::vector<uint64_t> time_vals_64(time_vals_.begin(), time_vals_.end());
        std::vector<char> blob;
        blob.resize(time_vals_64.size() * sizeof(uint64_t));
        memcpy(blob.data(), time_vals_64.data(), blob.size());
        return blob;
    }

    template <typename time_t = TimeT>
    typename std::enable_if<std::is_floating_point<time_t>::value, std::vector<char>>::type
    getTimeValsBlobImpl_() const
    {
        std::vector<double> time_vals_dbl(time_vals_.begin(), time_vals_.end());
        std::vector<char> blob;
        blob.resize(time_vals_dbl.size() * sizeof(double));
        memcpy(blob.data(), time_vals_dbl.data(), blob.size());
        return blob;
    }

    ScalarValueReader<TimeT> time_reader_;
    std::vector<TimeT> time_vals_;
};

} // namespace simdb
