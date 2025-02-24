#pragma once

#include <stdint.h>

namespace simdb
{

class RunningAverage
{
public:
    // Constructor
    RunningAverage()
        : mean_(0.0)
        , count_(0)
    {
    }

    // Update the running average with a new value
    void add(double value)
    {
        // Welford's method to update the mean
        count_++;
        mean_ += (value - mean_) / count_;
    }

    // Get the current running average
    double mean() const
    {
        return mean_;
    }

    // Get the number of values added
    uint64_t count() const
    {
        return count_;
    }

private:
    double mean_ = 0.0;
    uint64_t count_ = 0;
};

} // namespace simdb
