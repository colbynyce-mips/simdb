// <ValueContainer> -*- C++ -*-

#pragma once

#include "simdb/sqlite/ValueContainer.hpp"

namespace simdb
{

/*!
 * \class TimestampBase
 *
 * \brief Extends the ValueContainerBase class with extra functionality
 *        specific to capturing timestamps.
 */
class TimestampBase
{
public:
    virtual ColumnDataType getDataType() const = 0;
    virtual ValueContainerBase* createBinder() const = 0;
};

/*!
 * \class TimestampInt32
 *
 * \brief This class is used so we have the ability to collect
 *        timestamps as integral values (32-bit).
 * 
 * It uses either backpointers or function pointers to get the
 * timestamp values and bind them to INSERT statements.
 */
template <typename TimeT>
class TimestampInt32 : public TimestampBase
{
public:
    /// Construct with a backpointer to the time value.
    TimestampInt32(const TimeT* back_ptr)
        : time_(back_ptr)
    {
        static_assert(std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), "Invalid TimeT");
    }

    /// Construct with a function pointer to get the time value.
    TimestampInt32(std::function<TimeT()> func_ptr)
        : time_(func_ptr)
    {
        static_assert(std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint32_t), "Invalid TimeT");
    }

    ColumnDataType getDataType() const override
    {
        return ColumnDataType::int32_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new Integral32ValueContainer(time_.getValue());
    }

private:
    ScalarValueReader<TimeT> time_;
};

/*!
 * \class TimestampInt64
 *
 * \brief This class is used so we have the ability to collect
 *        timestamps as integral values (64-bit).
 * 
 * It uses either backpointers or function pointers to get the
 * timestamp values and bind them to INSERT statements.
 */
template <typename TimeT>
class TimestampInt64 : public TimestampBase
{
public:
    /// Construct with a backpointer to the time value.
    TimestampInt64(const TimeT* time_ptr)
        : time_(time_ptr)
    {
        static_assert(std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), "Invalid TimeT");
    }

    /// Construct with a function pointer to get the time value.
    TimestampInt64(std::function<TimeT()> func_ptr)
        : time_(func_ptr)
    {
        static_assert(std::is_integral<TimeT>::value && sizeof(TimeT) == sizeof(uint64_t), "Invalid TimeT");
    }

    ColumnDataType getDataType() const override
    {
        return ColumnDataType::int64_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new Integral64ValueContainer(time_.getValue());
    }

private:
    ScalarValueReader<TimeT> time_;
};

/*!
 * \class TimestampDouble
 *
 * \brief This class is used so we have the ability to collect
 *        timestamps as floating-point values.
 * 
 * It uses either backpointers or function pointers to get the
 * timestamp values and bind them to INSERT statements.
 */
template <typename TimeT>
class TimestampDouble : public TimestampBase
{
public:
    /// Construct with a backpointer to the time value.
    TimestampDouble(const TimeT* time_ptr)
        : time_(time_ptr)
    {
        static_assert(std::is_floating_point<TimeT>::value, "Invalid TimeT");
    }

    /// Construct with a function pointer to get the time value.
    TimestampDouble(std::function<TimeT()> func_ptr)
        : time_(func_ptr)
    {
        static_assert(std::is_floating_point<TimeT>::value, "Invalid TimeT");
    }

    ColumnDataType getDataType() const override
    {
        return ColumnDataType::double_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new FloatingPointValueContainer(time_.getValue());
    }

private:
    ScalarValueReader<TimeT> time_;
};

using TimestampPtr = std::shared_ptr<TimestampBase>;

} // namespace simdb
