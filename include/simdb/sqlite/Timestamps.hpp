// <ValueContainer> -*- C++ -*-

#pragma once

#include "simdb/sqlite/ValueContainer.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include <rapidjson/document.h>

namespace simdb
{

/// For developer use only.
class TimestampJsonSerializer
{
public:
    virtual ~TimestampJsonSerializer() = default;
    virtual void appendTimestamp(rapidjson::Value& time_vals_array, rapidjson::Document::AllocatorType& allocator) const = 0;
};

/// For developer use only.
class IntegralTimestampJsonSerializer : public TimestampJsonSerializer
{
public:
    IntegralTimestampJsonSerializer(const int64_t time)
        : time_(time)
    {
    }

    void appendTimestamp(rapidjson::Value& time_vals_array, rapidjson::Document::AllocatorType& allocator) const override
    {
        time_vals_array.PushBack(time_, allocator);
    }

private:
    int64_t time_;
};

/// For developer use only.
class FloatingPointTimestampJsonSerializer : public TimestampJsonSerializer
{
public:
    FloatingPointTimestampJsonSerializer(const double time)
        : time_(time)
    {
    }

    void appendTimestamp(rapidjson::Value& time_vals_array, rapidjson::Document::AllocatorType& allocator) const override
    {
        time_vals_array.PushBack(time_, allocator);
    }

private:
    double time_;
};

/*!
 * \class TimestampBase
 *
 * \brief Extends the ValueContainerBase class with extra functionality
 *        specific to capturing timestamps.
 */
class TimestampBase
{
public:
    virtual SqlDataType getDataType() const = 0;
    virtual ValueContainerBase* createBinder() const = 0;
    virtual void captureCurrentTime() = 0;
    virtual bool ensureTimeHasAdvanced() const = 0;
    virtual TimestampJsonSerializer* createTimestampJsonSerializer() const = 0;
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

    SqlDataType getDataType() const override
    {
        return SqlDataType::int32_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new Integral32ValueContainer(time_.getValue());
    }

    void captureCurrentTime() override
    {
        time_snapshot_ = std::make_pair(time_.getValue(), true);
    }

    bool ensureTimeHasAdvanced() const override
    {
        return (!time_snapshot_.second || time_.getValue() > time_snapshot_.first);
    }

    TimestampJsonSerializer* createTimestampJsonSerializer() const override
    {
        return time_snapshot_.second ? new IntegralTimestampJsonSerializer(time_snapshot_.first) : nullptr;
    }

private:
    ScalarValueReader<TimeT> time_;
    std::pair<TimeT, bool> time_snapshot_ = std::make_pair(0, false);
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

    SqlDataType getDataType() const override
    {
        return SqlDataType::int64_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new Integral64ValueContainer(time_.getValue());
    }

    void captureCurrentTime() override
    {
        time_snapshot_ = std::make_pair(time_.getValue(), true);
    }

    bool ensureTimeHasAdvanced() const override
    {
        return (!time_snapshot_.second || time_.getValue() > time_snapshot_.first);
    }

    TimestampJsonSerializer* createTimestampJsonSerializer() const override
    {
        return time_snapshot_.second ? new IntegralTimestampJsonSerializer(time_snapshot_.first) : nullptr;
    }

private:
    ScalarValueReader<TimeT> time_;
    std::pair<TimeT, bool> time_snapshot_ = std::make_pair(0, false);
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

    SqlDataType getDataType() const override
    {
        return SqlDataType::double_t;
    }

    ValueContainerBase* createBinder() const override
    {
        return new FloatingPointValueContainer(time_.getValue());
    }

    void captureCurrentTime() override
    {
        time_snapshot_ = std::make_pair(time_.getValue(), true);
    }

    bool ensureTimeHasAdvanced() const override
    {
        return (!time_snapshot_.second || time_.getValue() > time_snapshot_.first);
    }

    TimestampJsonSerializer* createTimestampJsonSerializer() const override
    {
        return time_snapshot_.second ? new FloatingPointTimestampJsonSerializer(time_snapshot_.first) : nullptr;
    }

private:
    ScalarValueReader<TimeT> time_;
    std::pair<TimeT, bool> time_snapshot_ = std::make_pair(0, false);
};

using TimestampPtr = std::shared_ptr<TimestampBase>;

} // namespace simdb
