// <ConstellationBase> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteTransaction.hpp"
#include "simdb/sqlite/Timestamps.hpp"
#include "simdb/schema/Schema.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace simdb
{

class DatabaseManager;

/*!
 * \class ConstellationBase
 *
 * \brief Base class for the Constellation class.
 */
class ConstellationBase
{
public:
    /// Destructor
    virtual ~ConstellationBase() = default;

    /// Get the name of this constellation.
    virtual std::string getName() const = 0;

    /// Get the sync/async mode for this constellation.
    virtual bool isSynchronous() const = 0;

    /// Write metadata about this constellation to the database.
    virtual void finalize(DatabaseManager* db_mgr) = 0;

    /// Collect all values in this constellation into one data vector
    /// and write the values to the database.
    virtual void collect(DatabaseManager* db_mgr, const TimestampBase* timestamp) = 0;
};

/*!
 * \class Constellations
 *
 * \brief This class holds onto all user-configured constellations for
 *        an easy way to trigger simulation-wide stat collection.
 */
class Constellations
{
public:
    /// Construct with the DatabaseManager and SQLiteTransaction.
    Constellations(DatabaseManager* db_mgr, SQLiteTransaction* db_conn)
        : db_mgr_(db_mgr)
        , db_conn_(db_conn)
    {
    }

    /// \brief  Use the given backpointer to an integral/double time value
    ///         when adding timestamps to collected constellations.
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
    ///         when adding timestamps to collected constellations.
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
    /// constellations. Must be called after useTimestampsFrom().
    void defineSchema(Schema& schema) const
    {
        if (!timestamp_) {
            throw DBException("Must be called after useTimestampsFrom()");
        }

        using dt = ColumnDataType;

        schema.addTable("ConstellationGlobals")
            .addColumn("TimeType", dt::string_t);

        schema.addTable("Constellations")
            .addColumn("Name", dt::string_t)
            .addColumn("DataType", dt::string_t)
            .addColumn("Compressed", dt::int32_t);

        schema.addTable("ConstellationPaths")
            .addColumn("ConstellationID", dt::int32_t)
            .addColumn("StatPath", dt::string_t);

        schema.addTable("ConstellationData")
            .addColumn("ConstellationID", dt::int32_t)
            .addColumn("TimeVal", timestamp_->getDataType())
            .addColumn("DataVals", dt::blob_t)
            .createCompoundIndexOn({"ConstellationID", "TimeVal"});
    }

    /// \brief  Add a user-configured constellation.
    ///
    /// \throws Throws an exception if a constellation with the same name as
    ///         this one was already added.
    void addConstellation(std::unique_ptr<ConstellationBase> constellation)
    {
        for (const auto& my_constellation : constellations_) {
            if (my_constellation->getName() == constellation->getName()) {
                throw DBException("Constellation with this name already exists: ") << constellation->getName();
            }
        }

        use_safe_transaction_ |= constellation->isSynchronous();
        constellations_.emplace_back(constellation.release());
    }

    /// Called manually during simulation to trigger automatic collection
    /// of all constellations.
    void collectConstellations()
    {
        auto collect = [&]() {
            for (auto& constellation : constellations_) {
                constellation->collect(db_mgr_, timestamp_.get());
            }
        };

        if (use_safe_transaction_) {
            db_conn_->safeTransaction(collect);
        } else {
            collect();
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

    /// One-time finalization of all constellations. Called by friend class DatabaseManager.
    void finalizeConstellations_()
    {
        auto finalize = [&]() {
            for (auto& constellation : constellations_) {
                constellation->finalize(db_mgr_);
            }
        };

        if (use_safe_transaction_) {
            db_conn_->safeTransaction(finalize);
        } else {
            finalize();
        }
    }

    /// DatabaseManager. Needed so we can call finalize() and collect() on the
    /// ConstellationBase objects.
    DatabaseManager* db_mgr_;

    /// SQLiteTransaction. Needed so we can put synchronously serialized constellations
    /// inside BEGIN/COMMIT TRANSACTION calls for best performance.
    SQLiteTransaction* db_conn_;

    /// Flag saying whether we need BEGIN/COMMIT TRANSACTION calls for best performance.
    /// Only applies to synchronously serialized constellations.
    bool use_safe_transaction_ = false;

    /// All user-configured constellations.
    std::vector<std::unique_ptr<ConstellationBase>> constellations_;

    /// This is used to dynamically get the timestamp for each INSERT from either
    /// a user-provided backpointer or a function pointer that can get a timestamp
    /// in either 32/64-bit integers or as floating-point values.
    TimestampPtr timestamp_;

    friend class DatabaseManager;
};

} // namespace simdb
