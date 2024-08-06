// <ConstellationBase> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteTransaction.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace simdb
{

class DatabaseManager;

enum class ValueReaderTypes {
    BACKPOINTER,
    FUNCPOINTER
};

/*!
 * \class ScalarValueReader
 *
 * \brief Helper class to store either backpointers or function pointers
 *        in the same vector / data structure. Used for reading values
 *        from objects' member variables or getter functions.
 */
template <typename T>
class ScalarValueReader
{
public:
    typedef struct {
        ValueReaderTypes getter_type;
        const T* backpointer;
        std::function<T()> funcpointer;
    } ValueReader;

    /// Construct with a backpointer to the data value.
    ScalarValueReader(const T* data_ptr)
    {
        reader_.backpointer = data_ptr;
        reader_.getter_type = ValueReaderTypes::BACKPOINTER;

        static_assert(std::is_integral<T>::value || std::is_floating_point<T>::value,
                      "ScalarValueReader only work for integral and floating-point types!");
    }

    /// Construct with a function pointer to get the data.
    ScalarValueReader(std::function<T()> func_ptr)
    {
        reader_.funcpointer = func_ptr;
        reader_.getter_type = ValueReaderTypes::FUNCPOINTER;

        static_assert(std::is_integral<T>::value || std::is_floating_point<T>::value,
                      "ScalarValueReader only work for integral and floating-point types!");
    }

    /// Read the data value.
    T getValue() const
    {
        if (reader_.getter_type == ValueReaderTypes::BACKPOINTER) {
            return *reader_.backpointer;
        } else {
            return reader_.funcpointer();
        }
    }

private:
    ValueReader reader_;
};

/*!
 * \class ConstellationBase
 *
 * \brief Base class for the ConstellationWithTime class.
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
    virtual void collect(DatabaseManager* db_mgr) = 0;
};

/*!
 * \class ConstellationWithTime<TimeT>
 *
 * \brief Extend the ConstellationBase class with the ability to collect
 *        timestamps as e.g. uint64_t or double values.
 */
template <typename TimeT>
class ConstellationWithTime : public ConstellationBase
{
public:
    /// Construct with a backpointer to the time value.
    ConstellationWithTime(const TimeT* time_ptr)
        : time_(time_ptr)
    {
    }

    /// Construct with a function pointer to get the time value.
    ConstellationWithTime(std::function<TimeT()> func_ptr)
        : time_(func_ptr)
    {
    }

    /// Read the current time value.
    TimeT getCurrentTime() const
    {
        return time_.getValue();
    }

private:
    ScalarValueReader<TimeT> time_;
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

    /// One-time finalization of all constellations.
    void finalizeConstellations()
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

    /// Called manually during simulation to trigger automatic collection
    /// of all constellations.
    void collectConstellations()
    {
        auto collect = [&]() {
            for (auto& constellation : constellations_) {
                constellation->collect(db_mgr_);
            }
        };

        if (use_safe_transaction_) {
            db_conn_->safeTransaction(collect);
        } else {
            collect();
        }
    }

private:
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
};

} // namespace simdb
