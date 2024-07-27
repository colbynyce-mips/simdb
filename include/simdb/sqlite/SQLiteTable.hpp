// <SQLiteTable.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/ColumnTypedefs.hpp"
#include "simdb/sqlite/SQLiteQuery.hpp"
#include "simdb_fwd.hpp"

#include <algorithm>
#include <list>
#include <sqlite3.h>

namespace simdb
{

//! Helper class that is used under the hood for:
//!   db_mgr.INSERT(SQL_TABLE("MyTable"), SQL_COLUMNS("ColA", "ColB"), SQL_VALUES(3.14, "foo"));
//!                 *********
class SqlTable
{
public:
    SqlTable(const char* table_name)
        : table_name_(table_name)
    {
    }

    //! Get this table's name.
    const std::string& getName() const
    {
        return table_name_;
    }

private:
    std::string table_name_;
};

//! Helper class that is used under the hood for:
//!   db_mgr.INSERT(SQL_TABLE("MyTable"), SQL_COLUMNS("ColA", "ColB"), SQL_VALUES(3.14, "foo"));
//!                                       ***********
class SqlColumns
{
public:
    template <typename... Rest>
    SqlColumns(const char* col_name, Rest... rest)
        : SqlColumns(std::forward<Rest>(rest)...)
    {
        col_names_.emplace_front(col_name);
    }

    SqlColumns(const char* col_name)
    {
        col_names_.emplace_front(col_name);
    }

    void writeColsForINSERT(std::ostringstream& oss) const
    {
        oss << " (";
        auto iter = col_names_.begin();
        size_t idx = 0;
        while (iter != col_names_.end()) {
            oss << *iter;
            if (idx != col_names_.size() - 1) {
                oss << ",";
            }
            ++idx;
            ++iter;
        }
        oss << ") ";
    }

    const std::list<std::string> getColNames() const
    {
        return col_names_;
    }

private:
    std::list<std::string> col_names_;
};

//! Helper class that is used under the hood for:
//!   db_mgr.INSERT(SQL_TABLE("MyTable"), SQL_COLUMNS("ColA", "ColB"), SQL_VALUES(3.14, "foo"));
//!                                                                    **********
class SqlValues
{
public:
    template <typename T, typename... Rest>
    SqlValues(T val, Rest... rest)
        : SqlValues(std::forward<Rest>(rest)...)
    {
        col_vals_.emplace_front(createValueContainer_<T>(val));
    }

    template <typename T>
    SqlValues(T val)
    {
        col_vals_.emplace_front(createValueContainer_<T>(val));
    }

    template <typename T>
    SqlValues(const std::vector<T>& val)
    {
        col_vals_.emplace_front(createValueContainer_(val));
    }

    void writeValsForINSERT(std::ostringstream& oss) const
    {
        oss << " VALUES(";
        size_t count = col_vals_.size();
        for (size_t idx = 0; idx < col_vals_.size(); ++idx) {
            oss << "?";
            if (idx != col_vals_.size() - 1) {
                oss << ",";
            }
        }
        oss << ") ";
    }

    void bindValsForINSERT(sqlite3_stmt* stmt) const
    {
        int32_t idx = 1;
        for (auto& val : col_vals_) {
            auto rc = val->bind(stmt, idx++);
            if (rc) {
                throw DBException("Could not bind to prepared statement. Error code: ") << rc;
            }
        }
    }

private:
    class ValueContainerBase
    {
    public:
        virtual ~ValueContainerBase() = default;
        virtual int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const = 0;
    };

    class Integral32ValueContainer : public ValueContainerBase
    {
    public:
        Integral32ValueContainer(int32_t val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_int(stmt, col_idx, val_);
        }

    private:
        int32_t val_;
    };

    class Integral64ValueContainer : public ValueContainerBase
    {
    public:
        Integral64ValueContainer(int64_t val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_int64(stmt, col_idx, val_);
        }

    private:
        int64_t val_;
    };

    class FloatingPointValueContainer : public ValueContainerBase
    {
    public:
        FloatingPointValueContainer(double val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_double(stmt, col_idx, val_);
        }

    private:
        double val_;
    };

    class StringValueContainer : public ValueContainerBase
    {
    public:
        StringValueContainer(const std::string& val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_text(stmt, col_idx, val_.c_str(), -1, 0);
        }

    private:
        std::string val_;
    };

    class BlobValueContainer : public ValueContainerBase
    {
    public:
        BlobValueContainer(const Blob& val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_blob(stmt, col_idx, val_.data_ptr, (int)val_.num_bytes, 0);
        }

    private:
        Blob val_;
    };

    template <typename T>
    class VectorValueContainer : public ValueContainerBase
    {
    public:
        VectorValueContainer(const std::vector<T>& val)
            : val_(val)
        {
        }

        int32_t bind(sqlite3_stmt* stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_blob(stmt, col_idx, val_.data(), (int)val_.size() * sizeof(T), 0);
        }

    private:
        std::vector<T> val_;
    };

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) <= sizeof(int32_t), ValueContainerBase*>::type
    createValueContainer_(T val)
    {
        return new Integral32ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(int64_t), ValueContainerBase*>::type
    createValueContainer_(T val)
    {
        return new Integral64ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_floating_point<T>::value, ValueContainerBase*>::type createValueContainer_(T val)
    {
        return new FloatingPointValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<typename std::decay<T>::type, const char*>::value, ValueContainerBase*>::type
    createValueContainer_(T val)
    {
        return new StringValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<T, std::string>::value, ValueContainerBase*>::type
    createValueContainer_(const T& val)
    {
        return new StringValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<T, Blob>::value, ValueContainerBase*>::type createValueContainer_(const T& val)
    {
        return new BlobValueContainer(val);
    }

    template <typename T>
    ValueContainerBase* createValueContainer_(const std::vector<T>& val)
    {
        return new VectorValueContainer<T>(val);
    }

    std::list<std::unique_ptr<ValueContainerBase>> col_vals_;
};

//! Helper class that wraps one table record.
class SqlRecord
{
public:
    SqlRecord(const std::string& table_name, const int32_t db_id, sqlite3* db_conn)
        : table_name_(table_name)
        , db_id_(db_id)
        , db_conn_(db_conn)
    {
    }

    //! Get the database ID (primary key) for this record.
    int32_t getId() const
    {
        return db_id_;
    }

    //! SELECT the given column value (int32)
    int32_t getPropertyInt32(const char* col_name) const;

    //! SELECT the given column value (int64)
    int64_t getPropertyInt64(const char* col_name) const;

    //! SELECT the given column value (double)
    double getPropertyDouble(const char* col_name) const;

    //! SELECT the given column value (string)
    std::string getPropertyString(const char* col_name) const;

    //! SELECT the given column value (blob)
    template <typename T>
    std::vector<T> getPropertyBlob(const char* col_name) const;

    //! UPDATE the given column value (int32)
    void setPropertyInt32(const char* col_name, const int32_t val) const;

    //! UPDATE the given column value (int32)
    void setPropertyInt64(const char* col_name, const int64_t val) const;

    //! UPDATE the given column value (int32)
    void setPropertyUInt32(const char* col_name, const uint32_t val) const;

    //! UPDATE the given column value (int32)
    void setPropertyUInt64(const char* col_name, const uint64_t val) const;

    //! UPDATE the given column value (double)
    void setPropertyDouble(const char* col_name, const double val) const;

    //! UPDATE the given column value (string)
    void setPropertyString(const char* col_name, const std::string& val) const;

    //! UPDATE the given column value (blob)
    template <typename T>
    void setPropertyBlob(const char* col_name, const std::vector<T>& val) const;

    //! UPDATE the given column value (blob)
    void setPropertyBlob(const char* col_name, const void* data, const size_t bytes) const;

    //! DELETE this record from its table. Returns TRUE if successful,
    //! FALSE otherwise. Should return FALSE on subsequent calls to this method.
    bool removeFromTable();

private:
    sqlite3_stmt* createGetPropertyStmt_(const char* col_name) const
    {
        std::string cmd = "SELECT ";
        cmd += col_name;
        cmd += " FROM " + table_name_;
        cmd += " WHERE Id=" + std::to_string(db_id_);

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_conn_, cmd.c_str(), -1, &stmt, 0)) {
            throw DBException(sqlite3_errmsg(db_conn_));
        }

        return stmt;
    }

    sqlite3_stmt* createSetPropertyStmt_(const char* col_name) const
    {
        std::string cmd = "UPDATE " + table_name_;
        cmd += " SET ";
        cmd += col_name;
        cmd += "=? WHERE Id=" + std::to_string(db_id_);

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_conn_, cmd.c_str(), -1, &stmt, 0)) {
            throw DBException(sqlite3_errmsg(db_conn_));
        }

        return stmt;
    }

    void stepStatement_(sqlite3_stmt* stmt, const std::initializer_list<int>& ret_codes) const
    {
        auto stringifyRetCodes = [&]() {
            std::ostringstream oss;
            auto iter = ret_codes.begin();
            for (size_t idx = 0; idx < ret_codes.size(); ++idx) {
                oss << *iter;
                if (idx != ret_codes.size() - 1) {
                    oss << ",";
                }
                ++iter;
            }
            return oss.str();
        };

        auto rc = sqlite3_step(stmt);
        if (std::find(ret_codes.begin(), ret_codes.end(), rc) == ret_codes.end()) {
            throw DBException("Unexpected sqlite3_step() return code:\n")
                << "\tActual: " << rc << "\tExpected: " << stringifyRetCodes()
                << "\tError: " << sqlite3_errmsg(db_conn_);
        }
    }

    const std::string table_name_;
    const int32_t db_id_;
    sqlite3* const db_conn_;
};

template <typename T>
inline T queryPropertyValue(const char* table_name, const char* col_name, const int db_id, sqlite3* db_conn)
{
    SqlQuery query(table_name, db_conn);

    T val;
    query.select(col_name, val);
    query.addConstraintForInt("Id", Constraints::EQUAL, db_id);

    auto result_set = query.getResultSet();
    if (!result_set.getNextRecord()) {
        throw DBException("Record not found");
    }

    return val;
}

inline int32_t SqlRecord::getPropertyInt32(const char* col_name) const
{
    return queryPropertyValue<int32_t>(table_name_.c_str(), col_name, db_id_, db_conn_);
}

inline int64_t SqlRecord::getPropertyInt64(const char* col_name) const
{
    return queryPropertyValue<int64_t>(table_name_.c_str(), col_name, db_id_, db_conn_);
}

inline double SqlRecord::getPropertyDouble(const char* col_name) const
{
    return queryPropertyValue<double>(table_name_.c_str(), col_name, db_id_, db_conn_);
}

inline std::string SqlRecord::getPropertyString(const char* col_name) const
{
    return queryPropertyValue<std::string>(table_name_.c_str(), col_name, db_id_, db_conn_);
}

template <typename T>
inline std::vector<T> SqlRecord::getPropertyBlob(const char* col_name) const
{
    return queryPropertyValue<std::vector<T>>(table_name_.c_str(), col_name, db_id_, db_conn_);
}

inline void SqlRecord::setPropertyInt32(const char* col_name, const int32_t val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_int(stmt, 1, val)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyInt64(const char* col_name, const int64_t val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_int64(stmt, 1, val)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyUInt32(const char* col_name, const uint32_t val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_int(stmt, 1, val)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyUInt64(const char* col_name, const uint64_t val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_int64(stmt, 1, val)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyDouble(const char* col_name, const double val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_double(stmt, 1, val)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyString(const char* col_name, const std::string& val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_text(stmt, 1, val.c_str(), -1, 0)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

template <typename T>
inline void SqlRecord::setPropertyBlob(const char* col_name, const std::vector<T>& val) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_blob(stmt, 1, val.data(), val.size() * sizeof(T), 0)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline void SqlRecord::setPropertyBlob(const char* col_name, const void* data, const size_t bytes) const
{
    sqlite3_stmt* stmt = createSetPropertyStmt_(col_name);
    if (sqlite3_bind_blob(stmt, 1, data, bytes, 0)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }
    stepStatement_(stmt, {SQLITE_DONE});
    sqlite3_finalize(stmt);
}

inline bool SqlRecord::removeFromTable()
{
    std::ostringstream oss;
    oss << "DELETE FROM " << table_name_ << " WHERE Id=" << db_id_;
    const auto cmd = oss.str();

    if (sqlite3_exec(db_conn_, cmd.c_str(), nullptr, nullptr, nullptr)) {
        throw DBException(sqlite3_errmsg(db_conn_));
    }

    return sqlite3_changes(db_conn_) == 1;
}

} // namespace simdb

#define SQL_TABLE(name) simdb::SqlTable(name)
#define SQL_COLUMNS(...) simdb::SqlColumns(__VA_ARGS__)
#define SQL_VALUES(...) simdb::SqlValues(__VA_ARGS__)
