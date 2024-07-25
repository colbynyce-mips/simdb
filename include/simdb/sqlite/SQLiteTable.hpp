// <SQLiteTable.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteConnection.hpp"
#include "simdb/schema/Schema.hpp"
#include "simdb_fwd.hpp"

namespace simdb {

//! Helper class that is used under the hood for:
//!   db_mgr.INSERT(SQL_TABLE("MyTable"), SQL_COLUMNS("ColA", "ColB"), SQL_VALUES(3.14, "foo"));
//!                 *********
class SqlTable
{
public:
    SqlTable(const char * table_name)
        : table_name_(table_name)
    {}

    const std::string & getName() const
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
    SqlColumns(const char * col_name, Rest... rest)
        : SqlColumns(std::forward<Rest>(rest)...)
    {
        col_names_.emplace_front(col_name);
    }

    SqlColumns(const char * col_name)
    {
        col_names_.emplace_front(col_name);
    }

    void writeColsForINSERT(std::ostringstream & oss) const
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
    SqlValues(const std::vector<T> & val)
    {
        col_vals_.emplace_front(createValueContainer_(val));
    }

    void writeValsForINSERT(std::ostringstream & oss) const
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

    void bindValsForINSERT(sqlite3_stmt * stmt) const
    {
        int32_t idx = 1;
        for (auto & val : col_vals_) {
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
        virtual int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const = 0;
    };

    class Integral32ValueContainer : public ValueContainerBase
    {
    public:
        Integral32ValueContainer(int32_t val)
            : val_(val)
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
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
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
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
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_double(stmt, col_idx, val_);
        }

    private:
        double val_;
    };

    class StringValueContainer : public ValueContainerBase
    {
    public:
        StringValueContainer(const std::string & val)
            : val_(val)
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_text(stmt, col_idx, val_.c_str(), -1, 0);
        }

    private:
        std::string val_;
    };

    class BlobValueContainer : public ValueContainerBase
    {
    public:
        BlobValueContainer(const Blob & val)
            : val_(val)
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
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
        VectorValueContainer(const std::vector<T> & val)
            : val_(val)
        {}

        int32_t bind(sqlite3_stmt * stmt, int32_t col_idx) const override
        {
            return sqlite3_bind_blob(stmt, col_idx, val_.data(), (int)val_.size() * sizeof(T), 0);
        }

    private:
        std::vector<T> val_;
    };

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) <= sizeof(int32_t), ValueContainerBase *>::type
    createValueContainer_(T val)
    {
        return new Integral32ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(int64_t), ValueContainerBase *>::type
    createValueContainer_(T val)
    {
        return new Integral64ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_floating_point<T>::value, ValueContainerBase *>::type
    createValueContainer_(T val)
    {
        return new FloatingPointValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<typename std::decay<T>::type, const char*>::value, ValueContainerBase *>::type
    createValueContainer_(T val)
    {
        return new StringValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<T, std::string>::value, ValueContainerBase *>::type
    createValueContainer_(const T & val)
    {
        return new StringValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_same<T, Blob>::value, ValueContainerBase *>::type
    createValueContainer_(const T & val)
    {
        return new BlobValueContainer(val);
    }

    template <typename T>
    ValueContainerBase * createValueContainer_(const std::vector<T> & val)
    {
        return new VectorValueContainer<T>(val);
    }

    std::list<std::unique_ptr<ValueContainerBase>> col_vals_;
};

//! Helper class that wraps one table record.
class SqlRecord
{
public:
    SqlRecord(const std::string & table_name, const int32_t db_id, sqlite3 * db_conn)
        : table_name_(table_name)
        , db_id_(db_id)
        , db_conn_(db_conn)
    {}

    int32_t getId() const
    {
        return db_id_;
    }

    int32_t     getPropertyInt32 (const char * col_name) const;
    int64_t     getPropertyInt64 (const char * col_name) const;
    uint32_t    getPropertyUInt32(const char * col_name) const;
    uint64_t    getPropertyUInt64(const char * col_name) const;
    double      getPropertyDouble(const char * col_name) const;
    std::string getPropertyString(const char * col_name) const;

    template <typename T>
    std::vector<T> getPropertyBlob(const char * col_name) const;

private:
    sqlite3_stmt * createGetPropertyStmt_(const char * col_name) const
    {
        std::string cmd = "SELECT ";
        cmd += col_name;
        cmd += " FROM " + table_name_;
        cmd += " WHERE Id=" + std::to_string(db_id_);

        sqlite3_stmt * stmt = nullptr;
        auto rc = sqlite3_prepare_v2(db_conn_, cmd.c_str(), -1, &stmt, 0);
        if (rc) {
            throw DBException("Malformed SQL command: ") << cmd;
        }

        return stmt;
    }

    void stepGetPropertyStmt_(sqlite3_stmt * stmt, const bool expect_done) const
    {
        auto rc = sqlite3_step(stmt);
        if (!expect_done && (rc != SQLITE_ROW || rc == SQLITE_DONE)) {
            throw DBException("Could not get record property value:\n")
                << "\tIssue: Nothing returned from database\n"
                << "\tError code: " << rc;
        } else if (expect_done && rc != SQLITE_DONE) {
            throw DBException("Could not get record property value:\n")
                << "\tIssue: Expected only one record\n"
                << "\tError code: " << rc;
        }
    }

    const std::string table_name_;
    const int32_t db_id_;
    sqlite3 *const db_conn_;
};

inline int32_t SqlRecord::getPropertyInt32(const char * col_name) const
{
    sqlite3_stmt * stmt = createGetPropertyStmt_(col_name);
    stepGetPropertyStmt_(stmt, false);
    auto val = sqlite3_column_int(stmt, 0);
    stepGetPropertyStmt_(stmt, true);
    sqlite3_finalize(stmt);
    return val;
}

inline int64_t SqlRecord::getPropertyInt64(const char * col_name) const
{
    sqlite3_stmt * stmt = createGetPropertyStmt_(col_name);
    stepGetPropertyStmt_(stmt, false);
    auto val = sqlite3_column_int64(stmt, 0);
    stepGetPropertyStmt_(stmt, true);
    sqlite3_finalize(stmt);
    return val;
}

inline uint32_t SqlRecord::getPropertyUInt32(const char * col_name) const
{
    auto val = getPropertyInt32(col_name);
    return static_cast<uint32_t>(val);
}

inline uint64_t SqlRecord::getPropertyUInt64(const char * col_name) const
{
    auto val = getPropertyInt64(col_name);
    return static_cast<uint64_t>(val);
}

inline double SqlRecord::getPropertyDouble(const char * col_name) const
{
    sqlite3_stmt * stmt = createGetPropertyStmt_(col_name);
    stepGetPropertyStmt_(stmt, false);
    auto val = sqlite3_column_double(stmt, 0);
    stepGetPropertyStmt_(stmt, true);
    sqlite3_finalize(stmt);
    return val;
}

inline std::string SqlRecord::getPropertyString(const char * col_name) const
{
    sqlite3_stmt * stmt = createGetPropertyStmt_(col_name);
    stepGetPropertyStmt_(stmt, false);
    auto val = sqlite3_column_text(stmt, 0);
    auto ret = val ? std::string((const char*)(val)) : std::string("");
    stepGetPropertyStmt_(stmt, true);
    sqlite3_finalize(stmt);
    return ret;
}

template <typename T>
inline std::vector<T> SqlRecord::getPropertyBlob(const char * col_name) const
{
    sqlite3_stmt * stmt = createGetPropertyStmt_(col_name);
    stepGetPropertyStmt_(stmt, false);
    auto num_bytes = sqlite3_column_bytes(stmt, 0);
    auto data_ptr = sqlite3_column_blob(stmt, 0);

    static_assert(std::is_arithmetic<T>::value && !std::is_same<T, bool>::value,
                  "SimDB blobs can only be retrieved with vectors of integral types.");

    std::vector<T> vals(num_bytes / sizeof(T));
    memcpy(vals.data(), data_ptr, num_bytes);
    stepGetPropertyStmt_(stmt, true);
    sqlite3_finalize(stmt);
    return vals;
}

} // namespace simdb

#define SQL_TABLE   (name) simdb::SqlTable(name)
#define SQL_COLUMNS (...)  simdb::SqlColumns(__VA_ARGS__)
#define SQL_VALUES  (...)  simdb::SqlValues(__VA_ARGS__)
