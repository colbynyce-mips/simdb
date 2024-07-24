// <SQLiteTable.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/SQLiteConnection.hpp"
#include "simdb/schema/Schema.hpp"
#include "simdb_fwd.hpp"

namespace simdb {

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

class SqlValues
{
public:
    template <typename T, typename... Rest>
    SqlValues(T val, Rest... rest)
        : SqlValues(std::forward<Rest>(rest)...)
    {
        col_vals_.emplace_front(createValueContainer<T>(val));
    }

    template <typename T>
    SqlValues(T val)
    {
        col_vals_.emplace_front(createValueContainer<T>(val));
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
        int idx = 1;
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
        virtual int bind(sqlite3_stmt * stmt, int col_idx) const = 0;
    };

    class Integral32ValueContainer : public ValueContainerBase
    {
    public:
        Integral32ValueContainer(int32_t val)
            : val_(val)
        {}

        int bind(sqlite3_stmt * stmt, int col_idx) const override
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

        int bind(sqlite3_stmt * stmt, int col_idx) const override
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

        int bind(sqlite3_stmt * stmt, int col_idx) const override
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

        int bind(sqlite3_stmt * stmt, int col_idx) const override
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

        int bind(sqlite3_stmt * stmt, int col_idx) const override
        {
            return sqlite3_bind_blob(stmt, col_idx, val_.data_ptr, (int)val_.num_bytes, 0);
        }

    private:
        Blob val_;
    };

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) <= sizeof(int32_t), ValueContainerBase *>::type
    createValueContainer(T val)
    {
        return new Integral32ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(int64_t), ValueContainerBase *>::type
    createValueContainer(T val)
    {
        return new Integral32ValueContainer(val);
    }

    template <typename T>
    typename std::enable_if<std::is_floating_point<T>::value, ValueContainerBase *>::type
    createValueContainer(T val)
    {
        return new FloatingPointValueContainer(val);
    }

    ValueContainerBase *createValueContainer(const char *val)
    {
        return new StringValueContainer(val);
    }

    ValueContainerBase *createValueContainer(const std::string & val)
    {
        return new StringValueContainer(val);
    }

    ValueContainerBase *createValueContainer(const Blob & val)
    {
        return new BlobValueContainer(val);
    }

    std::list<std::unique_ptr<ValueContainerBase>> col_vals_;
};

} // namespace simdb

#define SQL_TABLE(name)  simdb::SqlTable(name)
#define SQL_COLUMNS(...) simdb::SqlColumns(__VA_ARGS__)
#define SQL_VALUES(...)  simdb::SqlValues(__VA_ARGS__)

/*
    db_mgr.INSERT(SqlTable("MyTable"), SqlColumns("ColA", "ColB"), SqlValues(3.14, "string"));
    db_mgr.INSERT(SqlTable("MyTable"), SqlValues(3.14, "string", 4.56));
    db_mgr.INSERT(SqlTable("MyTable"));
*/
