#pragma once

#include "simdb/Errors.hpp"

#include <memory>
#include <sqlite3.h>
#include <string.h>
#include <string>
#include <vector>

namespace simdb
{

//! Base class for all SELECT objects that are responsible for
//! writing record values to the user's local variables whenever
//! this iterator is advanced.
class ResultWriterBase
{
public:
    virtual ~ResultWriterBase() = default;
    virtual void writeToUserVar(sqlite3_stmt* stmt, const int idx) const = 0;
    virtual ResultWriterBase* clone() const = 0;

    const std::string& getColName() const
    {
        return col_name_;
    }

protected:
    ResultWriterBase(const char* col_name)
        : col_name_(col_name)
    {
    }

private:
    std::string col_name_;
};

//! Write int32 values to the local variable pointer given to us.
class ResultWriterInt32 : public ResultWriterBase
{
public:
    ResultWriterInt32(const char* col_name, int32_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterInt32(getColName().c_str(), user_var_);
    }

private:
    int32_t* user_var_;
};

//! Write int64 values to the local variable pointer given to us.
class ResultWriterInt64 : public ResultWriterBase
{
public:
    ResultWriterInt64(const char* col_name, int64_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int64(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterInt64(getColName().c_str(), user_var_);
    }

private:
    int64_t* user_var_;
};

//! Write uint32 values to the local variable pointer given to us.
class ResultWriterUInt32 : public ResultWriterBase
{
public:
    ResultWriterUInt32(const char* col_name, uint32_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterUInt32(getColName().c_str(), user_var_);
    }

private:
    uint32_t* user_var_;
};

//! Write uint64 values to the local variable pointer given to us.
class ResultWriterUInt64 : public ResultWriterBase
{
public:
    ResultWriterUInt64(const char* col_name, uint64_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int64(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterUInt64(getColName().c_str(), user_var_);
    }

private:
    uint64_t* user_var_;
};

//! Write double values to the local variable pointer given to us.
class ResultWriterDouble : public ResultWriterBase
{
public:
    ResultWriterDouble(const char* col_name, double* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_double(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterDouble(getColName().c_str(), user_var_);
    }

private:
    double* user_var_;
};

//! Write string values to the local variable pointer given to us.
class ResultWriterString : public ResultWriterBase
{
public:
    ResultWriterString(const char* col_name, std::string* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = (const char*)sqlite3_column_text(stmt, idx);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterString(getColName().c_str(), user_var_);
    }

private:
    std::string* user_var_;
};

//! Write blob values to the local variable pointer given to us.
template <typename T>
class ResultWriterBlob : public ResultWriterBase
{
public:
    ResultWriterBlob(const char* col_name, std::vector<T>* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        const void* data = sqlite3_column_blob(stmt, idx);
        const int bytes = sqlite3_column_bytes(stmt, idx);
        user_var_->resize(bytes / sizeof(T));
        memcpy(user_var_->data(), data, bytes);
    }

    ResultWriterBase* clone() const override
    {
        return new ResultWriterBlob(getColName().c_str(), user_var_);
    }

private:
    std::vector<T>* user_var_;
};

//! This class is returned by SqlQuery::getResultSet() and is used to iterate
//! over a query result set.
class SqlResultIterator
{
public:
    SqlResultIterator(sqlite3_stmt* stmt, std::vector<std::shared_ptr<ResultWriterBase>>&& result_writers)
        : stmt_(stmt)
        , result_writers_(std::move(result_writers))
    {
    }

    //! Finalize the prepared statement on destruction.
    ~SqlResultIterator()
    {
        sqlite3_finalize(stmt_);
    }

    //! Get the next record, populate the user's local variables,
    //! and return TRUE if the record was found. FALSE is returned
    //! when the entire result set has been iterated over.
    bool getNextRecord()
    {
        auto rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) {
            for (size_t idx = 0; idx < result_writers_.size(); ++idx) {
                result_writers_[idx]->writeToUserVar(stmt_, (int)idx);
            }
            return true;
        } else if (rc != SQLITE_DONE) {
            throw DBException(sqlite3_errmsg(sqlite3_db_handle(stmt_)));
        }

        return false;
    }

    //! Go back to the beginning of the result set if you need
    //! to iterate over it again.
    void reset()
    {
        if (sqlite3_reset(stmt_)) {
            throw DBException(sqlite3_errmsg(sqlite3_db_handle(stmt_)));
        }
    }

private:
    sqlite3_stmt* const stmt_;
    std::vector<std::shared_ptr<ResultWriterBase>> result_writers_;
};

} // namespace simdb
