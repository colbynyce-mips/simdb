// <ValueContainer> -*- C++ -*-

#pragma once

#include "simdb/schema/ColumnTypedefs.hpp"

#include <functional>
#include <memory>
#include <sqlite3.h>
#include <vector>

namespace simdb
{

/*!
 * \class ValueContainerBase
 *
 * \brief This class is used for flexible varargs to SQL_VALUES(v1,v2,v3)
 *        where the types of v1/v2/v3 can all be different (int/double/blob...)
 */
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

using ValueContainerBasePtr = std::shared_ptr<ValueContainerBase>;

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

} // namespace simdb
