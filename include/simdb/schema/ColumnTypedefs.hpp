// <ColumnTypedefs> -*- C++ -*-

#pragma once

#include <cstdint>
#include <cstddef>
#include <iostream>
#include <utility>
#include <string>
#include <vector>

namespace simdb {

//! Data types supported by SimDB schemas
enum class ColumnDataType : int8_t {
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    double_t,
    string_t,
    blob_t,
    fkey_t
};

//! Stream operator used when creating a SQL command from an ostringstream.
inline std::ostream & operator<<(std::ostream & os, const ColumnDataType dtype)
{
    using dt = ColumnDataType;

    switch (dtype) {
        case dt::fkey_t:
        case dt::int32_t:
        case dt::uint32_t:
        case dt::int64_t:
        case dt::uint64_t: {
            os << "INT"; break;
        }

        case dt::string_t: {
            os << "TEXT"; break;
        }

        case dt::double_t: {
            os << "FLOAT"; break;
        }

        case dt::blob_t: {
            os << "BLOB"; break;
        }
    }

    return os;
}

//! From a table's perspective, each column can be uniquely
//! described by its column name and its data type.
using ColumnDescriptor = std::pair<std::string, ColumnDataType>;

//! Blob descriptor used for writing and reading raw bytes
//! to/from the database.
struct Blob {
    const void * data_ptr = nullptr;
    size_t num_bytes = 0;

    template <typename T>
    Blob(const std::vector<T> & vals)
        : data_ptr(vals.data())
        , num_bytes(vals.size() * sizeof(T))
    {}

    Blob() = default;
};

}

