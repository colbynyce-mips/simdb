// <ColumnTypedefs> -*- C++ -*-

#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace simdb
{

/// Data types supported by SimDB schemas
enum class ColumnDataType {
    int32_t,
    int64_t,
    double_t,
    string_t,
    blob_t
};

/// Stream operator used when creating various SQL commands.
inline std::ostream& operator<<(std::ostream& os, const ColumnDataType dtype)
{
    using dt = ColumnDataType;

    switch (dtype) {
        case dt::int32_t:
        case dt::int64_t: {
            os << "INT";
            break;
        }

        case dt::string_t: {
            os << "TEXT";
            break;
        }

        case dt::double_t: {
            os << "REAL";
            break;
        }

        case dt::blob_t: {
            os << "BLOB";
            break;
        }
    }

    return os;
}

/// Blob descriptor used for writing and reading raw bytes
/// to/from the database.
struct Blob {
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;

    template <typename T>
    Blob(const std::vector<T>& vals)
        : data_ptr(vals.data())
        , num_bytes(vals.size() * sizeof(T))
    {
    }

    Blob() = default;
};

} // namespace simdb
