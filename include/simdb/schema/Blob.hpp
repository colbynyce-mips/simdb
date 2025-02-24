// <ValueContainer> -*- C++ -*-

#pragma once

#include <cstddef>
#include <vector>

namespace simdb {

/// Blob descriptor used for writing and reading raw bytes
/// to/from the database.
struct SqlBlob {
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;

    template <typename T>
    SqlBlob(const std::vector<T>& vals)
        : data_ptr(vals.data())
        , num_bytes(vals.size() * sizeof(T)) {
    }

    SqlBlob() = default;
};

} // namespace simdb
