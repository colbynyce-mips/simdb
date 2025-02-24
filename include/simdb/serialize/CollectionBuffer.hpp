// <CollectionBuffer> -*- C++ -*-

#pragma once

#include "simdb/utils/MetaStructs.hpp"

#include <stdint.h>
#include <cstring>
#include <iostream>
#include <vector>

namespace simdb {

/*!
 * \class CollectionBuffer
 *
 * \brief A helper class to allow collections to write their data
 *        to a single buffer before sending it to the background
 *        thread for database insertion. We pack everything into
 *        one buffer to minimize the number of entries we have in
 *        the database, and to get maximum compression.
 */
class CollectionBuffer {
public:
    CollectionBuffer(std::vector<char>& buffer)
        : buffer_(buffer) {
        buffer_.clear();
        buffer_.reserve(buffer_.capacity());
    }

    void append(const void* data, size_t num_bytes) {
        const char* bytes = static_cast<const char*>(data);
        buffer_.insert(buffer_.end(), bytes, bytes + num_bytes);
    }

private:
    std::vector<char>& buffer_;
};

template <typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value && std::is_scalar<T>::value && !meta_utils::is_any_pointer<T>::value,
                               CollectionBuffer&>::type
operator<<(CollectionBuffer& buffer, const T& val) {
    if constexpr (std::is_same<T, bool>::value) {
        buffer << static_cast<int>(val);
    } else {
        buffer.append(&val, sizeof(T));
    }

    return buffer;
}

template <typename T>
inline typename std::enable_if<std::is_enum<T>::value, CollectionBuffer&>::type operator<<(CollectionBuffer& buffer, const T& val) {
    using dtype = typename std::underlying_type<T>::type;
    return buffer << static_cast<dtype>(val);
}

inline CollectionBuffer& operator<<(CollectionBuffer& buffer, const std::vector<char>& bytes) {
    buffer.append(bytes.data(), bytes.size());
    return buffer;
}

// Go through StringMap to serialize as uint32_t
inline CollectionBuffer& operator<<(CollectionBuffer& buffer, const std::string& val) = delete;
inline CollectionBuffer& operator<<(CollectionBuffer& buffer, const char* val) = delete;

} // namespace simdb
