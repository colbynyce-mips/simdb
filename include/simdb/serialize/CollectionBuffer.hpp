// <CollectionBuffer> -*- C++ -*-

#pragma once

#include <vector>
#include <stdint.h>
#include <cstring>

namespace simdb
{

/*!
 * \class CollectionBuffer
 *
 * \brief A helper class to allow collections to write their data
 *        to a single buffer before sending it to the background
 *        thread for database insertion. We pack everything into
 *        one buffer to minimize the number of entries we have in
 *        the database, and to get maximum compression.
 */
class CollectionBuffer
{
public:
    CollectionBuffer(std::vector<char> &all_collection_data)
        : all_collection_data_(all_collection_data)
    {
        all_collection_data_.clear();
        all_collection_data_.reserve(all_collection_data_.capacity());
    }

    void writeHeader(uint16_t collection_id, uint16_t num_elems)
    {
        all_collection_data_.resize(all_collection_data_.size() + 2 * sizeof(uint16_t));
        auto dest = all_collection_data_.data() + all_collection_data_.size() - 2 * sizeof(uint16_t);
        memcpy(dest, &collection_id, sizeof(uint16_t));
        memcpy(dest + sizeof(uint16_t), &num_elems, sizeof(uint16_t));
    }

    void writeBucket(uint16_t bucket_id)
    {
        all_collection_data_.resize(all_collection_data_.size() + sizeof(uint16_t));
        auto dest = all_collection_data_.data() + all_collection_data_.size() - sizeof(uint16_t);
        memcpy(dest, &bucket_id, sizeof(uint16_t));
    }

    template <typename T>
    void writeBytes(const T* data, size_t num_bytes)
    {
        all_collection_data_.resize(all_collection_data_.size() + num_bytes);
        auto dest = all_collection_data_.data() + all_collection_data_.size() - num_bytes;
        memcpy(dest, data, num_bytes);
    }

private:
    std::vector<char> &all_collection_data_;
};

template <>
inline void CollectionBuffer::writeBytes<bool>(const bool* data, size_t num_bytes)
{
    for (size_t idx = 0; idx < num_bytes; ++idx) {
        int32_t val = data[idx] ? 1 : 0;
        writeBytes(&val, sizeof(int32_t));
    }
}

} // namespace simdb
