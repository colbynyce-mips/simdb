// <CollectionPoints> -*- C++ -*-

#pragma once

#include "simdb/serialize/Serialize.hpp"

#include <stdint.h>
#include <memory>
#include <vector>
#include <cassert>
#include <string>

namespace simdb
{

struct ArgosRecord
{
    enum class Status { READ, READ_ONCE, DONT_READ };
    Status status = Status::DONT_READ;

    const uint16_t elem_id = 0;
    std::vector<char> data;

    ArgosRecord(uint16_t elem_id) : elem_id(elem_id) {}

    void reset()
    {
        status = Status::DONT_READ;
        data.clear();
    }
};

class CollectionPointBase
{
public:
    CollectionPointBase(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype)
        : argos_record_(elem_id)
        , elem_id_(elem_id)
        , clk_id_(clk_id)
        , heartbeat_(heartbeat)
        , dtype_(dtype)
    {}

    uint16_t getElemId() const { return elem_id_; }

    uint16_t getClockId() const { return clk_id_; }

    size_t getHeartbeat() const { return heartbeat_; }

    const std::string& getDataTypeStr() const { return dtype_; }

    void sweep(std::vector<char>& swept_data)
    {
        if (argos_record_.status != ArgosRecord::Status::DONT_READ) {
            swept_data.insert(swept_data.end(), argos_record_.data.begin(), argos_record_.data.end());
        }

        if (argos_record_.status == ArgosRecord::Status::READ_ONCE) {
            argos_record_.reset();
        }
    }

protected:
    ArgosRecord argos_record_;

private:
    const uint16_t elem_id_;
    const uint16_t clk_id_;
    const size_t heartbeat_;
    const std::string dtype_;
};

template <typename T>
struct is_std_vector : std::false_type {};

template <typename T>
struct is_std_vector<std::vector<T>> : std::true_type {};

template <typename T>
static constexpr bool is_std_vector_v = is_std_vector<T>::value;

class CollectionPoint : public CollectionPointBase
{
public:
    template <typename... Args>
    CollectionPoint(Args&&... args) : CollectionPointBase(std::forward<Args>(args)...) {}

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T& val, bool once = false)
    {
        if (val) {
            activate(*val, once);
        } else {
            deactivate();
        }
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T& val, bool once = false)
    {
        activateImpl_(val);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    template <typename T>
    typename std::enable_if<std::is_trivial<T>::value && std::is_standard_layout<T>::value, void>::type
    activateImpl_(const T val)
    {
        // Note that we do not do the carry-over optimization for POD types, as
        // it will not really result in any compression. The benefit starts with
        // types like structures or queues/vectors.
        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        buffer.write(val);
    }

    template <typename T>
    typename std::enable_if<!std::is_trivial<T>::value || !std::is_standard_layout<T>::value, void>::type
    activateImpl_(const T& val)
    {
        static std::unique_ptr<StructBlobSerializer> struct_serializer;
        if (!struct_serializer) {
            static StructDefnSerializer<T> defn_serializer;
            struct_serializer = defn_serializer.createBlobSerializer();
        }

        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        struct_serializer->writeStruct(&val, buffer);

        if (num_carry_overs_ < getHeartbeat() && argos_record_.data == prev_data_) {
            buffer.reset();
            buffer.write(getElemId());
            buffer.write(UINT16_MAX);
            ++num_carry_overs_;
        } else {
            prev_data_ = argos_record_.data;
            num_carry_overs_ = 0;
        }
    }

    std::vector<char> prev_data_;
    size_t num_carry_overs_ = 0;
};

class ContigIterableCollectionPoint : public CollectionPointBase
{
public:
    ContigIterableCollectionPoint(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype, size_t capacity)
        : CollectionPointBase(elem_id, clk_id, heartbeat, dtype)
        , expected_capacity_(capacity)
    {
        prev_data_by_bin_.resize(capacity);
        num_carry_overs_by_bin_.resize(capacity, 0);
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T container)
    {
        activate(*container);
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T& container)
    {
        readContainer_(container);
        argos_record_.status = ArgosRecord::Status::READ;
    }

private:
    template <typename T>
    void readContainer_(const T& container)
    {
        auto size = container.size();
        if (size > expected_capacity_) {
            size = expected_capacity_;
        }

        CollectionBuffer buffer(argos_record_.data);
        buffer.writeHeader(getElemId(), size);

        auto itr = container.begin();
        auto eitr = container.end();
        uint16_t bin_idx = 0;

        while (itr != eitr && bin_idx < size) {
            if (!writeStruct_(*itr, buffer, bin_idx)) {
                break;
            }
            ++itr;
            ++bin_idx;
        }
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        if (el) {
            return writeStruct_(*el, buffer, bin_idx);
        }
        return false;
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        static std::unique_ptr<StructBlobSerializer> struct_serializer;
        if (!struct_serializer) {
            static StructDefnSerializer<T> defn_serializer;
            struct_serializer = defn_serializer.createBlobSerializer();
        }

        buffer.writeBucket(bin_idx);

        CollectionBuffer buffer2(struct_bytes_);
        struct_serializer->writeStruct(&el, buffer2);

        if (num_carry_overs_by_bin_[bin_idx] < getHeartbeat() && struct_bytes_ == prev_data_by_bin_[bin_idx]) {
            buffer.write(UINT16_MAX);
            ++num_carry_overs_by_bin_[bin_idx];
        } else {
            buffer.write(struct_bytes_);
            prev_data_by_bin_[bin_idx] = struct_bytes_;
        }

        return true;
    }

    const size_t expected_capacity_;
    std::vector<char> struct_bytes_;
    std::vector<std::vector<char>> prev_data_by_bin_;
    std::vector<size_t> num_carry_overs_by_bin_;
};

class SparseIterableCollectionPoint : public CollectionPointBase
{
public:
    SparseIterableCollectionPoint(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype, size_t capacity)
        : CollectionPointBase(elem_id, clk_id, heartbeat, dtype)
        , expected_capacity_(capacity)
    {
        prev_data_by_bin_.resize(capacity);
        num_carry_overs_by_bin_.resize(capacity, 0);
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T container)
    {
        activate(*container);
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T& container)
    {
        readContainer_(container);
        argos_record_.status = ArgosRecord::Status::READ;
    }

private:
    template <typename T>
    void readContainer_(const T& container)
    {
        uint16_t num_valid = 0;

        {
            auto itr = container.begin();
            auto eitr = container.end();

            while (itr != eitr) {
                if constexpr (is_std_vector_v<T>) {
                    if (*itr) {
                        ++num_valid;
                    }
                } else if (itr.isValid()) {
                    ++num_valid;
                }
                ++itr;
            }
        }

        CollectionBuffer buffer(argos_record_.data);
        buffer.writeHeader(getElemId(), num_valid);

        uint16_t bin_idx = 0;
        auto itr = container.begin();
        auto eitr = container.end();
        while (itr != eitr && bin_idx < expected_capacity_) {
            bool valid;
            if constexpr (is_std_vector_v<T>) {
                valid = *itr != nullptr;
            } else {
                valid = itr.isValid();
            }

            if (valid) {
                writeStruct_(*itr, buffer, bin_idx);
            }
            ++itr;
            ++bin_idx;
        }
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        if (el) {
            return writeStruct_(*el, buffer, bin_idx);
        }
        return false;
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        static std::unique_ptr<StructBlobSerializer> struct_serializer;
        if (!struct_serializer) {
            static StructDefnSerializer<T> defn_serializer;
            struct_serializer = defn_serializer.createBlobSerializer();
        }

        buffer.writeBucket(bin_idx);

        CollectionBuffer buffer2(struct_bytes_);
        struct_serializer->writeStruct(&el, buffer2);

        if (num_carry_overs_by_bin_[bin_idx] < getHeartbeat() && struct_bytes_ == prev_data_by_bin_[bin_idx]) {
            buffer.write(UINT16_MAX);
            ++num_carry_overs_by_bin_[bin_idx];
        } else {
            buffer.write(struct_bytes_);
            prev_data_by_bin_[bin_idx] = struct_bytes_;
        }

        return true;
    }

    const size_t expected_capacity_;
    std::vector<char> struct_bytes_;
    std::vector<std::vector<char>> prev_data_by_bin_;
    std::vector<size_t> num_carry_overs_by_bin_;
};

} // namespace simdb
