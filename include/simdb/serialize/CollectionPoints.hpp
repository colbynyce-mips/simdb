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
    enum class Action : uint8_t { WRITE, CARRY };

    template <typename... Args>
    CollectionPoint(Args&&... args) : CollectionPointBase(std::forward<Args>(args)...) {}

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type
    activate(const T& val, bool once = false)
    {
        ensureNumBytes_(val);
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
        ensureNumBytes_(val);
        activateImpl_(val);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    template <typename T>
    void activateImpl_(const T& val)
    {
        CollectionBuffer buffer(argos_record_.data);
        buffer << getElemId();

        assert(num_bytes_ > 0);
        if constexpr (std::is_trivial<T>::value && std::is_standard_layout<T>::value) {
            if (num_bytes_ < 16) {
                // Collectables that are this small are written directly,
                // as any attempts at a DB size optimization would actually
                // make the database larger.
                buffer << val;
            }
            return;
        }

        static std::unique_ptr<StructBlobSerializer> struct_serializer;
        if (!struct_serializer) {
            static StructDefnSerializer<T> defn_serializer;
            struct_serializer = defn_serializer.createBlobSerializer();
        }

        struct_serializer->extract(&val, curr_data_);
        if (num_carry_overs_ < getHeartbeat() && curr_data_ == prev_data_) {
            buffer << CollectionPoint::Action::CARRY;
            ++num_carry_overs_;
        } else {
            buffer << CollectionPoint::Action::WRITE;
            buffer << curr_data_;
            prev_data_ = curr_data_;
            num_carry_overs_ = 0;
        }
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type
    ensureNumBytes_(const T& val)
    {
        if (num_bytes_) {
            return;
        }

        if (val) {
            ensureNumBytes_(*val);
        }

        if (num_bytes_ == 0) {
            throw DBException("Could not determine number of bytes for data type");
        }
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type
    ensureNumBytes_(const T& val)
    {
        if (num_bytes_) {
            return;
        }

        if constexpr (std::is_same_v<T, bool>) {
            num_bytes_ = sizeof(int);
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            num_bytes_ = sizeof(uint8_t);
        } else if constexpr (std::is_same_v<T, int8_t>) {
            num_bytes_ = sizeof(int8_t);
        } else if constexpr (std::is_same_v<T, uint16_t>) {
            num_bytes_ = sizeof(uint16_t);
        } else if constexpr (std::is_same_v<T, int16_t>) {
            num_bytes_ = sizeof(int16_t);
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            num_bytes_ = sizeof(uint32_t);
        } else if constexpr (std::is_same_v<T, int32_t>) {
            num_bytes_ = sizeof(int32_t);
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            num_bytes_ = sizeof(uint64_t);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            num_bytes_ = sizeof(int64_t);
        } else if constexpr (std::is_same_v<T, float>) {
            num_bytes_ = sizeof(float);
        } else if constexpr (std::is_same_v<T, double>) {
            num_bytes_ = sizeof(double);
        } else if constexpr (std::is_same_v<T, std::string>) {
            num_bytes_ = sizeof(uint32_t);
        } else if constexpr (std::is_enum<T>::value) {
            num_bytes_ = sizeof(typename std::underlying_type<T>::type);
        } else {
            static std::unique_ptr<StructDefnSerializer<T>> defn_serializer;
            if (!defn_serializer) {
                defn_serializer = std::make_unique<StructDefnSerializer<T>>();
            }

            num_bytes_ = defn_serializer->getStructNumBytes();
        }

        if (num_bytes_ == 0) {
            throw DBException("Could not determine number of bytes for data type");
        }
    }

    std::vector<char> curr_data_;
    std::vector<char> prev_data_;
    size_t num_carry_overs_ = 0;
    size_t num_bytes_ = 0;
};

class ContigIterableCollectionPoint : public CollectionPointBase
{
public:
    enum class Action : uint8_t { ARRIVE, DEPART, BOOKENDS, CHANGE, FULL, CARRY };

    ContigIterableCollectionPoint(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype, size_t capacity)
        : CollectionPointBase(elem_id, clk_id, heartbeat, dtype)
        , curr_snapshot_(capacity)
        , prev_snapshot_(capacity)
    {
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
    class IterableSnapshot
    {
    public:
        IterableSnapshot(size_t expected_capacity)
            : bytes_by_bin_(expected_capacity)
        {}

        uint16_t size() const
        {
            uint16_t num_valid = 0;
            for (const auto& bin : bytes_by_bin_) {
                if (!bin.empty()) {
                    ++num_valid;
                }
            }
            return num_valid;
        }

        uint16_t capacity() const
        {
            return bytes_by_bin_.size();
        }

        std::vector<char>& operator[](size_t idx)
        {
            return bytes_by_bin_[idx];
        }

        const std::vector<char>& operator[](size_t idx) const
        {
            return bytes_by_bin_[idx];
        }

        void clear()
        {
            for (auto& bin : bytes_by_bin_) {
                bin.clear();
            }
        }

        void compareAndSerialize(IterableSnapshot& prev, CollectionBuffer& buffer)
        {
            uint16_t changed_idx = 0;
            switch (getAction_(prev, changed_idx)) {
                case Action::CARRY:
                    buffer << ContigIterableCollectionPoint::Action::CARRY;
                    break;
                case Action::ARRIVE:
                    buffer << ContigIterableCollectionPoint::Action::ARRIVE;
                    buffer << bytes_by_bin_.back();
                    break;
                case Action::DEPART:
                    buffer << ContigIterableCollectionPoint::Action::DEPART;
                    break;
                case Action::CHANGE:
                    buffer << ContigIterableCollectionPoint::Action::CHANGE;
                    buffer << changed_idx;
                    buffer << bytes_by_bin_[changed_idx];
                    break;
                case Action::BOOKENDS:
                    buffer << ContigIterableCollectionPoint::Action::BOOKENDS;
                    buffer << bytes_by_bin_.back();
                    break;
                case Action::FULL:
                    buffer << ContigIterableCollectionPoint::Action::FULL;
                    buffer << size();
                    for (uint16_t idx = 0; idx < size(); ++idx) {
                        buffer << idx << bytes_by_bin_[idx];
                    }
                    break;
            }
        }

    private:
        Action getAction_(IterableSnapshot& prev, uint16_t& changed_idx)
        {
            if (++action_count_ == capacity()) {
                action_count_ = 0;
                return Action::FULL;
            }

            if (prev.bytes_by_bin_ == bytes_by_bin_) {
                if (bytes_by_bin_.empty()) {
                    return Action::FULL;
                }
                return Action::CARRY;
            }

            auto prev_size = prev.size();
            auto curr_size = size();
            if (prev_size == curr_size) {
                // If the (ith+1) of the prev container == the ith of the current container, then return BOOKEND
                bool bookends = true;
                for (size_t idx = 1; idx < prev_size; ++idx) {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx - 1]) {
                        bookends = false;
                        break;
                    }
                }
                if (bookends) {
                    return Action::BOOKENDS;
                }

                // If exactly one element has changed, then return CHANGE
                changed_idx = 0;
                for (size_t idx = 0; idx < curr_size; ++idx) {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx]) {
                        if (changed_idx) {
                            // If there is more than one change, then return FULL
                            return Action::FULL;
                        }
                        changed_idx = idx;
                        return Action::CHANGE;
                    }
                }

                // Not reachable
                assert(false);
            } else if (prev_size + 1 == curr_size) {
                bool arrive = true;
                for (size_t idx = 0; idx < prev_size; ++idx) {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx]) {
                        arrive = false;
                        break;
                    }
                }
                if (arrive) {
                    return Action::ARRIVE;
                }
            } else if (prev_size - 1 == curr_size) {
                bool depart = true;
                for (size_t idx = 0; idx < curr_size; ++idx) {
                    if (prev.bytes_by_bin_[idx + 1] != bytes_by_bin_[idx]) {
                        depart = false;
                        break;
                    }
                }
                if (depart) {
                    return Action::DEPART;
                }
            }

            return Action::FULL;
        }

        std::deque<std::vector<char>> bytes_by_bin_;
        size_t action_count_ = 0;
    };

    template <typename T>
    void readContainer_(const T& container)
    {
        auto size = container.size();
        if (size > prev_snapshot_.capacity()) {
            size = prev_snapshot_.capacity();
        }

        curr_snapshot_.clear();

        auto itr = container.begin();
        auto eitr = container.end();
        uint16_t bin_idx = 0;

        while (itr != eitr && bin_idx < size) {
            if (!writeStruct_(*itr, curr_snapshot_, bin_idx)) {
                break;
            }
            ++itr;
            ++bin_idx;
        }

        // Let the current snapshot take into account the previous
        // snapshot, and figure out the most compact way to represent
        // the current data.
        //
        // The only thing we must do for all collection points is to
        // write the element ID.
        CollectionBuffer buffer(argos_record_.data);
        buffer << getElemId();
        curr_snapshot_.compareAndSerialize(prev_snapshot_, buffer);
        prev_snapshot_ = curr_snapshot_;
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, IterableSnapshot& snapshot, uint16_t bin_idx)
    {
        if (el) {
            return writeStruct_(*el, snapshot, bin_idx);
        }
        return false;
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, IterableSnapshot& snapshot, uint16_t bin_idx)
    {
        static std::unique_ptr<StructBlobSerializer> struct_serializer;
        if (!struct_serializer) {
            static StructDefnSerializer<T> defn_serializer;
            struct_serializer = defn_serializer.createBlobSerializer();
        }

        struct_serializer->extract(&el, snapshot[bin_idx]);
        return true;
    }

    IterableSnapshot curr_snapshot_;
    IterableSnapshot prev_snapshot_;
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

        // TODO cnyce - optimize this
        CollectionBuffer buffer(argos_record_.data);
        buffer << getElemId() << num_valid;

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

        // TODO cnyce - optimize this
        buffer << bin_idx;
        struct_serializer->writeStruct(&el, buffer);
        return true;
    }

    const size_t expected_capacity_;
    std::vector<char> struct_bytes_;
    std::vector<std::vector<char>> prev_data_by_bin_;
    std::vector<size_t> num_carry_overs_by_bin_;
};

} // namespace simdb
