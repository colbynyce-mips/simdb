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
    CollectionPointBase(uint16_t elem_id, uint16_t clk_id, size_t heartbeat)
        : argos_record_(elem_id)
        , elem_id_(elem_id)
        , clk_id_(clk_id)
        , heartbeat_(heartbeat)
    {}

    virtual ~CollectionPointBase() = default;

    uint16_t getElemId() const { return elem_id_; }

    uint16_t getClockId() const { return clk_id_; }

    size_t getHeartbeat() const { return heartbeat_; }

    virtual std::string getDataTypeStr() const = 0;

    virtual void serializeDefn(DatabaseManager* db_mgr) = 0;

    virtual void autoCollect() {}

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
};

template <typename T>
class TrivialAutoCollectable : public CollectionPointBase
{
public:
    TrivialAutoCollectable(uint16_t elem_id, uint16_t clk_id, const T* data, size_t heartbeat)
        : CollectionPointBase(elem_id, clk_id, heartbeat)
        , data_(data)
    {
        static_assert(!meta_utils::is_any_pointer<T>::value, "Pointer-to-pointer types are not supported");
        assert(data_);

        // Auto-collectables are always in the "READ" state
        argos_record_.status = ArgosRecord::Status::READ;
    }

    std::string getDataTypeStr() const override
    {
        if constexpr (std::is_same<T, bool>::value) {
            return "bool";
        } else {
            auto dtype = getFieldDTypeEnum<T>();
            return getFieldDTypeStr(dtype);
        }
    }

    void serializeDefn(DatabaseManager*) override
    {
        // Trivial types don't need to serialize anything
    }

    void autoCollect() override
    {
        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        buffer.write(*data_);
    }

private:
    const T* data_;
};

template <typename T>
using TrivialAutoCollectablePtr = std::shared_ptr<TrivialAutoCollectable<T>>;

template <typename T>
class TrivialManualCollectable : public CollectionPointBase
{
public:
    TrivialManualCollectable(uint16_t elem_id, uint16_t clk_id, size_t heartbeat)
        : CollectionPointBase(elem_id, clk_id, heartbeat)
    {
    }

    std::string getDataTypeStr() const override
    {
        if constexpr (std::is_same<T, bool>::value) {
            return "bool";
        } else {
            auto dtype = getFieldDTypeEnum<T>();
            return getFieldDTypeStr(dtype);
        }
    }

    void serializeDefn(DatabaseManager*) override
    {
        // Trivial types don't need to serialize anything
    }

    template <typename tt = T>
    typename std::enable_if<meta_utils::is_any_pointer<tt>::value, void>::type
    activate(const tt& data, bool once = false)
    {
        if (data) {
            activate(*data, once);
        } else {
            deactivate();
        }
    }

    template <typename tt = T>
    typename std::enable_if<!meta_utils::is_any_pointer<tt>::value, void>::type
    activate(const tt& data, bool once = false)
    {
        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        buffer.write(data);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }
};

template <typename T>
using TrivialManualCollectablePtr = std::shared_ptr<TrivialManualCollectable<T>>;

template <typename T>
class StructAutoCollectable : public CollectionPointBase
{
public:
    using value_type = meta_utils::remove_any_pointer_t<T>;

    StructAutoCollectable(uint16_t elem_id, uint16_t clk_id, const T* data, size_t heartbeat)
        : CollectionPointBase(elem_id, clk_id, heartbeat)
        , data_(data)
    {
        struct_serializer_ = defn_serializer_.createBlobSerializer();
        static_assert(!meta_utils::is_any_pointer<value_type>::value, "Pointer-to-pointer types are not supported");
        assert(data_);

        // Auto-collectables are always in the "READ" state
        argos_record_.status = ArgosRecord::Status::READ;
    }

    std::string getDataTypeStr() const override
    {
        return defn_serializer_.getStructName();
    }

    void serializeDefn(DatabaseManager* db_mgr) override
    {
        defn_serializer_.serializeDefn(db_mgr);
    }

    void autoCollect() override
    {
        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        struct_serializer_->writeStruct(data_, buffer);
    }
    
private:
    StructDefnSerializer<value_type> defn_serializer_;
    std::unique_ptr<StructBlobSerializer> struct_serializer_;
    const T* data_;
};

template <typename T>
using StructAutoCollectablePtr = std::shared_ptr<StructAutoCollectable<T>>;

template <typename T>
class StructManualCollectable : public CollectionPointBase
{
public:
    using value_type = meta_utils::remove_any_pointer_t<T>;

    StructManualCollectable(uint16_t elem_id, uint16_t clk_id, size_t heartbeat)
        : CollectionPointBase(elem_id, clk_id, heartbeat)
    {
        struct_serializer_ = defn_serializer_.createBlobSerializer();
    }

    std::string getDataTypeStr() const override
    {
        return defn_serializer_.getStructName();
    }

    void serializeDefn(DatabaseManager* db_mgr) override
    {
        defn_serializer_.serializeDefn(db_mgr);
    }

    template <typename tt = T>
    typename std::enable_if<meta_utils::is_any_pointer<tt>::value, void>::type
    activate(const tt& data, bool once = false)
    {
        if (data) {
            activate(*data, once);
        } else {
            deactivate();
        }
    }

    template <typename tt = T>
    typename std::enable_if<!meta_utils::is_any_pointer<tt>::value, void>::type
    activate(const tt& data, bool once = false)
    {
        CollectionBuffer buffer(argos_record_.data);
        buffer.write(getElemId());
        struct_serializer_->writeStruct(&data, buffer);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    StructDefnSerializer<value_type> defn_serializer_;
    std::unique_ptr<StructBlobSerializer> struct_serializer_;
};

template <typename T>
using StructManualCollectablePtr = std::shared_ptr<StructManualCollectable<T>>;

template <typename T>
struct is_std_vector : std::false_type {};

template <typename T>
struct is_std_vector<std::vector<T>> : std::true_type {};

template <typename T>
static constexpr bool is_std_vector_v = is_std_vector<T>::value;

template <typename T, bool Sparse>
class IterableCollector : public CollectionPointBase
{
public:
    using value_type = meta_utils::remove_any_pointer_t<typename T::value_type>;

    IterableCollector(uint16_t elem_id, uint16_t clk_id, size_t capacity, const T* data, size_t heartbeat)
        : CollectionPointBase(elem_id, clk_id, heartbeat)
        , capacity_(capacity)
        , data_(data)
    {
        struct_serializer_ = defn_serializer_.createBlobSerializer();
        static_assert(!meta_utils::is_any_pointer<value_type>::value, "Pointer-to-pointer types are not supported");
        assert(data_);

        // Auto-collectables are always in the "READ" state
        argos_record_.status = ArgosRecord::Status::READ;
    }

    std::string getDataTypeStr() const override
    {
        std::string dtype;
        dtype += defn_serializer_.getStructName();

        if constexpr (Sparse) {
            dtype += "_sparse";
        } else {
            dtype += "_contig";
        }

        dtype += "_capacity" + std::to_string(capacity_);
        return dtype;
    }

    void serializeDefn(DatabaseManager* db_mgr) override
    {
        defn_serializer_.serializeDefn(db_mgr);
    }

    void autoCollect() override
    {
        autoCollect_();
    }

private:
    template <bool sparse = Sparse>
    typename std::enable_if<sparse, void>::type
    autoCollect_()
    {
        const auto container = data_;
        uint16_t num_valid = 0;

        {
            auto itr = container->begin();
            auto eitr = container->end();

            while (itr != eitr) {
                if (checkValid_(itr)) {
                    ++num_valid;
                }
                ++itr;
            }
        }

        CollectionBuffer buffer(argos_record_.data);
        buffer.writeHeader(getElemId(), num_valid);

        uint16_t bin_idx = 0;
        auto itr = container->begin();
        auto eitr = container->end();
        while (itr != eitr) {
            if (checkValid_(itr)) {
                writeStruct_(*itr, buffer, bin_idx);
            }
            ++itr;
            ++bin_idx;
        }
    }

    template <bool sparse = Sparse>
    typename std::enable_if<!sparse, void>::type
    autoCollect_()
    {
        const auto container = data_;
        auto size = container->size();

        CollectionBuffer buffer(argos_record_.data);
        buffer.writeHeader(getElemId(), size);

        auto itr = container->begin();
        auto eitr = container->end();
        uint16_t bin_idx = 0;

        while (itr != eitr) {
            writeStruct_(*itr, buffer, bin_idx);
            ++itr;
            ++bin_idx;
        }
    }

    template <typename tt = T>
    typename std::enable_if<meta_utils::is_any_pointer<tt>::value, bool>::type
    writeStruct_(const tt& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        if (el) {
            return writeStruct_(*el, buffer, bin_idx);
        }
        return false;
    }

    template <typename tt = T>
    typename std::enable_if<!meta_utils::is_any_pointer<tt>::value, bool>::type
    writeStruct_(const tt& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        if constexpr (Sparse) {
            buffer.writeBucket(bin_idx);
        }
        struct_serializer_->writeStruct(&el, buffer);
        return true;
    }

    template <typename tt = T, bool sparse = Sparse>
    typename std::enable_if<sparse && is_std_vector_v<T>, bool>::type
    checkValid_(typename T::const_iterator itr)
    {
        return *itr != nullptr;
    }

    template <typename tt = T, bool sparse = Sparse>
    typename std::enable_if<sparse && !is_std_vector_v<T>, bool>::type
    checkValid_(typename T::const_iterator itr)
    {
        return itr.isValid();
    }

    template <bool sparse = Sparse>
    typename std::enable_if<!sparse, bool>::type
    checkValid_(typename T::const_iterator)
    {
        return true;
    }

    StructDefnSerializer<value_type> defn_serializer_;
    std::unique_ptr<StructBlobSerializer> struct_serializer_;
    const size_t capacity_;
    const T* data_;
};

template <typename T, bool Sparse>
using IterableCollectorPtr = std::shared_ptr<IterableCollector<T, Sparse>>;

} // namespace simdb
