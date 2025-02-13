// <CollectionMgr> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/serialize/Serialize.hpp"

namespace simdb
{

class CollectionPointBase
{
public:
    CollectionPointBase(const std::string& path, const std::string& clock)
        : path_(path)
        , clock_(clock)
    {}

    const std::string& getPath() const
    {
        return path_;
    }

    const std::string& getClock() const
    {
        return clock_;
    }

    virtual ~CollectionPointBase() = default;

    virtual void getStructSchema(StructSchema& schema) const = 0;

    virtual std::string getDataTypeStr() const = 0;

    virtual void sweep() = 0;

private:
    std::string path_;
    std::string clock_;
};

template <typename T>
class Collectable : public CollectionPointBase
{
public:
    Collectable(const std::string& path, const std::string& clock, const T* data = nullptr)
        : CollectionPointBase(path, clock)
        , data_(data)
    {}

    void getStructSchema(StructSchema& schema) const override
    {
        using value_type = typename meta_utils::remove_any_pointer_t<T>;
        defineStructSchema<value_type>(schema);
    }

    std::string getDataTypeStr() const override
    {
        using value_type = typename meta_utils::remove_any_pointer_t<T>;

        if constexpr (std::is_trivial<value_type>::value && !std::is_same<value_type, bool>::value) {
            return getFieldDTypeStr(getFieldDTypeEnum<value_type>());
        } else if constexpr (std::is_same<value_type, std::string>::value) {
            return "string";
        } else if constexpr (std::is_same<value_type, bool>::value) {
            return "bool";
        } else {
            return "TODO";
        }
    }

    void sweep() override
    {
        (void)data_;
    }

private:
    const T* data_;
};

template <typename T, bool Sparse>
class IterableCollector : public CollectionPointBase
{
public:
    IterableCollector(const std::string& path, const std::string& clock, const T* data)
        : CollectionPointBase(path, clock)
        , data_(data)
    {}

    void getStructSchema(StructSchema& schema) const override
    {
        using value_type = meta_utils::remove_any_pointer_t<typename T::value_type>;
        defineStructSchema<value_type>(schema);
    }

    std::string getDataTypeStr() const override
    {
        std::string dtype_str;
        using value_type = typename meta_utils::remove_any_pointer_t<typename T::value_type>;
        dtype_str += demangle(typeid(value_type).name()) + "_";
        dtype_str += (Sparse ? "sparse" : "contig");
        dtype_str += "_capacity" + std::to_string(data_->capacity());
        return dtype_str;
    }

    void sweep() override
    {
        (void)data_;
    }

private:
    const T* data_;
};

class DatabaseManager;

/*!
 * \class CollectionMgr
 *
 * \brief This class provides an easy way to handle simulation-wide data collection.
 */
class CollectionMgr
{
public:
    /// Construct with the DatabaseManager and SQLiteTransaction.
    CollectionMgr(size_t heartbeat)
        : pipeline_heartbeat_(heartbeat)
    {
    }

    /// Add a new clock domain for collection.
    void addClock(const std::string& name, const uint32_t period)
    {
        clocks_[name] = period;
    }

    /// Populate the schema with the appropriate tables for all the collections.
    void defineSchema(Schema& schema) const
    {
        using dt = SqlDataType;

        schema.addTable("CollectionGlobals")
            .addColumn("Heartbeat", dt::int32_t)
            .setColumnDefaultValue("Heartbeat", 10);

        schema.addTable("Clocks")
            .addColumn("Name", dt::string_t)
            .addColumn("Period", dt::int32_t);

        schema.addTable("ElementTreeNodes")
            .addColumn("Name", dt::string_t)
            .addColumn("ParentID", dt::int32_t);

        schema.addTable("CollectableTreeNodes")
            .addColumn("ElementTreeNodeID", dt::int32_t)
            .addColumn("ClockID", dt::int32_t)
            .addColumn("DataType", dt::string_t);

        schema.addTable("StructFields")
            .addColumn("StructName", dt::string_t)
            .addColumn("FieldName", dt::string_t)
            .addColumn("FieldType", dt::string_t)
            .addColumn("FormatCode", dt::int32_t)
            .addColumn("IsAutoColorizeKey", dt::int32_t)
            .addColumn("IsDisplayedByDefault", dt::int32_t)
            .setColumnDefaultValue("IsAutoColorizeKey", 0)
            .setColumnDefaultValue("IsDisplayedByDefault", 1);

        schema.addTable("EnumDefns")
            .addColumn("EnumName", dt::string_t)
            .addColumn("EnumValStr", dt::string_t)
            .addColumn("EnumValBlob", dt::blob_t)
            .addColumn("IntType", dt::string_t);

        schema.addTable("StringMap")
            .addColumn("IntVal", dt::int32_t)
            .addColumn("String", dt::string_t);
    }

    template <typename T>
    std::shared_ptr<Collectable<T>> createCollectable(
        const std::string& path,
        const std::string& clock,
        const T* data = nullptr)
    {
        auto collectable = std::make_shared<Collectable<T>>(path, clock, data);
        collectables_.push_back(collectable);
        return collectable;
    }

    template <typename T, bool Sparse>
    std::shared_ptr<IterableCollector<T, Sparse>> createIterableCollector(
        const std::string& path,
        const std::string& clock,
        const T* data = nullptr)
    {
        auto collector = std::make_shared<IterableCollector<T, Sparse>>(path, clock, data);
        collectables_.push_back(collector);
        return collector;
    }

private:
    /// The max number of cycles that we employ the optimization "only write to the
    /// database if the collected data is different from the last collected data".
    /// This prevents Argos from having to go back more than N cycles to find the
    /// last known value.
    const size_t pipeline_heartbeat_;

    /// All registered clocks.
    std::unordered_map<std::string, uint32_t> clocks_;

    /// All collectables.
    std::vector<std::shared_ptr<CollectionPointBase>> collectables_;

    friend class DatabaseManager;
};

} // namespace simdb
