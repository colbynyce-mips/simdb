// <Structs> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/collection/CollectionBase.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/MetaStructs.hpp"
#include "simdb/utils/TreeSerializer.hpp"
#include <cstring>

namespace simdb
{

/// Data types supported by the collection system. Note that
/// enum struct fields use the std::underlying_type of that
/// enum, e.g. int32_t
enum class StructFields
{
    char_t,
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    float_t,
    double_t,
    string_t
};

template <typename FieldT>
StructFields getFieldDTypeEnum() = delete;

template <> inline StructFields getFieldDTypeEnum<char>() { return StructFields::char_t; }
template <> inline StructFields getFieldDTypeEnum<int8_t>() { return StructFields::int8_t; }
template <> inline StructFields getFieldDTypeEnum<uint8_t>() { return StructFields::uint8_t; }
template <> inline StructFields getFieldDTypeEnum<int16_t>() { return StructFields::int16_t; }
template <> inline StructFields getFieldDTypeEnum<uint16_t>() { return StructFields::uint16_t; }
template <> inline StructFields getFieldDTypeEnum<int32_t>() { return StructFields::int32_t; }
template <> inline StructFields getFieldDTypeEnum<uint32_t>() { return StructFields::uint32_t; }
template <> inline StructFields getFieldDTypeEnum<int64_t>() { return StructFields::int64_t; }
template <> inline StructFields getFieldDTypeEnum<uint64_t>() { return StructFields::uint64_t; }
template <> inline StructFields getFieldDTypeEnum<float>() { return StructFields::float_t; }
template <> inline StructFields getFieldDTypeEnum<double>() { return StructFields::double_t; }
template <> inline StructFields getFieldDTypeEnum<std::string>() { return StructFields::string_t; }

/// These dtype strings are stored in the database to inform the downstream 
/// python module how to interpret the structs' serialized raw bytes.
inline std::string getFieldDTypeStr(const StructFields dtype)
{
    switch (dtype) {
        case StructFields::char_t:   return "char_t";
        case StructFields::int8_t:   return "int8_t";
        case StructFields::uint8_t:  return "uint8_t";
        case StructFields::int16_t:  return "int16_t";
        case StructFields::uint16_t: return "uint16_t";
        case StructFields::int32_t:  return "int32_t";
        case StructFields::uint32_t: return "uint32_t";
        case StructFields::int64_t:  return "int64_t";
        case StructFields::uint64_t: return "uint64_t";
        case StructFields::float_t:  return "float_t";
        case StructFields::double_t: return "double_t";
        case StructFields::string_t: return "string_t";
    }

    throw DBException("Invalid data type");
}

inline size_t getDTypeNumBytes(const StructFields dtype)
{
    switch (dtype) {
        case StructFields::char_t:   return sizeof(char);
        case StructFields::int8_t:   return sizeof(int8_t);
        case StructFields::uint8_t:  return sizeof(uint8_t);
        case StructFields::int16_t:  return sizeof(int16_t);
        case StructFields::uint16_t: return sizeof(uint16_t);
        case StructFields::int32_t:  return sizeof(int32_t);
        case StructFields::uint32_t: return sizeof(uint32_t);
        case StructFields::int64_t:  return sizeof(int64_t);
        case StructFields::uint64_t: return sizeof(uint64_t);
        case StructFields::float_t:  return sizeof(float);
        case StructFields::double_t: return sizeof(double);
        default: break;
    }

    throw DBException("Invalid data type");
}

/// Avoid all SQLite issues (such as not natively supporting uint64_t, or
/// truncating double values to less precision that you wanted), these
/// convertIntToBlob() methods convert scalar PODs to vector<char> (blob).
template <typename IntT>
std::vector<char> convertIntToBlob(const IntT val) = delete;

template <> inline std::vector<char> convertIntToBlob<char>(const char val)
{
    return {val};
}

template <> inline std::vector<char> convertIntToBlob<int8_t>(const int8_t val)
{
    std::vector<char> blob(sizeof(int8_t));
    memcpy(blob.data(), &val, sizeof(int8_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint8_t>(const uint8_t val)
{
    std::vector<char> blob(sizeof(uint8_t));
    memcpy(blob.data(), &val, sizeof(uint8_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int16_t>(const int16_t val)
{
    std::vector<char> blob(sizeof(int16_t));
    memcpy(blob.data(), &val, sizeof(int16_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint16_t>(const uint16_t val)
{
    std::vector<char> blob(sizeof(uint16_t));
    memcpy(blob.data(), &val, sizeof(uint16_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int32_t>(const int32_t val)
{
    std::vector<char> blob(sizeof(int32_t));
    memcpy(blob.data(), &val, sizeof(int32_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint32_t>(const uint32_t val)
{
    std::vector<char> blob(sizeof(uint32_t));
    memcpy(blob.data(), &val, sizeof(uint32_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int64_t>(const int64_t val)
{
    std::vector<char> blob(sizeof(int64_t));
    memcpy(blob.data(), &val, sizeof(int64_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint64_t>(const uint64_t val)
{
    std::vector<char> blob(sizeof(uint64_t));
    memcpy(blob.data(), &val, sizeof(uint64_t));
    return blob;
}

/// Users specialize this template so we can serialize the int->string mapping
/// for their enums. Ints are stored in the database, and the python modules
/// turn them back into enums later.
template <typename EnumT>
void defineEnumMap(std::string& enum_name, std::map<std::string, typename std::underlying_type<EnumT>::type>& map) = delete;

/*!
 * \class EnumMap<EnumT>
 * \brief This class holds and serializes the int->string mapping for the EnumT.
 */
template <typename EnumT>
class EnumMap
{
public:
    using enum_int_t = typename std::underlying_type<EnumT>::type;
    using enum_map_t = std::shared_ptr<std::map<std::string, enum_int_t>>;

    static EnumMap* instance()
    {
        static EnumMap map;
        return &map;
    }

    const enum_map_t getMap() const
    {
        return map_;
    }

    const std::string& getEnumName() const
    {
        return enum_name_;
    }

    void serializeDefn(DatabaseManager* db_mgr) const
    {
        if (!serialized_) {
            auto dtype = getFieldDTypeEnum<enum_int_t>();
            auto int_type_str = getFieldDTypeStr(dtype);

            for (const auto& kvp : *map_) {
                auto enum_val_str = kvp.first;
                auto enum_val_vec = convertIntToBlob<enum_int_t>(kvp.second);

                SqlBlob enum_val_blob;
                enum_val_blob.data_ptr = enum_val_vec.data();
                enum_val_blob.num_bytes = enum_val_vec.size();

                db_mgr->INSERT(SQL_TABLE("EnumDefns"),
                               SQL_COLUMNS("EnumName", "EnumValStr", "EnumValBlob", "IntType"),
                               SQL_VALUES(enum_name_, enum_val_str, enum_val_blob, int_type_str));
            }

            serialized_ = true;
        }
    }

private:
    EnumMap()
    {
        map_ = std::make_shared<std::map<std::string, enum_int_t>>();
        defineEnumMap<EnumT>(enum_name_, *map_);
    }

    enum_map_t map_;
    std::string enum_name_;
    mutable bool serialized_ = false;
};

/// \class FieldBase
/// \brief This class is used to serialize information about a non-enum, non-string field.
class FieldBase
{
public:
    FieldBase(const std::string& name, const StructFields type, const Format format = Format::none)
        : name_(name)
        , dtype_(type)
        , format_(format)
    {
    }

    virtual ~FieldBase() = default;

    const std::string& getName() const
    {
        return name_;
    }

    Format getFormat() const
    {
        return format_;
    }

    StructFields getType() const
    {
        return dtype_;
    }

    virtual size_t getNumBytes() const
    {
        return getDTypeNumBytes(dtype_);
    }

    virtual void serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const
    {
        const auto field_dtype_str = getFieldDTypeStr(dtype_);
        const auto fmt = static_cast<int>(format_);
        const auto is_autocolorize_key = (int)isAutocolorizeKey();
        const auto is_displayed_by_default = (int)isDisplayedByDefault();

        db_mgr->INSERT(SQL_TABLE("StructFields"),
                       SQL_COLUMNS("StructName", "FieldName", "FieldType", "FormatCode", "IsAutoColorizeKey", "IsDisplayedByDefault"),
                       SQL_VALUES(struct_name, name_, field_dtype_str, fmt, is_autocolorize_key, is_displayed_by_default));
    }

    void setIsAutocolorizeKey(bool is_autocolorize_key)
    {
        if (is_autocolorize_key_ && !is_autocolorize_key) {
            throw DBException("Only one column can be used as the autocolorize key");
        }
        is_autocolorize_key_ = is_autocolorize_key;
    }

    bool isAutocolorizeKey() const
    {
        return is_autocolorize_key_;
    }

    void setIsDisplayedByDefault(bool is_displayed_by_default)
    {
        is_displayed_by_default_ = is_displayed_by_default;
    }

    bool isDisplayedByDefault() const
    {
        return is_displayed_by_default_;
    }

private:
    std::string name_;
    StructFields dtype_;
    Format format_;
    bool is_autocolorize_key_ = false;
    bool is_displayed_by_default_ = true;
};

/// \class EnumField
/// \brief This class is used to serialize information about an enum field.
template <typename EnumT>
class EnumField : public FieldBase
{
public:
    EnumField(const char* name)
        : FieldBase(name, getFieldDTypeEnum<typename EnumMap<EnumT>::enum_int_t>())
        , map_(EnumMap<EnumT>::instance()->getMap())
        , enum_name_(EnumMap<EnumT>::instance()->getEnumName())
    {
    }

    virtual void serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const override
    {
        const auto field_name = getName();
        const auto is_autocolorize_key = (int)isAutocolorizeKey();
        const auto is_displayed_by_default = (int)isDisplayedByDefault();

        db_mgr->INSERT(SQL_TABLE("StructFields"),
                       SQL_COLUMNS("StructName", "FieldName", "FieldType", "IsAutoColorizeKey", "IsDisplayedByDefault"),
                       SQL_VALUES(struct_name, field_name, enum_name_, is_autocolorize_key, is_displayed_by_default));

        EnumMap<EnumT>::instance()->serializeDefn(db_mgr);
    }

private:
    const typename EnumMap<EnumT>::enum_map_t map_;
    std::string enum_name_;
};

/// \class StringField
/// \brief This class is used to serialize information about a string field.
class StringField : public FieldBase
{
public:
    StringField(const char* name)
        : FieldBase(name, StructFields::string_t)
    {
    }

    size_t getNumBytes() const override
    {
        return getDTypeNumBytes(StructFields::uint32_t);
    }
};

template <typename StructT>
class StructFieldSerializer;

/// Users specialize this template to write the struct fields one by one into the serializer.
template <typename StructT>
void writeStructFields(const StructT* s, StructFieldSerializer<StructT>* serializer)
{
    (void)serializer;
}

template <typename StructT>
class StructFieldSerializer
{
public:
    StructFieldSerializer(const std::vector<std::unique_ptr<FieldBase>>& fields, CollectionBuffer& buffer)
        : fields_(fields)
        , buffer_(buffer)
    {
    }

    void writeFields(const StructT* s)
    {
        writeStructFields<StructT>(s, this);
    }

    template <typename FieldT>
    typename std::enable_if<!std::is_enum<FieldT>::value && !std::is_same<FieldT, std::string>::value, void>::type
    writeField(const FieldT val)
    {
        auto dtype = getFieldDTypeEnum<FieldT>();
        auto num_bytes = getDTypeNumBytes(dtype);

        if (fields_[current_field_idx_]->getNumBytes() != num_bytes) {
            throw DBException("Data type mismatch in writing struct field");
        }

        buffer_.writeBytes(&val, num_bytes);
        ++current_field_idx_;
    }

    template <typename FieldT>
    typename std::enable_if<std::is_enum<FieldT>::value, void>::type
    writeField(const FieldT val)
    {
        using dtype = typename std::underlying_type<FieldT>::type;
        writeField<dtype>(static_cast<dtype>(val));
    }

    void writeField(const std::string& val)
    {
        if (dynamic_cast<const StringField*>(fields_[current_field_idx_].get())) {
            uint32_t string_id = StringMap::instance()->getStringId(val);
            writeField<uint32_t>(string_id);
        } else {
            throw DBException("Data type mismatch in writing struct field");
        }
    }

    void writeField(const char* val)
    {
        writeField(std::string(val));
    }

private:
    const std::vector<std::unique_ptr<FieldBase>>& fields_;
    size_t current_field_idx_ = 0;
    CollectionBuffer& buffer_;
};

class StructBlobSerializer
{
public:
    StructBlobSerializer(std::vector<std::unique_ptr<FieldBase>>&& fields)
        : fields_(std::move(fields))
    {
    }

    template <typename StructT>
    void writeStruct(const StructT* s, CollectionBuffer& buffer) const
    {
        StructFieldSerializer<StructT> field_serializer(fields_, buffer);
        field_serializer.writeFields(s);
    }

    template <typename StructT>
    void extract(const StructT* s, std::vector<char>& bytes) const
    {
        CollectionBuffer buffer(bytes);
        writeStruct(s, buffer);
    }

private:
    std::vector<std::unique_ptr<FieldBase>> fields_;
};

class StructSchema
{
public:
    void setStructName(const std::string& name)
    {
        struct_name_ = name;
    }

    const std::string& getStructName() const
    {
        return struct_name_;
    }

    size_t getStructNumBytes() const
    {
        size_t num_bytes = 0;
        for (const auto& field : fields_) {
            num_bytes += field->getNumBytes();
        }
        return num_bytes;
    }

    template <typename FieldT>
    typename std::enable_if<!std::is_enum<FieldT>::value && !std::is_same<FieldT, std::string>::value, void>::type
    addField(const char* name)
    {
        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<FieldT>()));
    }

    template <typename FieldT>
    typename std::enable_if<std::is_enum<FieldT>::value, void>::type
    addField(const char* name)
    {
        fields_.emplace_back(new EnumField<FieldT>(name));
    }

    template <typename FieldT>
    typename std::enable_if<std::is_same<FieldT, std::string>::value, void>::type
    addField(const char* name)
    {
        fields_.emplace_back(new StringField(name));
    }

    template <typename FieldT>
    typename std::enable_if<std::is_same<FieldT, uint32_t>::value || std::is_same<FieldT, uint64_t>::value, void>::type
    addHexField(const char* name)
    {
        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<FieldT>(), Format::hex));
    }

    void addBoolField(const char* name)
    {
        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<int32_t>(), Format::boolalpha));
    }

    void setAutoColorizeColumn(const char* name)
    {
        bool found = false;
        for (auto& field : fields_) {
            if (field->getName() == name) {
                field->setIsAutocolorizeKey(true);
                found = true;
            } else {
                field->setIsAutocolorizeKey(false);
            }
        }

        if (!found) {
            throw DBException("Field not found: ") << name;
        }
    }

    void makeColumnHiddenByDefault(const char* name)
    {
        for (auto& field : fields_) {
            if (field->getName() == name) {
                field->setIsDisplayedByDefault(false);
                return;
            }
        }

        throw DBException("Field not found: ") << name;
    }

    void serializeDefn(DatabaseManager* db_mgr) const
    {
        static std::unordered_set<std::string> serialized_structs;
        if (serialized_structs.insert(struct_name_).second) {
            for (auto& field : fields_) {
                field->serializeDefn(db_mgr, struct_name_);
            }
        }
    }

    std::unique_ptr<StructBlobSerializer> createBlobSerializer()
    {
        return std::unique_ptr<StructBlobSerializer>(new StructBlobSerializer(std::move(fields_)));
    }

private:
    std::string struct_name_;
    std::vector<std::unique_ptr<FieldBase>> fields_;
};

template <typename StructT>
inline void defineStructSchema(StructSchema& schema)
{
    (void)schema;
}

template <typename StructT>
class StructDefnSerializer
{
public:
    StructDefnSerializer()
    {
        defineStructSchema<MetaStruct::remove_any_pointer_t<StructT>>(schema_);
    }

    const std::string& getStructName() const
    {
        return schema_.getStructName();
    }

    size_t getStructNumBytes() const
    {
        return schema_.getStructNumBytes();
    }

    void serializeDefn(DatabaseManager* db_mgr) const
    {
        schema_.serializeDefn(db_mgr);
    }

    std::unique_ptr<StructBlobSerializer> createBlobSerializer()
    {
        return schema_.createBlobSerializer();
    }

private:
    StructSchema schema_;
};

/*!
 * \class ScalarStructCollection
 *
 * \brief This class is used to collect struct-like data structures commonly encountered
 *        in simulators, with support for enum and string fields. If you want to collect 
 *        vectors/deques of structs use the IterableStructCollection class.
 */
template <typename StructT>
class ScalarStructCollection : public CollectionBase
{
public:
    using UnderlyingStructT = MetaStruct::remove_any_pointer_t<StructT>;

    /// Construct with a name for this collection.
    ScalarStructCollection(const std::string& name)
        : name_(name)
    {
        static_assert(!MetaStruct::is_any_pointer<UnderlyingStructT>::value,
                      "Template type must be a value type");
    }

    /// \brief   Add a struct to this collection using a backpointer to the struct.
    ///
    /// \param   struct_path Unique struct path e.g. variable name like "struct_foo", or a 
    ///                      dot-delimited simulator location such as "structs.foo"
    ///
    /// \param   struct_ptr Backpointer to the struct.
    ///
    /// \warning This pointer will be read every time the collect() method is called.
    ///          You must ensure that this is a valid pointer for the life of the simulation
    ///          else your program will crash or send bogus data to the database.
    ///
    /// \throws  Throws an exception if called after finalize() or if the struct_path is not unique.
    ///          Also throws if the struct path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addStruct(const std::string& struct_path, const UnderlyingStructT* struct_ptr, const std::string& clk_name = "")
    {
        validatePath_(struct_path);
        structs_.emplace_back(struct_ptr, struct_path, clk_name);
    }

    /// Get the name of this collection.
    std::string getName() const override
    {
        return name_;
    }

    /// Get if the given element path ("root.child1.child2") is in this collection.
    bool hasElement(const std::string& element_path) const override
    {
        for (const auto& tup : structs_) {
            if (std::get<1>(tup) == element_path) {
                return true;
            }
        }
        return false;
    }

    /// Get the element offset in the collection. This is for collections where we
    /// pack all stats of the same data type into the same collection buffer, specifically
    /// StatCollection<T> and ScalarStructCollection<T>.
    int getElementOffset(const std::string& element_path) const override
    {
        for (size_t idx = 0; idx < structs_.size(); ++idx) {
            if (std::get<1>(structs_[idx]) == element_path) {
                return idx;
            }
        }
        return -1;
    }

    /// Get the type of widget that should be displayed when the given element
    /// is dragged-and-dropped onto the Argos widget canvas.
    std::string getWidgetType(const std::string& element_path) const override
    {
        if (hasElement(element_path)) {
            return "StructViewer";
        }
        return "";
    }

    /// Write metadata about this collection to the database.
    /// Returns the collection's primary key in the Collections table.
    int writeCollectionMetadata(DatabaseManager* db_mgr) override
    {
        if (collection_pkey_ != -1) {
            return collection_pkey_;
        }

        auto record = db_mgr->INSERT(SQL_TABLE("Collections"),
                                     SQL_COLUMNS("Name", "DataType", "IsContainer", "IsSparse", "Capacity"),
                                     SQL_VALUES(name_, meta_serializer_.getStructName(), 0, 0, (int)structs_.size()));

        collection_pkey_ = record->getId();
        meta_serializer_.serializeDefn(db_mgr);

        return collection_pkey_;
    }

    /// Method does not apply to this collection type.
    bool rerouteTimeseries(TimeseriesCollector*) override
    {
        return false;
    }

    /// Give collections a chance to write to the database after simulation.
    void onPipelineCollectorClosing(DatabaseManager*) override
    {
    }

    /// Set the heartbeat for this collection. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    void setHeartbeat(const size_t heartbeat) override
    {
        (void)heartbeat;
    }

    /// \brief  Finalize this collection.
    /// \throws Throws an exception if called more than once.
    void finalize() override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }

        blob_serializer_ = meta_serializer_.createBlobSerializer();
        finalized_ = true;
    }

    /// \brief  Collect all structs in this collection into one data vector
    ///         and write the blob to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(CollectionBuffer& buffer) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        buffer.writeHeader(collection_pkey_, structs_.size());
        for (const auto& tup : structs_) {
            const UnderlyingStructT *container = std::get<0>(tup);
            writeStruct_(container, buffer);
        }
    }

private:
    template <typename S=UnderlyingStructT>
    typename std::enable_if<MetaStruct::is_any_pointer<S>::value, void>::type
    writeStruct_(const S s, CollectionBuffer& buffer)
    {
        if (s) {
            writeStruct_(*s, buffer);            
        }
    }

    template <typename S=UnderlyingStructT>
    typename std::enable_if<!MetaStruct::is_any_pointer<S>::value, void>::type
    writeStruct_(const S& s, CollectionBuffer& buffer)
    {
        blob_serializer_->writeStruct(&s, buffer);
    }

    /// Name of this collection. Serialized to the database.
    std::string name_;

    /// All the structs' backpointers their paths, and their clock names.
    std::vector<std::tuple<const UnderlyingStructT*, std::string, std::string>> structs_;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Serializer to write all info about this struct to the DB.
    StructDefnSerializer<UnderlyingStructT> meta_serializer_;

    /// Serializer to pack all struct data into a blob.
    std::unique_ptr<StructBlobSerializer> blob_serializer_;
};

} // namespace simdb
