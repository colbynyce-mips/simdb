// <Serialize.hpp> -*- C++ -*-

#pragma once

#include "simdb/serialize/CollectionBuffer.hpp"
#include "simdb/utils/StringMap.hpp"
#include "simdb/utils/MetaStructs.hpp"

#include <stdint.h>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <unordered_set>
#include <cxxabi.h>

namespace simdb
{

/*!
 * \brief Represents the internal buffer size for demangling C++ symbols via
 * sparta::demangle
 */
#define DEMANGLE_BUF_LENGTH 4096

/*!
 * \brief Demangles a C++ symbol
 * \param Name Symbol name to demangle
 * \return Demangled name if successful. If failed, returns the input
 * name. Note that demangled names may match input name.
 * \note Demangling is limited by DEMANGLE_BUF_LENGTH. results may be
 * truncated or fail for very symbols. Change this value to support longer
 * symbol names.
 */
inline std::string demangle(const std::string& name) noexcept
{
    char buf[DEMANGLE_BUF_LENGTH];
    size_t buf_size = DEMANGLE_BUF_LENGTH;
    int status;
    char* out = __cxxabiv1::__cxa_demangle(name.c_str(), buf, &buf_size, &status);
    if(nullptr == out){
        return name;
    }
    return std::string(out);
}

enum class Format
{
    none = 0,
    hex = 1,
    boolalpha = 2
};

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
        case StructFields::char_t   : return "char_t";
        case StructFields::int8_t   : return "int8_t";
        case StructFields::uint8_t  : return "uint8_t";
        case StructFields::int16_t  : return "int16_t";
        case StructFields::uint16_t : return "uint16_t";
        case StructFields::int32_t  : return "int32_t";
        case StructFields::uint32_t : return "uint32_t";
        case StructFields::int64_t  : return "int64_t";
        case StructFields::uint64_t : return "uint64_t";
        case StructFields::float_t  : return "float_t";
        case StructFields::double_t : return "double_t";
        case StructFields::string_t : return "string_t";
    }

    throw DBException("Invalid data type");
}

inline size_t getDTypeNumBytes(const StructFields dtype)
{
    switch (dtype) {
        case StructFields::char_t   : return sizeof(char);
        case StructFields::int8_t   : return sizeof(int8_t);
        case StructFields::uint8_t  : return sizeof(uint8_t);
        case StructFields::int16_t  : return sizeof(int16_t);
        case StructFields::uint16_t : return sizeof(uint16_t);
        case StructFields::int32_t  : return sizeof(int32_t);
        case StructFields::uint32_t : return sizeof(uint32_t);
        case StructFields::int64_t  : return sizeof(int64_t);
        case StructFields::uint64_t : return sizeof(uint64_t);
        case StructFields::float_t  : return sizeof(float);
        case StructFields::double_t : return sizeof(double);
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

    void serializeDefn(DatabaseManager* db_mgr) const;

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

    virtual void serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const;

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

    virtual void serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const override;

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
    StructBlobSerializer(const std::vector<std::unique_ptr<FieldBase>>& fields)
        : fields_(fields)
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
    const std::vector<std::unique_ptr<FieldBase>>& fields_;
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
        return std::unique_ptr<StructBlobSerializer>(new StructBlobSerializer(fields_));
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
        defineStructSchema<meta_utils::remove_any_pointer_t<StructT>>(schema_);
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

} // namespace simdb
