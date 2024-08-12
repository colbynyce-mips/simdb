// <Structs> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/collection/CollectionBase.hpp"
#include "simdb/collection/BlobSerializer.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/PointerUtils.hpp"
#include <cstring>

namespace simdb
{

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
    }

    throw DBException("Invalid data type");
}

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

template <typename EnumT>
void defineEnumMap(std::string& enum_name, std::map<std::string, typename std::underlying_type<EnumT>::type>& map) = delete;

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

    void writeMetadata(DatabaseManager* db_mgr) const
    {
        if (!serialized_) {
            auto dtype = getFieldDTypeEnum<enum_int_t>();
            auto int_type_str = getFieldDTypeStr(dtype);

            for (const auto& kvp : *map_) {
                auto enum_val_str = kvp.first;
                auto enum_val_vec = convertIntToBlob<enum_int_t>(kvp.second);

                Blob enum_val_blob;
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

class StructField
{
public:
    StructField(const std::string& name, const StructFields type)
        : name_(name)
        , dtype_(type)
    {
    }

    virtual ~StructField() = default;

    const std::string& getName() const
    {
        return name_;
    }

    StructFields getType() const
    {
        return dtype_;
    }

    virtual size_t getNumBytes() const
    {
        return getDTypeNumBytes(dtype_);
    }

    virtual void writeMetadata(DatabaseManager* db_mgr, const std::string& collection_name) const
    {
        db_mgr->INSERT(SQL_TABLE("StructFields"),
                       SQL_COLUMNS("CollectionName", "FieldName", "FieldType"),
                       SQL_VALUES(collection_name, name_, getFieldDTypeStr(dtype_)));
    }

private:
    std::string name_;
    StructFields dtype_;
};

template <typename EnumT>
class StructEnumField : public StructField
{
public:
    StructEnumField(const char* name)
        : StructField(name, getFieldDTypeEnum<typename EnumMap<EnumT>::enum_int_t>())
        , map_(EnumMap<EnumT>::instance()->getMap())
        , enum_name_(EnumMap<EnumT>::instance()->getEnumName())
    {
    }

    virtual void writeMetadata(DatabaseManager* db_mgr, const std::string& collection_name) const override
    {
        db_mgr->INSERT(SQL_TABLE("StructFields"),
                       SQL_COLUMNS("CollectionName", "FieldName", "FieldType"),
                       SQL_VALUES(collection_name, getName(), enum_name_));

        EnumMap<EnumT>::instance()->writeMetadata(db_mgr);
    }

private:
    const typename EnumMap<EnumT>::enum_map_t map_;
    std::string enum_name_;
};

class StructStringField : public StructField
{
public:
    StructStringField(const char* name)
        : StructField(name, StructFields::string_t)
    {
        map_ = StringMap::instance()->getMap();
    }

    size_t getNumBytes() const override
    {
        return getDTypeNumBytes(StructFields::uint32_t);
    }

private:
    typename StringMap::string_map_t map_;
};

template <typename StructT>
class StructFieldSerializer;

template <typename StructT>
void writeStructFields(const StructT* s, StructFieldSerializer<StructT>* serializer) = delete;

template <typename StructT>
class StructFieldSerializer
{
public:
    StructFieldSerializer(const std::vector<std::unique_ptr<StructField>>& fields, char*& dest)
        : fields_(fields)
        , dest_(dest)
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

        memcpy(dest_, &val, num_bytes);
        dest_ += num_bytes;
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
        if (dynamic_cast<const StructStringField*>(fields_[current_field_idx_].get())) {
            uint32_t string_id = StringMap::instance()->getStringId(val);
            writeField<uint32_t>(string_id);
        } else {
            throw DBException("Data type mismatch in writing struct field");
        }
    }

private:
    const std::vector<std::unique_ptr<StructField>>& fields_;
    size_t current_field_idx_ = 0;
    char*& dest_;
};

class StructBlobSerializer
{
public:
    StructBlobSerializer(std::vector<std::unique_ptr<StructField>>&& fields)
        : fields_(std::move(fields))
    {
    }

    template <typename StructT>
    void writeStruct(const StructT* s, char*& dest)
    {
        StructFieldSerializer<StructT> field_serializer(fields_, dest);
        field_serializer.writeFields(s);
    }

private:
    std::vector<std::unique_ptr<StructField>> fields_;
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
        fields_.emplace_back(new StructField(name, getFieldDTypeEnum<FieldT>()));
    }

    template <typename FieldT>
    typename std::enable_if<std::is_enum<FieldT>::value && !std::is_same<FieldT, std::string>::value, void>::type
    addField(const char* name)
    {
        fields_.emplace_back(new StructEnumField<FieldT>(name));
    }

    template <typename FieldT>
    typename std::enable_if<std::is_same<FieldT, std::string>::value, void>::type
    addField(const char* name)
    {
        fields_.emplace_back(new StructStringField(name));
    }

    void writeMetadata(DatabaseManager* db_mgr, const std::string& collection_name) const
    {
        for (auto& field : fields_) {
            field->writeMetadata(db_mgr, collection_name);
        }
    }

    std::unique_ptr<StructBlobSerializer> createBlobSerializer()
    {
        return std::unique_ptr<StructBlobSerializer>(new StructBlobSerializer(std::move(fields_)));
    }

private:
    std::string struct_name_;
    std::vector<std::unique_ptr<StructField>> fields_;
};

template <typename StructT>
void defineStructSchema(StructSchema& schema);

template <typename StructT>
class StructDefnSerializer
{
public:
    StructDefnSerializer()
    {
        defineStructSchema<typename remove_any_pointer<StructT>::type>(schema_);
    }

    const std::string& getStructName() const
    {
        return schema_.getStructName();
    }

    size_t getStructNumBytes() const
    {
        return schema_.getStructNumBytes();
    }

    void writeMetadata(DatabaseManager* db_mgr, const std::string& collection_name) const
    {
        schema_.writeMetadata(db_mgr, collection_name);
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
    /// Construct with a name for this collection.
    ScalarStructCollection(const std::string& name)
        : name_(name)
    {
        static_assert(!is_any_pointer<StructT>::value,
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
    void addStruct(const std::string& struct_path, const StructT* struct_ptr)
    {
        validateStructPath_(struct_path);

        if (finalized_) {
            throw DBException("Cannot add struct to collection after it's been finalized");
        }

        if (!struct_paths_.insert(struct_path).second) {
            throw DBException("Cannot add struct to collection - already have a struct with this path: ") << struct_path;
        }

        structs_.emplace_back(struct_ptr, struct_path);
    }

    /// Get the name of this collection.
    std::string getName() const override
    {
        return name_;
    }

    /// \brief  Write metadata about this collection to the database.
    /// \throws Throws an exception if called more than once.
    void finalize(DatabaseManager* db_mgr) override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }

        auto record = db_mgr->INSERT(SQL_TABLE("Collections"),
                                     SQL_COLUMNS("Name", "DataType", "IsContainer"),
                                     SQL_VALUES(name_, meta_serializer_.getStructName(), 0));

        collection_pkey_ = record->getId();

        for (const auto& pair : structs_) {
            db_mgr->INSERT(SQL_TABLE("CollectionPaths"),
                           SQL_COLUMNS("CollectionID", "StatPath"),
                           SQL_VALUES(collection_pkey_, pair.second));
        }

        auto struct_num_bytes = meta_serializer_.getStructNumBytes();
        auto total_num_bytes = struct_num_bytes * structs_.size();
        structs_blob_.resize(total_num_bytes);

        meta_serializer_.writeMetadata(db_mgr, getName());
        blob_serializer_ = meta_serializer_.createBlobSerializer();
        finalized_ = true;
    }

    /// \brief  Collect all structs in this collection into one data vector
    ///         and write the blob to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(DatabaseManager* db_mgr, const TimestampBase* timestamp) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        char* dest = structs_blob_.data();
        for (auto& pair : structs_) {
            blob_serializer_->writeStruct(pair.first, dest);
        }

        std::unique_ptr<WorkerTask> task(new CollectableSerializer<char>(
            db_mgr, collection_pkey_, timestamp, structs_blob_));

        db_mgr->getConnection()->getTaskQueue()->addTask(std::move(task));
    }

private:
    /// Validate that the struct path is either a valid python variable name, or a
    /// dot-delimited path of valid python variable names:
    ///
    ///   counter_foo              VALID
    ///   structs.foo              VALID
    ///   5_counter_foo            INVALID
    ///   structs?.foo             INVALID 
    void validateStructPath_(std::string struct_path)
    {
        auto validate_python_var = [&](const std::string& varname) {
            if (varname.empty() || !isalpha(varname[0]) && varname[0] != '_') {
                return false;
            }

            for (char ch : varname) {
                if (!isalnum(ch) && ch != '_') {
                    return false;
                }
            }

            return true;
        };

        std::vector<std::string> varnames;
        const char* delim = ".";
        char* token = std::strtok(const_cast<char*>(struct_path.c_str()), delim);

        while (token) {
            varnames.push_back(token);
            token = std::strtok(nullptr, delim);
        }

        for (const auto& varname : varnames) {
            if (!validate_python_var(varname)) {
                std::ostringstream oss;
                oss << "Invalid struct path for collection '" << name_ << "'. Not a valid python variable name: " << varname;
                throw DBException(oss.str());
            }
        }
    }

    /// Name of this collection. Serialized to the database.
    std::string name_;

    /// Quick lookup to ensure that struct paths are all unique.
    std::unordered_set<std::string> struct_paths_;

    /// All the structs' backpointers and their paths.
    std::vector<std::pair<const StructT*, std::string>> structs_;

    /// Flag saying whether we can add more structs to this collection.
    bool finalized_ = false;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Serializer to write all info about this struct to the DB.
    StructDefnSerializer<StructT> meta_serializer_;

    /// Serializer to pack all struct data into a blob.
    std::unique_ptr<StructBlobSerializer> blob_serializer_;

    /// All the structs' values in one vector. Held in a member variable
    /// so we do not reallocate these (potentially large) vectors with
    /// every call to collect(), which can be called very many times
    /// during the simulation.
    std::vector<char> structs_blob_;

    /// All the structs' compressed values in one vector.
    std::vector<char> structs_blob_compressed_;
};

} // namespace simdb
