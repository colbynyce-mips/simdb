// <Scalars> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/collection/Structs.hpp"
#include "simdb/collection/Scalars.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

<<<<<<< Updated upstream
class BlobConverter
{
public:
    template <typename CollectableT>
    typename std::enable_if<meta_utils::is_any_pointer<CollectableT>::value, void>::type
    convertToByteVector(const CollectableT& val, std::vector<char>& vec) const
=======
template <typename CollectableT, typename Enable=void>
class BlobConverter;

template <typename CollectableT>
class BlobConverter<CollectableT, typename std::enable_if<std::is_trivial<CollectableT>::value && std::is_standard_layout<CollectableT>::value>::type>
{
public:
    template <typename tt = CollectableT>
    typename std::enable_if<meta_utils::is_any_pointer<tt>::value, void>::type
    convertToByteVector(const tt& val, std::vector<char>& vec) const
>>>>>>> Stashed changes
    {
        if (val) {
            convertToByteVector(*val, vec);
        } else {
            vec.clear();
        }
    }

<<<<<<< Updated upstream
    template <typename CollectableT>
    typename std::enable_if<!meta_utils::is_any_pointer<CollectableT>::value && std::is_trivial<CollectableT>::value, void>::type
    convertToByteVector(const CollectableT& val, std::vector<char>& vec) const
=======
    template <typename tt = CollectableT>
    typename std::enable_if<!meta_utils::is_any_pointer<tt>::value, void>::type
    convertToByteVector(const tt& val, std::vector<char>& vec) const
>>>>>>> Stashed changes
    {
        vec.resize(sizeof(tt));
        memcpy(vec.data(), &val, sizeof(tt));
    }

<<<<<<< Updated upstream
    template <typename CollectableT>
    typename std::enable_if<!meta_utils::is_any_pointer<CollectableT>::value && !std::is_trivial<CollectableT>::value, void>::type
    convertToByteVector(const CollectableT& val, std::vector<char>& vec) const
=======
template <typename CollectableT>
class BlobConverter<CollectableT, typename std::enable_if<!std::is_trivial<CollectableT>::value || !std::is_standard_layout<CollectableT>::value>::type>
{
public:
    BlobConverter()
>>>>>>> Stashed changes
    {
        StructDefnSerializer<CollectableT> meta_serializer;
        auto blob_serializer = meta_serializer.createBlobSerializer();

<<<<<<< Updated upstream
=======
    template <typename tt = CollectableT>
    typename std::enable_if<meta_utils::is_any_pointer<tt>::value, void>::type
    convertToByteVector(const tt& val, std::vector<char>& vec) const
    {
        if (val) {
            convertToByteVector(*val, vec);
        } else {
            vec.clear();
        }
    }

    template <typename tt = CollectableT>
    typename std::enable_if<!meta_utils::is_any_pointer<tt>::value, void>::type
    convertToByteVector(const tt& val, std::vector<char>& vec) const
    {
>>>>>>> Stashed changes
        CollectionBuffer buffer(vec);
        blob_serializer->writeStruct(&val, buffer);
    }
<<<<<<< Updated upstream
=======

private:
    /// Serializer to pack all struct data into a blob.
    std::unique_ptr<StructBlobSerializer> blob_serializer_;
>>>>>>> Stashed changes
};

/*!
 * \class AnyCollection
 *
 * \brief This collection class is a catch-all for collecting
 * any data type as a byte vector (sqlite blob).
 */
template <typename CollectableT>
class AnyCollection : public CollectionBase
{
public:
    /// Construct with an element path that is unique across the simulator.
    AnyCollection(const std::string& elem_path)
        : elem_path_(elem_path)
    {
        validatePath_(elem_path_);
    }

    /// Get the name of this collection.
    std::string getName() const override final
    {
        return elem_path_;
    }

    /// AnyCollections only have one element.
    bool hasElement(const std::string& element_path) const override final
    {
        return elem_path_ == element_path;
    }

    /// AnyCollections do not use offsets.
    int getElementOffset(const std::string&) const override final
    {
        return -1;
    }

    /// Get the type of widget that should be displayed when the given element
    /// is dragged-and-dropped onto the Argos widget canvas.
    std::string getWidgetType(const std::string& element_path) const override
    {
        (void)element_path;
        return "";
    }

    /// Write metadata about this collection to the database.
    /// Returns the collection's primary key in the Collections table.
    int writeCollectionMetadata(DatabaseManager* db_mgr) override
    {
        if (collection_pkey_ != -1) {
            return collection_pkey_;
        }

        constexpr auto is_simple_scalar =
            std::is_same<CollectableT, uint8_t>::value  ||
            std::is_same<CollectableT, uint16_t>::value ||
            std::is_same<CollectableT, uint32_t>::value ||
            std::is_same<CollectableT, uint64_t>::value ||
            std::is_same<CollectableT, int8_t>::value   ||
            std::is_same<CollectableT, int16_t>::value  ||
            std::is_same<CollectableT, int32_t>::value  ||
            std::is_same<CollectableT, int64_t>::value  ||
            std::is_same<CollectableT, float>::value    ||
            std::is_same<CollectableT, double>::value   ||
            std::is_same<CollectableT, bool>::value;

        // Scalar numbers
        if constexpr (is_simple_scalar) {
            StatCollection<CollectableT> collection("");
            collection_pkey_ = collection.writeCollectionMetadata(db_mgr);
        }

        // Structs
        else {
            ScalarStructCollection<CollectableT> collection("");
            collection_pkey_ = collection.writeCollectionMetadata(db_mgr);
        }

        return collection_pkey_;
    }

    /// AnyCollections do not use the TimeseriesCollector.
    bool rerouteTimeseries(TimeseriesCollector*) override final
    {
        return false;
    }

    /// AnyCollections have nothing to do after collection is over.
    void onPipelineCollectorClosing(DatabaseManager*) override final
    {
    }

    /// AnyCollections do not use heartbeats.
    void setHeartbeat(const size_t) override final
    {
    }

    /// \brief  Finalize this collection.
    /// \throws Throws an exception if called more than once.
    void finalize() override final
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }
        finalized_ = true;
    }

    /// \brief Collect a scalar integer/float once.
    template <typename T=CollectableT>
    typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) && !std::is_enum<T>::value, void>::type
    collect(const T& val)
    {
        blob_converter_.convertToByteVector(val, raw_bytes_);
        num_valid_byte_vec_reads_ = 1;
    }

    /// \brief Collect a non-scalar (e.g. struct) once.
    template <typename T=CollectableT>
    typename std::enable_if<!std::is_scalar<T>::value, void>::type
    collect(const T& val)
    {
        blob_converter_.convertToByteVector(val, raw_bytes_);
        num_valid_byte_vec_reads_ = 1;
    }

    /// \brief Collect a scalar integer/float for more than one cycle.
    template <typename T=CollectableT>
    typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) && !std::is_enum<T>::value, void>::type
    collectWithDuration(const T& val, const uint32_t duration)
    {
        blob_converter_.convertToByteVector(val, raw_bytes_);
        num_valid_byte_vec_reads_ = duration;
    }

    /// \brief Collect a non-scalar (e.g. struct) for more than one cycle.
    template <typename T=CollectableT>
    typename std::enable_if<!std::is_scalar<T>::value, void>::type
    collectWithDuration(const T& val, const uint32_t duration)
    {
        blob_converter_.convertToByteVector(val, raw_bytes_);
        num_valid_byte_vec_reads_ = duration;
    }

private:
    /// \brief Collection is performed by the TimeseriesCollector.
    void collect(CollectionBuffer& buffer) override final
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        if (num_valid_byte_vec_reads_ == 0) {
            return;
        }

        buffer.writeHeader(collection_pkey_, 1);
        buffer.writeBytes(raw_bytes_.data(), raw_bytes_.size());
        --num_valid_byte_vec_reads_;
    }

    /// Simulator-wide unique element path.
    std::string elem_path_;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Collected data as raw bytes.
    std::vector<char> raw_bytes_;

    /// Serializer to pack all collected data into a byte vector.
    BlobConverter blob_converter_;

    /// Track the number of times we have read the collected byte vector.
    /// This is used to support collectWithDuration(), where we will hold
    /// onto the serialized byte vector for N cycles.
    uint32_t num_valid_byte_vec_reads_ = 0;
};

} // namespace simdb
