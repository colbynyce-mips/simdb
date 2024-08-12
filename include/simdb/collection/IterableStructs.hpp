// <IterableStructs> -*- C++ -*-

#pragma once

#include "simdb/collection/Structs.hpp"

namespace simdb
{

/*!
 * \class IterableStructCollection
 *
 * \brief This class is used to collect struct-like data structures held in
 *        vectors, deques, and hand-rolled containers supporting range-based
 *        loop semantics. These are most commonly held in containers of smart
 *        pointers or raw pointers, which we enforce as well to support sparse
 *        containers (a nullptr entry results in a python None value when the
 *        collection is deserialized).
 */
template <typename ContainerT, bool Sparse=false>
class IterableStructCollection : public CollectionBase
{
public:
    using StructPtrT = typename ContainerT::value_type;
    using StructT = typename std::remove_pointer<StructPtrT>::type;

    /// Construct with a name for this collection.
    IterableStructCollection(const std::string& name)
        : name_(name)
    {
        static_assert(is_any_pointer<StructPtrT>::value,
                      "Must collect a container such as " \
                      "std::vector<std::shared_ptr<MyStruct>> or " \
                      "std::deque<MyStruct*> (note that you may have to " \
                      "specialize is_pointer<T> for your type T in the std " \
                      "namespace)");
    }

    /// \brief   Add a container to this collection using a backpointer to the container.
    ///
    /// \param   stat_path Unique container path e.g. variable name like "container_foo", or a 
    ///                    dot-delimited simulator location such as "containers.foo"
    ///
    /// \param   container_ptr Backpointer to the container.
    ///
    /// \param   capacity Container capacity. Can be zero for unbounded containers.
    ///
    /// \warning The container pointer will be read every time the collect() method is called.
    ///          You must ensure that this is a valid pointer for the life of the simulation
    ///          else your program will crash or send bogus data to the database.
    ///
    /// \throws  Throws an exception if called after finalize() or if the stat_path is not unique.
    ///          Also throws if the stat path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addContainer(const std::string& container_path, const ContainerT* container_ptr, size_t capacity)
    {
        validatePath_(container_path);
        containers_.emplace_back(container_ptr, container_path, capacity);
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
                                     SQL_VALUES(name_, meta_serializer_.getStructName(), 1));

        collection_pkey_ = record->getId();
        auto struct_num_bytes = meta_serializer_.getStructNumBytes();

        for (const auto& tup : containers_) {
            auto record = db_mgr->INSERT(SQL_TABLE("CollectionPaths"),
                                         SQL_COLUMNS("CollectionID", "SimPath"),
                                         SQL_VALUES(collection_pkey_, std::get<1>(tup)));

            auto path_id = record->getId();
            auto capacity = std::get<2>(tup);
            auto is_sparse = Sparse ? 1 : 0;

            db_mgr->INSERT(SQL_TABLE("ContainerMeta"),
                           SQL_COLUMNS("PathID", "Capacity", "IsSparse"),
                           SQL_VALUES(path_id, capacity, is_sparse));
        }

        struct_num_bytes_ = struct_num_bytes;
        meta_serializer_.writeMetadata(db_mgr, getName());
        blob_serializer_ = meta_serializer_.createBlobSerializer();
        finalized_ = true;
    }

    /// \brief  Collect all containers in this collection into separate data vectors (one blob
    ///         per container) and write them to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(DatabaseManager* db_mgr, const TimestampBase* timestamp) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        for (const auto& tup : containers_) {
            const ContainerT* container = std::get<0>(tup);

            auto size = container->size();
            auto container_bytes = size * struct_num_bytes_;
            container_blob_.resize(container_bytes);

            if (Sparse) {
                sparse_container_valid_flags_.resize(size);
                std::fill(sparse_container_valid_flags_.begin(), sparse_container_valid_flags_.end(), 0);
            }

            char *dest = container_blob_.data();
            size_t num_structs_written = 0;

            size_t container_idx = 0;
            for (const auto& s : *container) {
                if (writeStruct_(s, dest)) {
                    ++num_structs_written;
                    if (Sparse) {
                        sparse_container_valid_flags_[container_idx] = 1;
                    }
                } else if (!Sparse) {
                    container_blob_.resize(num_structs_written);
                    break;
                } else {
                    sparse_container_valid_flags_[container_idx] = 0;
                }

                ++container_idx;
            }

            std::unique_ptr<WorkerTask> task(new IterableStructSerializer<char>(
                db_mgr, collection_pkey_, timestamp, container_blob_, sparse_container_valid_flags_));

            db_mgr->getConnection()->getTaskQueue()->addTask(std::move(task));
        }
    }

private:
    template <typename ElemT>
    typename std::enable_if<is_any_pointer<ElemT>::value, bool>::type
    writeStruct_(const ElemT& el, char*& dest)
    {
        return el ? writeStruct_(*el, dest) : false;
    }

    template <typename ElemT>
    typename std::enable_if<!is_any_pointer<ElemT>::value, bool>::type
    writeStruct_(const ElemT& el, char*& dest)
    {
        blob_serializer_->writeStruct(&el, dest);
        return true;
    }

    /// Name of this collection. Serialized to the database.
    std::string name_;

    /// All the containers' backpointers, their paths, and their capacities.
    std::vector<std::tuple<const ContainerT*, std::string, size_t>> containers_;

    /// Size of one struct when serialized to raw bytes.
    size_t struct_num_bytes_ = 0;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Serializer to write all info about this struct to the DB.
    StructDefnSerializer<StructT> meta_serializer_;

    /// Serializer to pack all struct data into a blob.
    std::unique_ptr<StructBlobSerializer> blob_serializer_;

    /// All raw bytes for one struct in one container. Reused over and over 
    /// throughout collection without reallocating.
    std::vector<char> container_blob_;

    /// Mask of valid/invalid flags for one container:
    ///    container:  {a*, b*, nullptr, d*}
    ///    sparse_container_valid_flags_: {1,1,0,1}
    std::vector<int> sparse_container_valid_flags_;
};

} // namespace simdb
