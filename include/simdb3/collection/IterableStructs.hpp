// <IterableStructs> -*- C++ -*-

#pragma once

#include "simdb3/collection/Structs.hpp"

namespace simdb3
{

template <typename T>
struct is_std_vector : std::false_type {};

template <typename T>
struct is_std_vector<std::vector<T>> : std::true_type {};

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
    using StructT = MetaStruct::remove_any_pointer_t<StructPtrT>;

    /// Construct with a name for this collection.
    IterableStructCollection(const std::string& name)
        : name_(name)
    {
        static_assert(MetaStruct::is_any_pointer<StructPtrT>::value,
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
    ///          Also throws if the element path cannot later be used in python (do not use uuids of
    ///          the form "abc123-def456").
    void addContainer(const std::string& container_path, const ContainerT* container_ptr, size_t capacity, const std::string& clk_name = "")
    {
        validatePath_(container_path);
        container_ = std::make_tuple(container_ptr, container_path, capacity, clk_name);
    }

    /// Get the name of this collection.
    std::string getName() const override
    {
        return name_;
    }

    /// \brief  Write metadata about this collection to the database.
    /// \throws Throws an exception if called more than once.
    void finalize(DatabaseManager* db_mgr, TreeNode* root) override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }

        serializeElementTree(db_mgr, root);

        auto record1 = db_mgr->INSERT(SQL_TABLE("Collections"),
                                      SQL_COLUMNS("Name", "DataType", "IsContainer"),
                                      SQL_VALUES(name_, meta_serializer_.getStructName(), 1));

        collection_pkey_ = record1->getId();
        auto struct_num_bytes = meta_serializer_.getStructNumBytes();

        auto record2 = db_mgr->INSERT(SQL_TABLE("CollectionElems"),
                                      SQL_COLUMNS("CollectionID", "SimPath"),
                                      SQL_VALUES(collection_pkey_, std::get<1>(container_)));

        auto path_id = record2->getId();
        auto capacity = std::get<2>(container_);
        auto is_sparse = Sparse ? 1 : 0;

        db_mgr->INSERT(SQL_TABLE("ContainerMeta"),
                       SQL_COLUMNS("PathID", "Capacity", "IsSparse"),
                       SQL_VALUES(path_id, capacity, is_sparse));

        meta_serializer_.writeMetadata(db_mgr, getName());
        blob_serializer_ = meta_serializer_.createBlobSerializer();
        finalized_ = true;
    }

    /// \brief  Collect all containers in this collection into separate data vectors (one blob
    ///         per container) and write them to the database.
    ///
    /// \throws Throws an exception if finalize() was not already called first.
    void collect(CollectionBuffer& buffer) override
    {
        if (!finalized_) {
            throw DBException("Cannot call collect() on a collection before calling finalize()");
        }

        auto sparse_array_type = std::integral_constant<bool, Sparse>{};
        collect_(buffer, sparse_array_type);
    }

    /// \brief Collect all containers in this collection (sparse version).
    void collect_(CollectionBuffer& buffer, std::true_type) {
        const ContainerT* container = std::get<0>(container_);
        auto itr = container->begin();
        auto eitr = container->end();
        uint16_t num_valid = 0;

        while (itr != eitr) {
            if (checkValid_(itr)) {
                ++num_valid;
            }
            ++itr;
        }

        if (num_valid == 0) {
            return;
        }

        buffer.writeHeader(collection_pkey_, num_valid);

        uint16_t bucket_idx = 0;
        itr = container->begin();
        while (itr != eitr) {
            if (checkValid_(itr)) {
                writeStruct_(*itr, buffer, bucket_idx);
            }
            ++itr;
            ++bucket_idx;
        }
    }

    /// \brief Collect all containers in this collection (non-sparse version).
    void collect_(CollectionBuffer& buffer, std::false_type) {
        const ContainerT* container = std::get<0>(container_);
        auto size = container->size();
        if (size == 0) {
            return;
        }

        buffer.writeHeader(collection_pkey_, size);
        auto itr = container->begin();
        auto eitr = container->end();
        while (itr != eitr) {
            writeStruct_(*itr, buffer, -1);
            ++itr;
        }
    }

private:
    template <typename container_t = ContainerT>
    typename std::enable_if<Sparse && is_std_vector<container_t>::value, bool>::type
    checkValid_(typename container_t::const_iterator itr)
    {
        return *itr != nullptr;
    }

    template <typename container_t = ContainerT>
    typename std::enable_if<Sparse && !is_std_vector<container_t>::value, bool>::type
    checkValid_(typename container_t::const_iterator itr)
    {
        return itr.isValid();
    }

    template <typename S=StructT>
    typename std::enable_if<MetaStruct::is_any_pointer<S>::value, StructT*>::type
    cloneStruct_(const StructT& s)
    {
        return cloneStruct_(*s);
    }

    template <typename S=StructT>
    typename std::enable_if<!MetaStruct::is_any_pointer<S>::value, StructT*>::type
    cloneStruct_(const StructT& s)
    {
        return new StructT(s);
    }

    template <typename ElemT>
    typename std::enable_if<MetaStruct::is_any_pointer<ElemT>::value, bool>::type
    writeStruct_(const ElemT& el, CollectionBuffer& buffer, uint16_t bucket_idx)
    {
        if (el) {
            return writeStruct_(*el, buffer, bucket_idx);
        }
        return false;
    }

    template <typename ElemT>
    typename std::enable_if<!MetaStruct::is_any_pointer<ElemT>::value, bool>::type
    writeStruct_(const ElemT& el, CollectionBuffer& buffer, uint16_t bucket_idx)
    {
        if (Sparse) {
            buffer.writeBytes(&bucket_idx, sizeof(uint16_t));
        }
        blob_serializer_->writeStruct(&el, buffer);
        return true;
    }

    /// Name of this collection. Serialized to the database.
    std::string name_;

    /// Container backpointer, its path, its capacity, and its clock name.
    std::tuple<const ContainerT*, std::string, size_t, std::string> container_;

    /// Our primary key in the Collections table.
    int collection_pkey_ = -1;

    /// Serializer to write all info about this struct to the DB.
    StructDefnSerializer<StructT> meta_serializer_;

    /// Serializer to pack all struct data into a blob.
    std::unique_ptr<StructBlobSerializer> blob_serializer_;
};

} // namespace simdb3
