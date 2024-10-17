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
    void finalize(DatabaseManager* db_mgr, TreeNode* root, size_t heartbeat) override
    {
        if (finalized_) {
            throw DBException("Cannot call finalize() on a collection more than once");
        }

        serializeElementTree(db_mgr, root);

        auto record1 = db_mgr->INSERT(SQL_TABLE("Collections"),
                                      SQL_COLUMNS("Name", "DataType", "IsContainer"),
                                      SQL_VALUES(name_, meta_serializer_.getStructName(), 1));

        collection_pkey_ = record1->getId();

        auto record2 = db_mgr->INSERT(SQL_TABLE("CollectionElems"),
                                      SQL_COLUMNS("CollectionID", "ElemPath"),
                                      SQL_VALUES(collection_pkey_, std::get<1>(container_)));

        auto elem_id = record2->getId();
        auto capacity = std::get<2>(container_);
        auto is_sparse = Sparse ? 1 : 0;

        db_mgr->INSERT(SQL_TABLE("ContainerMeta"),
                       SQL_COLUMNS("CollectionElemID", "Capacity", "IsSparse"),
                       SQL_VALUES(elem_id, capacity, is_sparse));

        meta_serializer_.writeMetadata(db_mgr, record1->getId());
        blob_serializer_ = meta_serializer_.createBlobSerializer();
        pipeline_heartbeat_ = heartbeat;
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

private:
    /// \brief Collect all containers in this collection (sparse version).
    void collect_(CollectionBuffer& buffer, std::true_type) {
        const ContainerT* container = std::get<0>(container_);
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

        CollectionBuffer current_container_buffer(current_container_data_);
        current_container_buffer.writeHeader(collection_pkey_, num_valid);

        uint16_t bucket_idx = 0;
        auto itr = container->begin();
        auto eitr = container->end();
        while (itr != eitr) {
            if (checkValid_(itr)) {
                writeStruct_(*itr, current_container_buffer, bucket_idx);
            }
            ++itr;
            ++bucket_idx;
        }

        if (current_container_data_ != prev_container_data_ || num_carry_forward_unchanged_ == pipeline_heartbeat_) {
            buffer.writeBytes(current_container_data_.data(), current_container_data_.size());
            prev_container_data_ = current_container_data_;
            num_carry_forward_unchanged_ = 0;
        } else {
            // We use UINT16_MAX to denote that the container data is unchanged.
            buffer.writeHeader(collection_pkey_, UINT16_MAX);
            ++num_carry_forward_unchanged_;
        }
    }

    /// \brief Collect all containers in this collection (non-sparse version).
    void collect_(CollectionBuffer& buffer, std::false_type) {
        const ContainerT* container = std::get<0>(container_);
        auto size = container->size();

        CollectionBuffer current_container_buffer(current_container_data_);
        current_container_buffer.writeHeader(collection_pkey_, size);

        auto itr = container->begin();
        auto eitr = container->end();
        size_t bucket_idx = 0;

        while (itr != eitr) {
            writeStruct_(*itr, current_container_buffer, bucket_idx);
            ++itr;
            ++bucket_idx;
        }

        if (current_container_data_ != prev_container_data_ || num_carry_forward_unchanged_ == pipeline_heartbeat_) {
            buffer.writeBytes(current_container_data_.data(), current_container_data_.size());
            prev_container_data_ = current_container_data_;
            num_carry_forward_unchanged_ = 0;
        } else {
            // We use UINT16_MAX to denote that the container data is unchanged.
            buffer.writeHeader(collection_pkey_, UINT16_MAX);
            ++num_carry_forward_unchanged_;
        }
    }

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
            buffer.writeBucket(bucket_idx);
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

    /// Previous container data. This is used so we can diff the current container
    /// data with the previous container data and only write to the database if they
    /// are different. Note that even if they are the same, we still periodically
    /// force a write to the database to prevent Argos from having to go back more
    /// than N cycles to find the last known value.
    std::vector<char> prev_container_data_;

    /// Hold onto a reusable data vector to avoid reallocating it every time.
    std::vector<char> current_container_data_;

    /// The heartbeat interval for this collection. This is the max number of collection
    /// cycles that can pass before we force a write to the database, even if the data
    /// hasn't changed.
    size_t pipeline_heartbeat_ = 5;

    /// Number of times we have carried forward the current container data without
    /// writing it to the database. This is used together with the heartbeat to 
    /// determine when to force a write to the database.
    size_t num_carry_forward_unchanged_ = 0;
};

} // namespace simdb3
