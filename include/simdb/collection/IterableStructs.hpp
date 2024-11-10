// <IterableStructs> -*- C++ -*-

#pragma once

#include "simdb/collection/Structs.hpp"

namespace simdb
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
    using StructT = utils::remove_any_pointer_t<StructPtrT>;

    /// Construct with a name for this collection.
    IterableStructCollection(const std::string& name)
        : name_(name)
    {
        static_assert(utils::is_any_pointer<StructPtrT>::value,
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

    /// Get if the given element path ("root.child1.child2") is in this collection.
    bool hasElement(const std::string& element_path) const override
    {
        return element_path == std::get<1>(container_);
    }

    /// Get the element offset in the collection. This is for collections where we
    /// pack all stats of the same data type into the same collection buffer, specifically
    /// StatCollection<T> and ScalarStructCollection<T>.
    int getElementOffset(const std::string& element_path) const override
    {
        (void)element_path;
        return -1;
    }

    /// Get the type of widget that should be displayed when the given element
    /// is dragged-and-dropped onto the Argos widget canvas.
    std::string getWidgetType(const std::string& element_path) const override
    {
        if (hasElement(element_path)) {
            return "QueueTable";
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
                                     SQL_VALUES(name_, meta_serializer_.getStructName(), 1, Sparse ? 1 : 0, std::get<2>(container_)));

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
    void onPipelineCollectorClosing(DatabaseManager* db_mgr) override
    {
        if (max_container_size_ > 0) {
            db_mgr->INSERT(SQL_TABLE("QueueMaxSizes"),
                           SQL_COLUMNS("CollectionID", "MaxSize"),
                           SQL_VALUES(collection_pkey_, max_container_size_));
        }
    }

    /// Set the heartbeat for this collection. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    void setHeartbeat(const size_t heartbeat) override
    {
        if (heartbeat == 0) {
            throw DBException("Heartbeat must be greater than zero");
        }
        pipeline_heartbeat_ = heartbeat;
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

        max_container_size_ = std::max(max_container_size_, (size_t)size);
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
    typename std::enable_if<utils::is_any_pointer<S>::value, StructT*>::type
    cloneStruct_(const StructT& s)
    {
        return cloneStruct_(*s);
    }

    template <typename S=StructT>
    typename std::enable_if<!utils::is_any_pointer<S>::value, StructT*>::type
    cloneStruct_(const StructT& s)
    {
        return new StructT(s);
    }

    template <typename ElemT>
    typename std::enable_if<utils::is_any_pointer<ElemT>::value, bool>::type
    writeStruct_(const ElemT& el, CollectionBuffer& buffer, uint16_t bucket_idx)
    {
        if (el) {
            return writeStruct_(*el, buffer, bucket_idx);
        }
        return false;
    }

    template <typename ElemT>
    typename std::enable_if<!utils::is_any_pointer<ElemT>::value, bool>::type
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
    size_t pipeline_heartbeat_ = 10;

    /// Number of times we have carried forward the current container data without
    /// writing it to the database. This is used together with the heartbeat to 
    /// determine when to force a write to the database.
    size_t num_carry_forward_unchanged_ = 0;

    /// Keep track of the max number of elements seen in the container. This is
    /// used to support Argos UI performance for the Scheduling Lines feature.
    size_t max_container_size_ = 0;
};

} // namespace simdb
