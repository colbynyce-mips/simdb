// <CollectionMgr> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"

namespace simdb
{

class DatabaseManager;
class SQLiteTransaction;

/*!
 * \class CollectionMgr
 *
 * \brief This class provides an easy way to handle simulation-wide data collection.
 */
class CollectionMgr
{
public:
    /// Construct with the DatabaseManager and SQLiteTransaction.
    CollectionMgr(DatabaseManager* db_mgr, SQLiteTransaction* db_conn)
        : db_mgr_(db_mgr)
        , db_conn_(db_conn)
    {
    }

    /// Set the heartbeat for all collections. This is the max number of cycles
    /// that we employ the optimization "only write to the database if the collected
    /// data is different from the last collected data". This prevents Argos from
    /// having to go back more than N cycles to find the last known value.
    void setHeartbeat(size_t heartbeat)
    {
        pipeline_heartbeat_ = heartbeat;
    }

    /// Get the heartbeat for all collections.
    size_t getHeartbeat() const
    {
        return pipeline_heartbeat_;
    }

    /// Set the compression level for all collections. This is the zlib compression
    /// level, where 0 is no compression, 1 is fastest, and 9 is best compression.
    void setCompressionLevel(int level)
    {
        compression_level_ = level;
    }

    /// Populate the schema with the appropriate tables for all the
    /// collections.
    void defineSchema(Schema& schema) const
    {
        using dt = SqlDataType;

        schema.addTable("Clocks")
            .addColumn("Name", dt::string_t)
            .addColumn("Period", dt::int32_t);

        schema.addTable("ElementTreeNodes")
            .addColumn("Name", dt::string_t)
            .addColumn("ParentID", dt::int32_t);

        schema.addTable("CollectableTreeNodes")
            .addColumn("ElementTreeNodeID", dt::int32_t)
            .addColumn("ClockID", dt::int32_t)
            // DataType could be:
            //    int16_t                          <-- Collectable<int16_t>
            //    double                           <-- Collectable<double>
            //    ExampleInst                      <-- Collectable<ExampleInst>
            //    ExampleInst_sparse_capacity64    <-- IterableCollector<ExampleInst, SchedulingPhase::Collection, true>
            //    ExampleInst_contig_capacity8     <-- IterableCollector<ExampleInst, SchedulingPhase::Collection, false>
            .addColumn("DataType", dt::string_t);

        schema.addTable("StructFields")
            // This table is populated when non-POD types are encountered at the
            // collectable leaves. For example, if a Collectable<ExampleInst> is
            // encountered, numerous entries will be added to this table that all
            // have the StructName "ExampleInst". Each entry will represent a field
            // in the ExampleInst struct, and the order of the entries will be the
            // order in which the fields were added to the Collectable<ExampleInst>.
            .addColumn("StructName", dt::string_t)
            .addColumn("FieldName", dt::string_t)
            .addColumn("FieldType", dt::string_t)
            .addColumn("FormatCode", dt::int32_t)
            .addColumn("IsAutoColorizeKey", dt::int32_t)
            .addColumn("IsDisplayedByDefault", dt::int32_t)
            .setColumnDefaultValue("IsAutoColorizeKey", 0)
            .setColumnDefaultValue("IsDisplayedByDefault", 1);
    }

private:
    /// DatabaseManager. Needed so we can call finalize() and collect() on the
    /// CollectionBase objects.
    DatabaseManager* db_mgr_;

    /// SQLiteTransaction. Needed so we can put synchronously serialized collections
    /// inside BEGIN/COMMIT TRANSACTION calls for best performance.
    SQLiteTransaction* db_conn_;

    /// The max number of cycles that we employ the optimization "only write to the
    /// database if the collected data is different from the last collected data".
    /// This prevents Argos from having to go back more than N cycles to find the
    /// last known value.
    size_t pipeline_heartbeat_ = 10;

    /// Compression level. This starts out as the default compromise between speed and compression,
    /// and will gradually move towards fastest compression if the worker thread is falling behind.
    /// Note that the levels are 0-9, where 0 is no compression, 1 is fastest, and 9 is best compression.
    /// We currently do not go all the way to zero compression or the database will be too large.
    int compression_level_ = 6;

    /// Keep track of the "highwater mark" representing the number of tasks in the queue at
    /// the time of each collection. Start with a highwater mark of 5 so we do not inadvertently
    /// lower the compression level too soon. We want to give the worker thread a chance to catch up.
    size_t num_tasks_highwater_mark_ = 5;

    /// Keep track of how many times the highwater mark is exceeded. When it reaches 3, we will
    /// decrement the compression level to make it go faster and reset this count back to 0.
    size_t num_times_highwater_mark_exceeded_ = 0;

    friend class DatabaseManager;
};

} // namespace simdb
