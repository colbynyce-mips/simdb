/// Tests for SimDB collections feature (specifically containers e.g. vector of structs
/// of PODs, enums, and strings).

#include "simdb/collection/IterableStructs.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

#define INST_GROUP_CAPACITY 8
#define SPARSE_INST_GROUP_CAPACITY 16

enum class TargetUnit
{
    ALU0,
    ALU1,
    FPU,
    BR,
    LSU,
    ROB,
    __NUM_UNITS__
};

struct Instruction
{
    TargetUnit unit;
    uint64_t vaddr;
    std::string mnemonic;
};

namespace simdb
{
    template <>
    void defineStructSchema<Instruction>(StructSchema& schema)
    {
        schema.setStructName("Instruction");
        schema.addField<TargetUnit>("unit");
        schema.addField<uint64_t>("vaddr");
        schema.addField<std::string>("mnemonic");
    }

    template <>
    void defineEnumMap<TargetUnit>(std::string& enum_name, std::map<std::string, int32_t>& map)
    {
        enum_name = "TargetUnit";
        map["ALU0"] = static_cast<int32_t>(TargetUnit::ALU0);
        map["ALU1"] = static_cast<int32_t>(TargetUnit::ALU1);
        map["FPU"]  = static_cast<int32_t>(TargetUnit::FPU);
        map["BR"]   = static_cast<int32_t>(TargetUnit::BR);
        map["LSU"]  = static_cast<int32_t>(TargetUnit::LSU);
        map["ROB"]  = static_cast<int32_t>(TargetUnit::ROB);
    }

    template <>
    void writeStructFields(const Instruction* inst, StructFieldSerializer<Instruction>* serializer)
    {
        serializer->writeField(inst->unit);
        serializer->writeField(inst->vaddr);
        serializer->writeField(inst->mnemonic);
    }
}

using InstGroup = std::vector<std::shared_ptr<Instruction>>;
using InstGroupCollection = simdb::IterableStructCollection<InstGroup>;
using SparseInstGroupCollection = simdb::IterableStructCollection<InstGroup, true>;

void appendHeader(std::vector<char> &blob, uint16_t collection_id, uint16_t num_packets)
{
    blob.resize(blob.size() + 4);
    memcpy(&blob[blob.size() - 4], &collection_id, sizeof(uint16_t));
    memcpy(&blob[blob.size() - 2], &num_packets, sizeof(uint16_t));
}

void appendPacket(std::vector<char> &blob, const Instruction &inst)
{
    blob.resize(blob.size() + sizeof(int) + sizeof(uint64_t) + sizeof(uint32_t));
    //                        unit(enum)    vaddr(uint64_t)    mnemonic(string as uint32_t)

    int unit = static_cast<int>(inst.unit);
    auto dest = &blob[blob.size() - sizeof(int) - sizeof(uint64_t) - sizeof(uint32_t)];
    memcpy(dest, &unit, sizeof(int));
    dest += sizeof(int);

    uint64_t vaddr = inst.vaddr;
    memcpy(dest, &vaddr, sizeof(uint64_t));
    dest += sizeof(uint64_t);

    uint32_t string_id = simdb::StringMap::instance()->getStringId(inst.mnemonic);
    memcpy(dest, &string_id, sizeof(uint32_t));
}

void appendBucket(std::vector<char> &blob, uint16_t bucket_idx)
{
    blob.resize(blob.size() + sizeof(uint16_t));
    memcpy(&blob[blob.size() - sizeof(uint16_t)], &bucket_idx, sizeof(uint16_t));
}

void verifyBlob(const std::vector<char> &expected, uint32_t cycle, simdb::DatabaseManager &db_mgr)
{
    auto query = db_mgr.createQuery("CollectionData");
    query->addConstraintForInt("TimeVal", simdb::Constraints::EQUAL, cycle);

    std::vector<char> actual;
    query->select("DataVals", actual);

    auto result_set = query->getResultSet();
    EXPECT_TRUE(result_set.getNextRecord());
    EXPECT_EQUAL(expected, actual);
}

int main()
{
    DB_INIT;

    // Note that we only care about the collection data and have
    // no need for any other tables, aside from the tables that the
    // DatabaseManager adds automatically to support this feature.
    simdb::Schema schema;

    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    InstGroup contig;
    InstGroup sparse(SPARSE_INST_GROUP_CAPACITY);
    uint32_t cycle = 1;

    auto collection_mgr = db_mgr.getCollectionMgr();
    collection_mgr->useTimestampsFrom(&cycle);
    collection_mgr->setHeartbeat(5);

    std::unique_ptr<InstGroupCollection> contig_collection(new InstGroupCollection("ContigStructs"));
    contig_collection->addContainer("iterables.contig", &contig, INST_GROUP_CAPACITY);
    collection_mgr->addCollection(std::move(contig_collection));

    std::unique_ptr<SparseInstGroupCollection> sparse_collection(new SparseInstGroupCollection("SparseStructs"));
    sparse_collection->addContainer("iterables.sparse", &sparse, SPARSE_INST_GROUP_CAPACITY);
    collection_mgr->addCollection(std::move(sparse_collection));

    collection_mgr->setCompressionLevel(0);
    db_mgr.finalizeCollections();

    // Collection #1:
    //   Contig:
    //     1. ALU0, 0x1000, "add"
    //     2. ALU1, 0x2000, "sub"
    //   Sparse:
    //     1. ALU0, 0x3000, "mul"
    //     3. ALU1, 0x4000, "div"
    contig.push_back(std::make_shared<Instruction>(Instruction{TargetUnit::ALU0, 0x1000, "add"}));
    contig.push_back(std::make_shared<Instruction>(Instruction{TargetUnit::ALU1, 0x2000, "sub"}));
    sparse[0] = std::make_shared<Instruction>(Instruction{TargetUnit::ALU0, 0x3000, "mul"});
    sparse[2] = std::make_shared<Instruction>(Instruction{TargetUnit::ALU1, 0x4000, "div"});

    // Run 8 collection events without changing the data
    for (int i = 0; i < 8; ++i) {
        collection_mgr->collectAll();
        ++cycle;
    }

    // Now change the data by pushing another packet into the contig collection,
    // and inserting a new packet into the sparse collection.
    contig.push_back(std::make_shared<Instruction>(Instruction{TargetUnit::FPU, 0x5000, "sqrt"}));
    sparse[5] = std::make_shared<Instruction>(Instruction{TargetUnit::BR, 0x6000, "jmp"});

    // Run 8 more collection events with the new data
    for (int i = 0; i < 8; ++i) {
        collection_mgr->collectAll();
        ++cycle;
    }

    db_mgr.getConnection()->getTaskQueue()->stopThread();

    // Out of all 16 collection events, we should see:
    //   Cycle 1:  full data set #1
    //   Cycle 2:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 3:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 4:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 5:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 6:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 7:  full data set #1 <-------------------------- Even though the data is the same as cycle 1,
    //                                                          we should still see the full data set here
    //                                                          because we reached the heartbeat interval
    //                                                          where we force a DB write even if the data
    //                                                          hasn't changed.
    //   Cycle 8:  abbreviated data set (size==UINT16_MAX)
    //   Cycle 9:  full data set #2
    //   Cycle 10: abbreviated data set (size==UINT16_MAX)
    //   Cycle 11: abbreviated data set (size==UINT16_MAX)
    //   Cycle 12: abbreviated data set (size==UINT16_MAX)
    //   Cycle 13: abbreviated data set (size==UINT16_MAX)
    //   Cycle 14: abbreviated data set (size==UINT16_MAX)
    //   Cycle 15: full data set #2 <-------------------------- Even though the data is the same as cycle 9,
    //                                                          we should still see the full data set here
    //                                                          because we reached the heartbeat interval
    //                                                          where we force a DB write even if the data
    //                                                          hasn't changed.
    //   Cycle 16: abbreviated data set (size==UINT16_MAX)

    // Build up the expected blob for both collections for the first collectAll().
    // |---------------------------------------------------------------------------------------------------------------|
    // | 1 | 2 | ALU0 | 0x1000 | add | ALU1 | 0x2000 | sub | 2 | 2 | 0 | ALU0 | 0x3000 | mul | 2 | ALU1 | 0x4000 | div |
    // |---------------------------------------------------------------------------------------------------------------|
    //   ^   ^                                               ^   ^   ^                         ^
    //   |   |                                               |   |   |                         |
    //   |   |                                               |   |   sparse idx 0              sparse idx 2
    //   |   |                                               |   2 sparse packets
    //   |   |                                               sparse collection ID
    //   |   2 contig packets
    //   contig collection ID
    std::vector<char> full_data_set;
    appendHeader(full_data_set, 1, 2);
    appendPacket(full_data_set, *contig[0]);
    appendPacket(full_data_set, *contig[1]);
    appendHeader(full_data_set, 2, 2);
    appendBucket(full_data_set, 0);
    appendPacket(full_data_set, *sparse[0]);
    appendBucket(full_data_set, 2);
    appendPacket(full_data_set, *sparse[2]);

    // Run a query to get all collection data at cycle 1 and verify the full data set #1 was written.
    verifyBlob(full_data_set, 1, db_mgr);

    // Run a query to get all collection data at cycles 2 through 6 and verify the abbreviated data set was written.
    std::vector<char> abbreviated_data_set;
    appendHeader(abbreviated_data_set, 1, UINT16_MAX);
    appendHeader(abbreviated_data_set, 2, UINT16_MAX);

    verifyBlob(abbreviated_data_set, 2, db_mgr);
    verifyBlob(abbreviated_data_set, 3, db_mgr);
    verifyBlob(abbreviated_data_set, 4, db_mgr);
    verifyBlob(abbreviated_data_set, 5, db_mgr);
    verifyBlob(abbreviated_data_set, 6, db_mgr);

    // Run a query to get all collection data at cycle 7 and verify the full data set #1 was written.
    verifyBlob(full_data_set, 7, db_mgr);

    // Run a query to get all collection data at cycle 8 and verify the abbreviated data set was written.
    verifyBlob(abbreviated_data_set, 8, db_mgr);

    // Build up the expected blob (full data set #2) for both collections for the ninth collectAll().
    full_data_set.clear();
    appendHeader(full_data_set, 1, 3);
    appendPacket(full_data_set, *contig[0]);
    appendPacket(full_data_set, *contig[1]);
    appendPacket(full_data_set, *contig[2]);
    appendHeader(full_data_set, 2, 3);
    appendBucket(full_data_set, 0);
    appendPacket(full_data_set, *sparse[0]);
    appendBucket(full_data_set, 2);
    appendPacket(full_data_set, *sparse[2]);
    appendBucket(full_data_set, 5);
    appendPacket(full_data_set, *sparse[5]);

    // Run a query to get all collection data at cycle 9 and verify the full data set #2 was written.
    verifyBlob(full_data_set, 9, db_mgr);

    // Run a query to get all collection data at cycles 10 through 14 and verify the abbreviated data set was written.
    verifyBlob(abbreviated_data_set, 10, db_mgr);
    verifyBlob(abbreviated_data_set, 11, db_mgr);
    verifyBlob(abbreviated_data_set, 12, db_mgr);
    verifyBlob(abbreviated_data_set, 13, db_mgr);
    verifyBlob(abbreviated_data_set, 14, db_mgr);

    // Run a query to get all collection data at cycle 15 and verify the full data set #2 was written.
    verifyBlob(full_data_set, 15, db_mgr);

    // Run a query to get all collection data at cycle 16 and verify the abbreviated data set was written.
    verifyBlob(abbreviated_data_set, 16, db_mgr);

    db_mgr.closeDatabase();

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}
