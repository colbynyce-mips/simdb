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

std::shared_ptr<Instruction> generateRandomInst()
{
    auto inst = std::make_shared<Instruction>();
    inst->unit = static_cast<TargetUnit>(rand() % (int)TargetUnit::__NUM_UNITS__);
    inst->vaddr = rand();
    inst->mnemonic = rand() % 2 ? (rand() % 2 ? "MOV" : "SUB") : "ADD";
    return inst;
}

/// Example simulator that configures a collection of Instruction vectors, both
/// sparse and non-sparse.
class Sim
{
public:
    Sim(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
    }

    void runSimulation()
    {
        configCollection_();

        while (time_++ < 1000) {
            generateRandomInstGroups_();
            db_mgr_->getCollectionMgr()->collectAll();
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollection_()
    {
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<InstGroupCollection> inst_collection(new InstGroupCollection("ContigStructs"));
        inst_collection->addContainer("iterables.contig", &insts_, INST_GROUP_CAPACITY);
        collection_mgr->addCollection(std::move(inst_collection));

        std::unique_ptr<SparseInstGroupCollection> sparse_inst_collection(new SparseInstGroupCollection("SparseStructs"));
        sparse_inst_collection->addContainer("iterables.sparse", &sparse_insts_, SPARSE_INST_GROUP_CAPACITY);
        collection_mgr->addCollection(std::move(sparse_inst_collection));

        db_mgr_->finalizeCollections();
    }

    void generateRandomInstGroups_()
    {
        insts_.clear();
        auto num_contig_insts = rand() % INST_GROUP_CAPACITY;
        for (size_t idx = 0; idx < num_contig_insts; ++idx) {
            insts_.push_back(generateRandomInst());
        }

        sparse_insts_.clear();
        sparse_insts_.resize(SPARSE_INST_GROUP_CAPACITY);
        for (size_t idx = 0; idx < SPARSE_INST_GROUP_CAPACITY; ++idx) {
            if (rand() % 3 == 0) {
                sparse_insts_[idx] = generateRandomInst();
            }
        }
    }

    simdb::DatabaseManager* db_mgr_;
    uint64_t time_ = 0;
    InstGroup insts_;
    InstGroup sparse_insts_;
};

int main()
{
    DB_INIT;

    // Note that we only care about the collection data and have
    // no need for any other tables, aside from the tables that the
    // DatabaseManager adds automatically to support this feature.
    simdb::Schema schema;

    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    Sim sim(&db_mgr);
    sim.runSimulation();
    db_mgr.closeDatabase();

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}
