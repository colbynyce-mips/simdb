/// Tests for SimDB collections feature (specifically structs of PODs, enums, and strings).

#include "simdb/collection/Any.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

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

/// Note that when collecting anything like shared_ptr's or raw pointers, you should use
/// the value type in the ScalarStructCollection<DataT> signature.
using InstCollection = simdb::AnyCollection<Instruction>;

std::unique_ptr<Instruction> generateRandomInst()
{
    std::unique_ptr<Instruction> inst(new Instruction);
    inst->unit = static_cast<TargetUnit>(rand() % (int)TargetUnit::__NUM_UNITS__);
    inst->vaddr = rand();
    inst->mnemonic = rand() % 2 ? (rand() % 2 ? "MOV" : "SUB") : "ADD";
    return inst;
}

/// Example simulator that configures a collection of structs and scalars.
/// This is different than the other collection tests in that we do not give
/// the collector any backpointer / function pointer to our data. We directly
/// serialize anything given to our collect() / collectWithDuration() methods
/// as a byte vector.
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
            collectInt_();
            collectInstruction_();
            db_mgr_->getCollectionMgr()->collectAll();
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollection_()
    {
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->useTimestampsFrom(&time_);

        using InstCollection = simdb::AnyCollection<Instruction>;
        std::unique_ptr<InstCollection> inst_collection(new InstCollection("InstAsBlob"));
        inst_collector_ = inst_collection.get();
        collection_mgr->addCollection(std::move(inst_collection));

        using IntCollection = simdb::AnyCollection<int>;
        std::unique_ptr<IntCollection> int_collection(new IntCollection("IntAsBlob"));
        int_collector_ = int_collection.get();
        collection_mgr->addCollection(std::move(int_collection));

        db_mgr_->finalizeCollections();
    }

    void collectInt_()
    {
        switch (time_ % 10) {
            case 3: {
                auto val = rand() % 10;
                int_collector_->collect(val);
                break;
            }
            case 5: {
                auto val = rand() % 10;
                int_collector_->collectWithDuration(val, 3);
                break;
            }
        }
    }

    void collectInstruction_()
    {
        switch (time_ % 10) {
            case 3: {
                auto inst = generateRandomInst();
                inst_collector_->collect(*inst);
                break;
            }
            case 5: {
                auto inst = generateRandomInst();
                inst_collector_->collectWithDuration(*inst, 3);
                break;
            }
        }
    }

    simdb::DatabaseManager* db_mgr_;
    simdb::AnyCollection<Instruction>* inst_collector_ = nullptr;
    simdb::AnyCollection<int>* int_collector_ = nullptr;
    uint64_t time_ = 0;
};

int main()
{
    DB_INIT;

    // Note that we only care about the constellation data and have
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
