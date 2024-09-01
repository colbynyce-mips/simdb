/// Tests for SimDB collections feature (specifically structs of PODs, enums, and strings).

#include "simdb/collection/Structs.hpp"
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

namespace simdb3
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
using InstCollection = simdb3::ScalarStructCollection<Instruction>;

Instruction* generateRandomInst()
{
    std::unique_ptr<Instruction> inst(new Instruction);
    inst->unit = static_cast<TargetUnit>(rand() % (int)TargetUnit::__NUM_UNITS__);
    inst->vaddr = rand();
    inst->mnemonic = rand() % 2 ? (rand() % 2 ? "MOV" : "SUB") : "ADD";
    return inst.release();
}

void populateInst(Instruction& inst)
{
    std::unique_ptr<Instruction> rnd(generateRandomInst());
    inst.unit = rnd->unit;
    inst.mnemonic = rnd->mnemonic;
    inst.vaddr = rnd->vaddr;
}

/// Custom smart pointer class. It is common in large simulators to use memory
/// pools with custom allocators / custom smart pointers for best performance.
/// Ensure this works.
template <typename T>
class CustomUniquePtr
{
public:
    CustomUniquePtr() = default;
    explicit CustomUniquePtr(T *ptr) : ptr_(ptr) {}
    ~CustomUniquePtr() { if (ptr_) delete ptr_; }

    T* operator->() { return ptr_; }
    const T* operator->() const { return ptr_; }

    T& operator*() { return *ptr_; }
    const T& operator*() const { return *ptr_; }

    T* get() { return ptr_; }
    const T* get() const { return ptr_; }

    void reset(T *ptr) {
        if (ptr_) delete ptr_;
        ptr_ = ptr;
    }

private:
    T* ptr_ = nullptr;
};

/*!
 * \class InstGroup
 *
 * \brief This class simply holds onto Instruction's as a raw pointer, another
 *        as a shared_ptr, another as a unique_ptr, and another as a custom
 *        smart pointer. We will collect all of them to ensure their functionality. 
 */
class InstGroup
{
public:
    InstGroup()
    {
        // Since the collection system will hold onto backpointers to our
        // data, we cannot reallocate the instructions after collection
        // is configured. 
        inst_raw_ = generateRandomInst();
        inst_shared_.reset(generateRandomInst());
        inst_unique_.reset(generateRandomInst());
        inst_custom_.reset(generateRandomInst());
    }

    ~InstGroup()
    {
        delete inst_raw_;
    }

    /// Add our structs to the collection.
    void configCollection(InstCollection* collection)
    {
        collection->addStruct("insts.foo", inst_raw_);
        collection->addStruct("insts.bar", inst_shared_.get());
        collection->addStruct("insts.fiz", inst_unique_.get());
        collection->addStruct("insts.fuz", inst_custom_.get());
    }

    /// Regnerate all of the Instruction member variables.
    void regenerateInstructions()
    {
        populateInst(*inst_raw_);
        populateInst(*inst_shared_);
        populateInst(*inst_unique_);
        populateInst(*inst_custom_);
    }

private:
    Instruction* inst_raw_ = nullptr;
    std::shared_ptr<Instruction> inst_shared_;
    std::unique_ptr<Instruction> inst_unique_;
    CustomUniquePtr<Instruction> inst_custom_;
};

/// Example simulator that configures a collection of various structs.
class Sim
{
public:
    Sim(simdb3::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
    }

    void runSimulation()
    {
        configCollection_();

        while (time_++ < 1000) {
            inst_group_.regenerateInstructions();
            db_mgr_->getCollectionMgr()->collectAll();
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollection_()
    {
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<InstCollection> inst_collection(new InstCollection("Structs"));
        inst_group_.configCollection(inst_collection.get());
        collection_mgr->addCollection(std::move(inst_collection));

        db_mgr_->finalizeCollections();
    }

    simdb3::DatabaseManager* db_mgr_;
    InstGroup inst_group_;
    uint64_t time_ = 0;
};

int main()
{
    DB_INIT;

    // Note that we only care about the constellation data and have
    // no need for any other tables, aside from the tables that the
    // DatabaseManager adds automatically to support this feature.
    simdb3::Schema schema;

    simdb3::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    Sim sim(&db_mgr);
    sim.runSimulation();
    db_mgr.closeDatabase();

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}
