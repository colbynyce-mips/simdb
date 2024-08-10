/*
 \brief Tests for SimDB constellations feature (collecting groups of stats
 *      from all over a simulator). Unlike the test in the 'stats' directory,
 *      this tests for more general-purpose collection of structs with fields
 *      that are PODs, enums, and strings. We also test that we can collect
 *      smart pointers (or raw pointers) of these structs.
 */

#include "simdb/constellations/StructConstellation.hpp"
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

/// Note that when collecting anything like shared_ptr's or raw pointers, you should use
/// the value type in the Constellation<DataT> signature.
using InstConstellation = simdb::Constellation<Instruction, simdb::CompressionModes::COMPRESSED>;

InstructionPtr generateRandomInst()
{
    auto inst = std::make_shared<Instruction>();
    inst->unit = static_cast<TargetUnit>(rand() % (int)TargetUnit::__NUM_UNITS__);
    inst->vaddr = rand();
    inst->mnemonic = rand() % 2 ? (rand() % 2 ? "MOV" : "SUB") : "ADD";
    return inst;
}

/// Custom smart pointer class. It is common in large simulators to use memory
/// pools with custom allocators / custom smart pointers for best performance.
/// Ensure this works.
template <typename T>
class CustomUniquePtr
{
public:
    explicit CustomUniquePtr(T *ptr) : ptr_(ptr) {}
    ~CustomUniquePtr() { delete ptr_; }

    T* operator->() { return ptr_; }
    const T* operator->() const { return ptr_; }

    T& operator*() { return *ptr_; }
    const T& operator*() const { return *ptr_; }

    T* get() { return ptr_; }
    const T* get() const { return ptr_; }

private:
    T* ptr_;
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
    /// Regnerate all of the Instruction member variables.
    

private:
    Instruction* inst_raw_ = nullptr;
    std::shared_ptr<Instruction> inst_shared_;
    std::unique_ptr<Instruction> inst_unique_;
    CustomUniquePtr<Instruction> inst_custom_;
};

/// Fetch->Decode->Dispatch
///        ******
/// This class will hold onto instructions in a vector.
///
class Decode
{
public:
    Decode(uint32_t capacity = DECODE_CAPACITY)
    {
        insts_.reserve(capacity);
    }

    /// Receive an instruction from the Fetch unit
    void receive(InstructionPtr inst)
    {
        assert(insts_.size() != insts_.capacity());
        insts_.push_back(inst);
    }

    /// Release the oldest instruction to be sent to the Dispatch unit
    InstructionPtr release()
    {
        assert(!insts_.empty());
        auto inst = insts_[0];
        insts_.erase(insts_.begin());
        return inst;
    }

private:
    InstructionQueue insts_;
};

/// Fetch->Decode->Dispatch
///                ********
/// This class will hold onto instructions in a deque.
///
class Dispatch
{
public:
    Dispatch(uint32_t capacity = DISPATCH_CAPACITY)
        : capacity_(capacity)
    {
    }

    /// Receive an instruction from the Fetch unit
    void receive(InstructionPtr inst)
    {
        assert(insts_.size() != capacity_);
        insts_.push_back(inst);
    }

    /// Release the oldest instruction
    InstructionPtr release()
    {
        assert(!insts_.empty());
        auto inst = insts_[0];
        insts_.erase(insts_.begin());
        return inst;
    }

private:
    const uint32_t capacity_;
    InstructionDeque insts_;
};

/// Example simulator with Fetch->Decode->Dispatch units. We will configure constellations
/// for all units.
class Sim
{
public:
    Sim(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
    }

    void runSimulation()
    {
        configConstellations_();

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configConstellations_()
    {
        auto constellation_mgr = db_mgr_->getConstellationMgr();
        constellation_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<InstConstellation> inst_constellation(new InstConstellation("FetchInstruction"));


#if 0
        ctr_constellation->addStat("stats.num_insts_issued", std::bind(&Execute::getNumIssued, &execute_));
        ctr_constellation->addStat("stats.num_insts_retired", std::bind(&Retire::getNumRetired, &retire_));

        constellation_mgr->addConstellation(std::move(ctr_constellation));

        addConstellation_<int8_t>(rand_int8s_, "stats.rand_int8s.bin", "RandInt8s", constellation_mgr);
        addConstellation_<int16_t>(rand_int16s_, "stats.rand_int16s.bin", "RandInt16s", constellation_mgr);
        addConstellation_<int32_t>(rand_int32s_, "stats.rand_int32s.bin", "RandInt32s", constellation_mgr);
        addConstellation_<int64_t>(rand_int64s_, "stats.rand_int64s.bin", "RandInt64s", constellation_mgr);
        addConstellation_<uint8_t>(rand_uint8s_, "stats.rand_uint8s.bin", "RandUInt8s", constellation_mgr);
        addConstellation_<uint16_t>(rand_uint16s_, "stats.rand_uint16s.bin", "RandUInt16s", constellation_mgr);
        addConstellation_<uint32_t>(rand_uint32s_, "stats.rand_uint32s.bin", "RandUInt32s", constellation_mgr);
        addConstellation_<uint64_t>(rand_uint64s_, "stats.rand_uint64s.bin", "RandUInt64s", constellation_mgr);
        addConstellation_<float>(rand_floats_, "stats.rand_floats.bin", "RandFloats", constellation_mgr);
        addConstellation_<double>(rand_doubles_, "stats.rand_doubles.bin", "RandDoubles", constellation_mgr);
#endif
        db_mgr_->finalizeConstellations();
    }

    simdb::DatabaseManager* db_mgr_;
    Fetch fetch_;
    Decode decode_;
    Dispatch dispatch_;
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
