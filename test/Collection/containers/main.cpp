/*
 \brief Tests for SimDB constellations feature (collecting groups of stats
 *      from all over a simulator). Unlike the test in the 'stats' directory,
 *      this tests for more general-purpose collection of commonly found data
 *      structures in simulators:
 * 
 *        struct
 *        shared_ptr<struct>
 *        string
 *        enum
 *
 *      And vectors of the above. One powerful use is in CPU/GPU modeling,
 *      where a struct like this:
 * 
 *        struct Instruction {
 *            TargetUnit unit;      // enum e.g. ALU0, ALU1, LSU, etc.
 *            uint64_t vaddr;       // virtual address formatted as hex
 *            std::string mnemonic; // e.g. "add", "sub", "fabs"
 *        };
 * 
 *      Is held in variable-size FIFO containers:
 * 
 *        using InstPtr = std::shared_ptr<Inst>;
 *        using InstQueue = std::vector<InstPtr>;
 * 
 *      SimDB supports vector and deque out of the box, as these are most
 *      commonly used in industry for this use case. It also supports sparse 
 *      data, where the python constellations module will return None for the 
 *      bins where a nullptr was encountered:
 * 
 *        [
 *            {'unit':'LSU', 'vaddr':'0x1941d4c2ca1ccb56', 'mnemonic':'ADD'},
 *            None,
 *            {'unit':'ROB', 'vaddr':'0x340fa0cb0e22efbd', 'mnemonic':'MOV'}
 *        ]
 */

#include "simdb/constellations/StructConstellation.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

#define DECODE_CAPACITY 4
#define DISPATCH_CAPACITY 4

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

using InstructionPtr = std::shared_ptr<Instruction>;
using InstructionQueue = std::vector<InstructionPtr>;
using InstructionDeque = std::deque<InstructionPtr>;

using InstConstellation = simdb::Constellation<InstructionPtr, simdb::CompressionModes::COMPRESSED>;
using InstQueueConstellation = simdb::Constellation<InstructionQueue, simdb::CompressionModes::COMPRESSED>;
using InstDequeConstellation = simdb::Constellation<InstructionDeque, simdb::CompressionModes::COMPRESSED>;

InstructionPtr generateRandomInst()
{
    auto inst = std::make_shared<Instruction>();
    inst->unit = static_cast<TargetUnit>(rand() % (int)TargetUnit::__NUM_UNITS__);
    inst->vaddr = rand();
    inst->mnemonic = rand() % 2 ? (rand() % 2 ? "MOV" : "SUB") : "ADD";
    return inst;
}

/// Fetch->Decode->Dispatch
/// *****
/// This class will hold onto just one InstructionPtr (sent out one at a time).
///
class Fetch
{
public:
    /// Create and hold onto a new instruction
    void fetch()
    {
        inst_ = generateRandomInst();
    }

    /// Release the instruction to be sent to the Decode unit
    InstructionPtr release()
    {
        auto inst = inst_;
        fetch();
        return inst;
    }

private:
    InstructionPtr inst_;
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
