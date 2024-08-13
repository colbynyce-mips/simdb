/// Tests for SimDB collections feature. This test brings everything together
/// and produces collections of scalar stats, structs, iterable structs (both
/// sparse and non-sparse), and non-default format options.
///
/// This test is meant to drive the python module deserializers / reports.
/// All supported data types are tested here.

#include "simdb/collection/IterableStructs.hpp"
#include "simdb/collection/Structs.hpp"
#include "simdb/collection/Scalars.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <random>

TEST_INIT;

std::random_device rd;  // a seed source for the random number engine
std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()

template <typename T>
T generateRandomInt()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    std::uniform_int_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

template <typename T>
T generateRandomReal()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    std::uniform_real_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

char generateRandomChar()
{
    return 'A' + rand() % 26;
}

std::string generateRandomString()
{
    std::string str;
    for (size_t idx = 0; idx < rand() % 3 + 3; ++idx) {
        str += generateRandomChar();
    }
    return str;
}

template <typename T>
struct minval { static constexpr auto value = std::numeric_limits<T>::min(); };

template <typename T>
struct maxval { static constexpr auto value = std::numeric_limits<T>::max(); };

enum class EnumInt8   : int8_t   { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, MINVAL=minval<int8_t>::value, __COUNT__ };
enum class EnumInt16  : int16_t  { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, MINVAL=minval<int8_t>::value, __COUNT__ };
enum class EnumInt32  : int32_t  { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, MINVAL=minval<int8_t>::value, __COUNT__ };
enum class EnumInt64  : int64_t  { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, MINVAL=minval<int8_t>::value, __COUNT__ };
enum class EnumUInt8  : uint8_t  { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, __COUNT__ };
enum class EnumUInt16 : uint16_t { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, __COUNT__ };
enum class EnumUInt32 : uint32_t { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, __COUNT__ };
enum class EnumUInt64 : uint64_t { ONE=1, TWO=2, MAXVAL=maxval<int8_t>::value, __COUNT__ };

struct AllTypes
{
    EnumInt8    e_int8;
    EnumInt16   e_int16;
    EnumInt32   e_int32;
    EnumInt64   e_int64;
    EnumUInt8   e_uint8;
    EnumUInt16  e_uint16;
    EnumUInt32  e_uint32;
    EnumUInt64  e_uint64;

    char        ch;
    int8_t      int8;
    int16_t     int16;
    int32_t     int32;
    int64_t     int64;
    uint8_t     uint8;
    uint16_t    uint16;
    uint32_t    uint32;
    uint64_t    uint64;
    float       flt;
    double      dbl;
    std::string str;

    int8_t      int8_hex;
    int16_t     int16_hex;
    int32_t     int32_hex;
    int64_t     int64_hex;
    uint8_t     uint8_hex;
    uint16_t    uint16_hex;
    uint32_t    uint32_hex;
    uint64_t    uint64_hex;
};

namespace simdb
{
    template <>
    void defineStructSchema<AllTypes>(StructSchema& schema)
    {
        schema.setStructName("AllTypes");
        schema.addField<EnumInt8>("e_int8");
        schema.addField<EnumInt16>("e_int16");
        schema.addField<EnumInt32>("e_int32");
        schema.addField<EnumInt64>("e_int64");
        schema.addField<EnumUInt8>("e_uint8");
        schema.addField<EnumUInt16>("e_uint16");
        schema.addField<EnumUInt32>("e_uint32");
        schema.addField<EnumUInt64>("e_uint64");

        schema.addField<char>("ch");
        schema.addField<int8_t>("int8");
        schema.addField<int16_t>("int16");
        schema.addField<int32_t>("int32");
        schema.addField<int64_t>("int64");
        schema.addField<uint8_t>("uint8");
        schema.addField<uint16_t>("uint16");
        schema.addField<uint32_t>("uint32");
        schema.addField<uint64_t>("uint64");
        schema.addField<float>("flt");
        schema.addField<double>("dbl");
        schema.addField<std::string>("str");

        schema.addField<int8_t>("int8_hex", Format::hex);
        schema.addField<int16_t>("int16_hex", Format::hex);
        schema.addField<int32_t>("int32_hex", Format::hex);
        schema.addField<int64_t>("int64_hex", Format::hex);
        schema.addField<uint8_t>("uint8_hex", Format::hex);
        schema.addField<uint16_t>("uint16_hex", Format::hex);
        schema.addField<uint32_t>("uint32_hex", Format::hex);
        schema.addField<uint64_t>("uint64_hex", Format::hex);
    }

    template <>
    void defineEnumMap<EnumInt8>(std::string& enum_name, std::map<std::string, int8_t>& map)
    {
        enum_name = "EnumInt8";
        map["ONE"] = static_cast<int8_t>(EnumInt8::ONE);
        map["TWO"] = static_cast<int8_t>(EnumInt8::TWO);
        map["MAXVAL"] = static_cast<int8_t>(EnumInt8::MAXVAL);
        map["MINVAL"] = static_cast<int8_t>(EnumInt8::MINVAL);
    }

    template <>
    void defineEnumMap<EnumInt16>(std::string& enum_name, std::map<std::string, int16_t>& map)
    {
        enum_name = "EnumInt16";
        map["ONE"] = static_cast<int16_t>(EnumInt16::ONE);
        map["TWO"] = static_cast<int16_t>(EnumInt16::TWO);
        map["MAXVAL"] = static_cast<int16_t>(EnumInt16::MAXVAL);
        map["MINVAL"] = static_cast<int16_t>(EnumInt16::MINVAL);
    }

    template <>
    void defineEnumMap<EnumInt32>(std::string& enum_name, std::map<std::string, int32_t>& map)
    {
        enum_name = "EnumInt32";
        map["ONE"] = static_cast<int32_t>(EnumInt32::ONE);
        map["TWO"] = static_cast<int32_t>(EnumInt32::TWO);
        map["MAXVAL"] = static_cast<int32_t>(EnumInt32::MAXVAL);
        map["MINVAL"] = static_cast<int32_t>(EnumInt32::MINVAL);
    }

    template <>
    void defineEnumMap<EnumInt64>(std::string& enum_name, std::map<std::string, int64_t>& map)
    {
        enum_name = "EnumInt64";
        map["ONE"] = static_cast<int64_t>(EnumInt64::ONE);
        map["TWO"] = static_cast<int64_t>(EnumInt64::TWO);
        map["MAXVAL"] = static_cast<int64_t>(EnumInt64::MAXVAL);
        map["MINVAL"] = static_cast<int64_t>(EnumInt64::MINVAL);
    }

    template <>
    void defineEnumMap<EnumUInt8>(std::string& enum_name, std::map<std::string, uint8_t>& map)
    {
        enum_name = "EnumInt8";
        map["ONE"] = static_cast<int8_t>(EnumUInt8::ONE);
        map["TWO"] = static_cast<int8_t>(EnumUInt8::TWO);
        map["MAXVAL"] = static_cast<int8_t>(EnumUInt8::MAXVAL);
    }

    template <>
    void defineEnumMap<EnumUInt16>(std::string& enum_name, std::map<std::string, uint16_t>& map)
    {
        enum_name = "EnumInt16";
        map["ONE"] = static_cast<int16_t>(EnumUInt16::ONE);
        map["TWO"] = static_cast<int16_t>(EnumUInt16::TWO);
        map["MAXVAL"] = static_cast<int16_t>(EnumUInt16::MAXVAL);
    }

    template <>
    void defineEnumMap<EnumUInt32>(std::string& enum_name, std::map<std::string, uint32_t>& map)
    {
        enum_name = "EnumInt32";
        map["ONE"] = static_cast<int32_t>(EnumUInt32::ONE);
        map["TWO"] = static_cast<int32_t>(EnumUInt32::TWO);
        map["MAXVAL"] = static_cast<int32_t>(EnumUInt32::MAXVAL);
    }

    template <>
    void defineEnumMap<EnumUInt64>(std::string& enum_name, std::map<std::string, uint64_t>& map)
    {
        enum_name = "EnumInt64";
        map["ONE"] = static_cast<int64_t>(EnumUInt64::ONE);
        map["TWO"] = static_cast<int64_t>(EnumUInt64::TWO);
        map["MAXVAL"] = static_cast<int64_t>(EnumUInt64::MAXVAL);
    }

    template <>
    void writeStructFields(const AllTypes* all, StructFieldSerializer<AllTypes>* serializer)
    {
        serializer->writeField(all->e_int8);
        serializer->writeField(all->e_int16);
        serializer->writeField(all->e_int32);
        serializer->writeField(all->e_int64);
        serializer->writeField(all->e_uint8);
        serializer->writeField(all->e_uint16);
        serializer->writeField(all->e_uint32);
        serializer->writeField(all->e_uint64);
        serializer->writeField(all->ch);
        serializer->writeField(all->int8);
        serializer->writeField(all->int16);
        serializer->writeField(all->int32);
        serializer->writeField(all->int64);
        serializer->writeField(all->uint8);
        serializer->writeField(all->uint16);
        serializer->writeField(all->uint32);
        serializer->writeField(all->uint64);
        serializer->writeField(all->flt);
        serializer->writeField(all->dbl);
        serializer->writeField(all->str);
        serializer->writeField(all->int8_hex);
        serializer->writeField(all->int16_hex);
        serializer->writeField(all->int32_hex);
        serializer->writeField(all->int64_hex);
        serializer->writeField(all->uint8_hex);
        serializer->writeField(all->uint16_hex);
        serializer->writeField(all->uint32_hex);
        serializer->writeField(all->uint64_hex);
    }
}

using StatCollectionInt8   = simdb::StatCollection<int8_t>;
using StatCollectionInt16  = simdb::StatCollection<int16_t>;
using StatCollectionInt32  = simdb::StatCollection<int32_t>;
using StatCollectionInt64  = simdb::StatCollection<int64_t>;
using StatCollectionUInt8  = simdb::StatCollection<uint8_t>;
using StatCollectionUInt16 = simdb::StatCollection<uint16_t>;
using StatCollectionUInt32 = simdb::StatCollection<uint32_t>;
using StatCollectionUInt64 = simdb::StatCollection<uint64_t>;
using StatCollectionFloat  = simdb::StatCollection<float>;
using StatCollectionDouble = simdb::StatCollection<double>;

using ScalarStructCollection = simdb::ScalarStructCollection<AllTypes>;
using StructGroup = std::vector<std::shared_ptr<AllTypes>>;
using StructGroupCollection = simdb::IterableStructCollection<StructGroup>;
using SparseStructGroupCollection = simdb::IterableStructCollection<StructGroup, true>;

#define STRUCT_GROUP_CAPACITY 8
#define SPARSE_STRUCT_GROUP_CAPACITY 16

std::shared_ptr<AllTypes> generateRandomStruct()
{
    auto s = std::make_shared<AllTypes>();

    s->e_int8 = static_cast<EnumInt8>(rand() % (int)EnumInt8::__COUNT__);
    s->e_int16 = static_cast<EnumInt16>(rand() % (int)EnumInt16::__COUNT__);
    s->e_int32 = static_cast<EnumInt32>(rand() % (int)EnumInt32::__COUNT__);
    s->e_int64 = static_cast<EnumInt64>(rand() % (int)EnumInt64::__COUNT__);
    s->e_uint8 = static_cast<EnumUInt8>(rand() % (int)EnumUInt8::__COUNT__);
    s->e_uint16 = static_cast<EnumUInt16>(rand() % (int)EnumUInt16::__COUNT__);
    s->e_uint32 = static_cast<EnumUInt32>(rand() % (int)EnumUInt32::__COUNT__);
    s->e_uint64 = static_cast<EnumUInt64>(rand() % (int)EnumUInt64::__COUNT__);

    s->ch = generateRandomChar();
    s->int8 = generateRandomInt<int8_t>();
    s->int16 = generateRandomInt<int16_t>();
    s->int32 = generateRandomInt<int32_t>();
    s->int64 = generateRandomInt<int64_t>();
    s->uint8 = generateRandomInt<uint8_t>();
    s->uint16 = generateRandomInt<uint16_t>();
    s->uint32 = generateRandomInt<uint32_t>();
    s->uint64 = generateRandomInt<uint64_t>();
    s->flt = generateRandomReal<float>();
    s->dbl = generateRandomReal<double>();
    s->str = generateRandomString();

    s->int8_hex = generateRandomInt<int8_t>();
    s->int16_hex = generateRandomInt<int16_t>();
    s->int32_hex = generateRandomInt<int32_t>();
    s->int64_hex = generateRandomInt<int64_t>();
    s->uint8_hex = generateRandomInt<uint8_t>();
    s->uint16_hex = generateRandomInt<uint16_t>();
    s->uint32_hex = generateRandomInt<uint32_t>();
    s->uint64_hex = generateRandomInt<uint64_t>();

    return s;
}

/// Example simulator that configures all supported types of collections.
class Sim
{
public:
    Sim(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
    }

    void runSimulation()
    {
        configCollections_();

        while (time_++ < 1000) {
            generateRandomStats_();
            generateRandomStructs_();
            generateRandomStructGroups_();
            db_mgr_->getCollectionMgr()->collectAll();
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollections_()
    {
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->useTimestampsFrom(&time_);

        std::unique_ptr<StatCollectionInt8> int8_collection(new StatCollectionInt8("Int8Collection"));
        int8_collection->addStat("stats.int8", &stat_int8_);
        collection_mgr->addCollection(std::move(int8_collection));

        std::unique_ptr<StatCollectionInt16> int16_collection(new StatCollectionInt16("Int16Collection"));
        int16_collection->addStat("stats.int16", &stat_int16_);
        collection_mgr->addCollection(std::move(int16_collection));

        std::unique_ptr<StatCollectionInt32> int32_collection(new StatCollectionInt32("Int32Collection"));
        int32_collection->addStat("stats.int32", &stat_int32_);
        collection_mgr->addCollection(std::move(int32_collection));

        std::unique_ptr<StatCollectionInt64> int64_collection(new StatCollectionInt64("Int64Collection"));
        int64_collection->addStat("stats.int64", &stat_int64_);
        collection_mgr->addCollection(std::move(int64_collection));

        std::unique_ptr<StatCollectionUInt8> uint8_collection(new StatCollectionUInt8("UInt8Collection"));
        uint8_collection->addStat("stats.uint8", &stat_uint8_);
        collection_mgr->addCollection(std::move(uint8_collection));

        std::unique_ptr<StatCollectionUInt16> uint16_collection(new StatCollectionUInt16("UInt16Collection"));
        uint16_collection->addStat("stats.uint16", &stat_uint16_);
        collection_mgr->addCollection(std::move(uint16_collection));

        std::unique_ptr<StatCollectionUInt32> uint32_collection(new StatCollectionUInt32("UInt32Collection"));
        uint32_collection->addStat("stats.uint32", &stat_uint32_);
        collection_mgr->addCollection(std::move(uint32_collection));

        std::unique_ptr<StatCollectionUInt64> uint64_collection(new StatCollectionUInt64("UInt64Collection"));
        uint64_collection->addStat("stats.uint64", &stat_uint64_);
        collection_mgr->addCollection(std::move(uint64_collection));

        std::unique_ptr<StatCollectionFloat> float_collection(new StatCollectionFloat("FloatCollection"));
        float_collection->addStat("stats.float", &stat_flt_);
        collection_mgr->addCollection(std::move(float_collection));

        std::unique_ptr<StatCollectionDouble> double_collection(new StatCollectionDouble("DoubleCollection"));
        double_collection->addStat("stats.double", &stat_dbl_);
        collection_mgr->addCollection(std::move(double_collection));

        std::unique_ptr<ScalarStructCollection> scalar_struct_collection(new ScalarStructCollection("StructCollection"));
        scalar_struct_collection->addStruct("structs.scalar", &scalar_struct_);
        collection_mgr->addCollection(std::move(scalar_struct_collection));

        std::unique_ptr<StructGroupCollection> iterable_struct_collection(new StructGroupCollection("ContigStructsCollection"));
        iterable_struct_collection->addContainer("structs.iterables.contig", &iterable_structs_, STRUCT_GROUP_CAPACITY);
        collection_mgr->addCollection(std::move(iterable_struct_collection));

        std::unique_ptr<SparseStructGroupCollection> sparse_iterable_struct_collection(new SparseStructGroupCollection("SparseStructsCollection"));
        sparse_iterable_struct_collection->addContainer("structs.iterables.sparse", &sparse_iterable_structs_, SPARSE_STRUCT_GROUP_CAPACITY);
        collection_mgr->addCollection(std::move(sparse_iterable_struct_collection));

        db_mgr_->finalizeCollections();
    }

    void generateRandomStats_()
    {
        stat_int8_   = generateRandomInt<int8_t>();
        stat_int16_  = generateRandomInt<int16_t>();
        stat_int32_  = generateRandomInt<int32_t>();
        stat_int64_  = generateRandomInt<int64_t>();
        stat_uint8_  = generateRandomInt<uint8_t>();
        stat_uint16_ = generateRandomInt<uint16_t>();
        stat_uint32_ = generateRandomInt<uint32_t>();
        stat_uint64_ = generateRandomInt<uint64_t>();
        stat_flt_ = generateRandomReal<float>();
        stat_dbl_ = generateRandomReal<double>();
    }

    void generateRandomStructs_()
    {
        auto s = generateRandomStruct();
        scalar_struct_ = *s;
    }

    void generateRandomStructGroups_()
    {
        iterable_structs_.clear();
        auto num_contig_insts = rand() % STRUCT_GROUP_CAPACITY;
        for (size_t idx = 0; idx < num_contig_insts; ++idx) {
            iterable_structs_.push_back(generateRandomStruct());
        }

        sparse_iterable_structs_.clear();
        sparse_iterable_structs_.resize(SPARSE_STRUCT_GROUP_CAPACITY);
        for (size_t idx = 0; idx < SPARSE_STRUCT_GROUP_CAPACITY; ++idx) {
            if (rand() % 3 == 0) {
                sparse_iterable_structs_[idx] = generateRandomStruct();
            }
        }
    }

    simdb::DatabaseManager* db_mgr_;
    uint64_t time_ = 0;

    int8_t stat_int8_;
    int16_t stat_int16_;
    int32_t stat_int32_;
    int64_t stat_int64_;
    uint8_t stat_uint8_;
    uint16_t stat_uint16_;
    uint32_t stat_uint32_;
    uint64_t stat_uint64_;
    float stat_flt_;
    double stat_dbl_;

    AllTypes scalar_struct_;
    StructGroup iterable_structs_;
    StructGroup sparse_iterable_structs_;
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
