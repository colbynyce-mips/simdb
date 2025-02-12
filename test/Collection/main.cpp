/// Tests for SimDB collections feature. This test brings everything together
/// and produces collections of scalar stats, structs, iterable structs (both
/// sparse and non-sparse), and non-default format options.
///
/// This test is meant to drive the python module deserializers / reports.
/// All supported data types are tested here.

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <random>

TEST_INIT;

std::random_device rd;  // a seed source for the random number engine
std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()

enum class Colors { RED=1, GREEN=2, BLUE=3, WHITE=0, TRANSPARENT=-1 };

struct DummyPacket
{
    Colors      e_color;
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
};

using DummyPacketPtr = std::shared_ptr<DummyPacket>;
using DummyPacketPtrVec = std::vector<DummyPacketPtr>;

template <typename T>
T generateRandomInt()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_int_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

template <typename T>
T generateRandomFloat()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_real_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

char generateRandomChar()
{
    return 'A' + rand() % 26;
}

std::string generateRandomString(size_t minchars = 2, size_t maxchars = 8)
{
    EXPECT_TRUE(minchars <= maxchars);

    std::string str;
    while (str.size() < minchars) {
        str += generateRandomChar();
    }

    size_t num_extra_chars = rand() % (maxchars - minchars);
    while (str.size() < maxchars) {
        str += generateRandomChar();
    }

    return str;
}

Colors generateRandomColor()
{
    return static_cast<Colors>(rand() % 6 - 1);
}

DummyPacketPtr generateRandomDummyPacket()
{
    auto s = std::make_shared<DummyPacket>();

    s->e_color = generateRandomColor();
    s->ch = generateRandomChar();
    s->int8 = generateRandomInt<int8_t>();
    s->int16 = generateRandomInt<int16_t>();
    s->int32 = generateRandomInt<int32_t>();
    s->int64 = generateRandomInt<int64_t>();
    s->uint8 = generateRandomInt<uint8_t>();
    s->uint16 = generateRandomInt<uint16_t>();
    s->uint32 = generateRandomInt<uint32_t>();
    s->uint64 = generateRandomInt<uint64_t>();
    s->flt = generateRandomFloat<float>();
    s->dbl = generateRandomFloat<double>();
    s->str = generateRandomString();

    return s;
}

namespace simdb
{
    template <>
    void defineStructSchema<DummyPacket>(StructSchema& schema)
    {
        schema.setStructName("DummyPacket");
        schema.addField<Colors>("color");
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
        schema.addHexField<uint32_t>("uint32h");
        schema.addHexField<uint64_t>("uint64h");
    }

    template <>
    void defineEnumMap<Colors>(std::string& enum_name, std::map<std::string, int>& map)
    {
        enum_name = "Colors";
        map["RED"] = static_cast<int>(Colors::RED);
        map["GREEN"] = static_cast<int>(Colors::GREEN);
        map["BLUE"] = static_cast<int>(Colors::BLUE);
        map["WHITE"] = static_cast<int>(Colors::WHITE);
        map["TRANSPARENT"] = static_cast<int>(Colors::TRANSPARENT);
    }

    template <>
    void writeStructFields(const DummyPacket* all, StructFieldSerializer<DummyPacket>* serializer)
    {
        serializer->writeField(all->e_color);
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
        serializer->writeField(all->uint32);
        serializer->writeField(all->uint64);
    }

} // namespace simdb

#define STRUCT_GROUP_CAPACITY 8
#define SPARSE_STRUCT_GROUP_CAPACITY 16

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

        while (false) {
            generateRandomStats_();
            generateRandomStructs_();
            generateRandomStructGroups_();
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollections_()
    {
    }

    void generateRandomStats_()
    {
        int8_ = generateRandomInt<int8_t>();
        int16_ = generateRandomInt<int16_t>();
        int32_ = generateRandomInt<int32_t>();
        int64_ = generateRandomInt<int64_t>();
        uint8_ = generateRandomInt<uint8_t>();
        uint16_ = generateRandomInt<uint16_t>();
        uint32_ = generateRandomInt<uint32_t>();
        uint64_ = generateRandomInt<uint64_t>();
        flt_ = generateRandomFloat<float>();
        dbl_ = generateRandomFloat<double>();
    }

    void generateRandomStructs_()
    {
    }

    void generateRandomStructGroups_()
    {
    }

    simdb::DatabaseManager* db_mgr_;

    int8_t int8_;
    int16_t int16_;
    int32_t int32_;
    int64_t int64_;
    uint8_t uint8_;
    uint16_t uint16_;
    uint32_t uint32_;
    uint64_t uint64_;
    float flt_;
    double dbl_;
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
