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
    bool        b;
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

bool generateRandomBool()
{
    return rand() % 2 == 0;
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
    s->b = generateRandomBool();
    s->str = generateRandomString();

    return s;
}

namespace simdb
{
    template <>
    void defineStructSchema<DummyPacket>(StructSchema<DummyPacket>& schema)
    {
        schema.addEnum<Colors>("color");
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
        schema.addBool("bool");
        schema.addString("str");
        schema.addHex<uint32_t>("uint32h");
        schema.addHex<uint64_t>("uint64h");
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
        serializer->writeField(all->b);
        serializer->writeField(all->str);
        serializer->writeField(all->uint32);
        serializer->writeField(all->uint64);
    }

} // namespace simdb

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
        configCollectables_();

        size_t tick = 0;
        while (++tick < 10000) {
            randomizeDummyPacketCollectables_();

            // Collect a random uint64_t between ticks 10 and 25
            if (tick == 1000) {
                uint64_collectable_->activate(generateRandomInt<uint64_t>());
            } else if (tick == 2000) {
                uint64_collectable_->deactivate();
            }

            // Collect a random bool between ticks 1500 and 2500
            if (tick == 1500) {
                bool_collectable_->activate(rand() % 2 == 0);
            } else if (tick == 2500) {
                bool_collectable_->deactivate();
            }

            // Collect a random enum between ticks 1800 and 2800
            if (tick == 1800) {
                enum_collectable_->activate(generateRandomColor());
            } else if (tick == 2800) {
                enum_collectable_->deactivate();
            }

            // Collect a random DummyPacket between ticks 2000 and 3000
            if (tick == 2000) {
                dummy_packet_collectable_->activate(generateRandomDummyPacket());
            } else if (tick == 3000) {
                dummy_packet_collectable_->deactivate();
            }

            // Collect some different values for just one cycle. To do this, we call
            // the activate() method, passing in "once=true".
            if (tick >= 5000 && tick % 5 == 0) {
                uint64_collectable_->activate(generateRandomInt<uint64_t>(), true);
                bool_collectable_->activate(rand() % 2 == 0, true);
                enum_collectable_->activate(generateRandomColor(), true);
                dummy_packet_collectable_->activate(generateRandomDummyPacket(), true);
            }

            dummy_collectable_vec_contig_->activate(&dummy_packet_vec_contig_);
            dummy_collectable_vec_sparse_->activate(&dummy_packet_vec_sparse_);

            // "Sweep" the collection system for the current cycle,
            // sending all active values to the database.
            db_mgr_->getCollectionMgr()->sweep("root", tick);
        }

        db_mgr_->getConnection()->getTaskQueue()->stopThread();
    }

private:
    void configCollectables_()
    {
        db_mgr_->enableCollection(10);
        auto collection_mgr = db_mgr_->getCollectionMgr();
        collection_mgr->addClock("root", 10);

        uint64_collectable_ = collection_mgr->createCollectable<uint64_t>("top.uint64", "root");
        bool_collectable_ = collection_mgr->createCollectable<bool>("top.bool", "root");
        enum_collectable_ = collection_mgr->createCollectable<Colors>("top.enum", "root");
        dummy_packet_collectable_ = collection_mgr->createCollectable<DummyPacket>("top.dummy_packet", "root");
        dummy_collectable_vec_contig_ = collection_mgr->createIterableCollector<DummyPacketPtrVec, false>("top.dummy_packet_vec_contig", "root", 32);
        dummy_collectable_vec_sparse_ = collection_mgr->createIterableCollector<DummyPacketPtrVec, true>("top.dummy_packet_vec_sparse", "root", 32);

        db_mgr_->finalizeCollections();
    }

    void randomizeDummyPacketCollectables_()
    {
        dummy_packet_vec_contig_.clear();
        for (size_t i = 0; i < rand() % 10; ++i) {
            dummy_packet_vec_contig_.push_back(generateRandomDummyPacket());
        }

        dummy_packet_vec_sparse_.clear();
        dummy_packet_vec_sparse_.resize(32);
        for (size_t i = 0; i < rand() % 10; ++i) {
            if (rand() % 2 == 0) {
                dummy_packet_vec_sparse_[i] = generateRandomDummyPacket();
            }
        }
    }

    simdb::DatabaseManager* db_mgr_;

    std::shared_ptr<simdb::CollectionPoint> uint64_collectable_;
    std::shared_ptr<simdb::CollectionPoint> bool_collectable_;
    std::shared_ptr<simdb::CollectionPoint> enum_collectable_;
    std::shared_ptr<simdb::CollectionPoint> dummy_packet_collectable_;

    DummyPacketPtrVec dummy_packet_vec_contig_;
    std::shared_ptr<simdb::ContigIterableCollectionPoint> dummy_collectable_vec_contig_;

    DummyPacketPtrVec dummy_packet_vec_sparse_;
    std::shared_ptr<simdb::SparseIterableCollectionPoint> dummy_collectable_vec_sparse_;
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
