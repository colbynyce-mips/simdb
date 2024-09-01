// <StringMap> -*- C++ -*-

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace simdb3
{

class StringMap
{
public:
    using string_map_t = std::shared_ptr<std::unordered_map<std::string, uint32_t>>;
    using unserialized_string_map_t = std::unordered_map<uint32_t, std::string>;

    static StringMap* instance()
    {
        static StringMap map;
        return &map;
    }

    const string_map_t getMap() const
    {
        return map_;
    }

    uint32_t getStringId(const std::string& s)
    {
        auto iter = map_->find(s);
        if (iter == map_->end()) {
            uint32_t id = map_->size();
            map_->insert({s, id});
            unserialized_map_.insert({id, s});
            return id;
        } else {
            return iter->second;
        }
    }

    const unserialized_string_map_t& getUnserializedMap() const
    {
        return unserialized_map_;
    }

    void clearUnserializedMap()
    {
        unserialized_map_.clear();
    }

private:
    StringMap() = default;
    string_map_t map_ = std::make_shared<std::unordered_map<std::string, uint32_t>>();
    unserialized_string_map_t unserialized_map_;
};

} // namespace simdb3
