// <TreeSerializer> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

inline void serializeElementTree(DatabaseManager* db_mgr,
                                 const TreeNode* root,
                                 std::unordered_map<const TreeNode*, int>* node_map = nullptr)
{
    if (!root) {
        return;
    }

    static std::unordered_map<const TreeNode*, int> s_node_map;
    if (!node_map) {
        node_map = &s_node_map;
        s_node_map.clear();
    }

    int parent_id = 0;
    if (root->parent) {
        auto it = node_map->find(root->parent);
        assert(it != node_map->end());
        parent_id = it->second;
    }

    auto root_record = db_mgr->INSERT(SQL_TABLE("ElementTreeNodes"),
                                      SQL_COLUMNS("Name", "ParentID"),
                                      SQL_VALUES(root->name, parent_id));

    s_node_map[root] = root_record->getId();

    for (const auto& child : root->children) {
        auto child_record = db_mgr->INSERT(SQL_TABLE("ElementTreeNodes"),
                                           SQL_COLUMNS("Name", "ParentID"),
                                           SQL_VALUES(child->name, root_record->getId()));

        s_node_map[child.get()] = child_record->getId();

        serializeElementTree(db_mgr, child.get(), node_map);
    }
}

} // namespace simdb
