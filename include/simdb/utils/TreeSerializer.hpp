// <TreeSerializer> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

inline void serializeElementTree(DatabaseManager* db_mgr,
                                 const TreeNode* start,
                                 const int parent_db_id = 0)
{
    if (!start) {
        return;
    }

    auto root_record = db_mgr->INSERT(SQL_TABLE("ElementTreeNodes"),
                                      SQL_COLUMNS("Name", "ParentID"),
                                      SQL_VALUES(start->name, parent_db_id));

    for (const auto& child : start->children) {
        serializeElementTree(db_mgr, child.get(), root_record->getId());
    }
}

} // namespace simdb
