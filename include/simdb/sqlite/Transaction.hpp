#pragma once

namespace simdb {

class SQLiteTransaction
{
public:
    virtual ~SQLiteTransaction() = default;

    virtual void beginTransaction() const = 0;

    virtual void endTransaction() const = 0;
};

} // namespace simdb
