#pragma once

#include "simdb/sqlite/SQLiteTable.hpp"
#include "simdb/schema/ColumnTypedefs.hpp"
#include "simdb/Errors.hpp"

#include <algorithm>
#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace simdb {

//! Compression enumeration specifying various levels
//! of compression which may be available in the SimDB
//! implementation.
enum class CompressionType : int8_t {
    NONE,
    DEFAULT_COMPRESSION,
    BEST_COMPRESSION_RATIO,
    BEST_COMPRESSION_SPEED
};

//! Column class used for creating SimDB tables
class Column
{
public:
    //! Construct a column with a name and one of the supported data types.
    //! The column name must not be empty.
    Column(const std::string & column_name, const ColumnDataType dt) :
        name_(column_name),
        dt_(dt)
    {
        if (name_.empty()) {
            throw DBException(
                "You cannot create a database column with no name");
        }
    }

    //! Default copies, default assignments
    Column(const Column &) = default;
    Column & operator=(const Column &) = default;

    //! Equivalence against another Column
    bool operator==(const Column & rhs) const {
        if (getName() != rhs.getName()) {
            return false;
        }
        if (getDataType() != rhs.getDataType()) {
            return false;
        }
        return true;
    }

    //! Equivalence against another Column
    bool operator!=(const Column & rhs) const {
        return !(*this == rhs);
    }

    //! Name of this table column
    const std::string & getName() const {
        return name_;
    }

    //! Data type of this table column
    ColumnDataType getDataType() const {
        return dt_;
    }

    //! See if this column is indexed, either by itself or
    //! indexed against other Columns too.
    bool isIndexed() const {
        return !indexed_properties_.empty();
    }

    //! If indexed, return the list of indexed Column names.
    //! Returns an empty vector if this Column is not indexed.
    const std::vector<Column*> & getIndexedProperties() const {
        return indexed_properties_;
    }

    //! See if this Column has a default value set or not.
    bool hasDefaultValue() const {
        return !default_val_string_.empty();
    }

    //! Get this Column's default value. These are returned as
    //! strings since the schema creation code is implementation-
    //! specific (SQLite builds table creation statements one way,
    //! HDF5 builds them another way, etc.) and default values
    //! are only allowed on simple column data types (any type
    //! that isn't a blob) so these strings can be lexical-casted
    //! back to their native form if needed ("123" -> 123, etc.)
    const std::string & getDefaultValueAsString() const {
        return default_val_string_;
    }

private:
    // Optionally specify a default value for this Column.
    // Defaults for Blob data types are not allowed and will
    // throw if you attempt to set a Blob default value.
    // The DefaultValueT template type needs to be able to
    // be lexical-casted to a std::string, or this method
    // will throw.
    template <typename DefaultValueT>
    void setDefaultValue_(const DefaultValueT & val) {
        if (dt_ == ColumnDataType::blob_t) {
            throw DBException(
                "Cannot set default value for a database "
                "column with blob data type");
        }

        if (dt_ == ColumnDataType::fkey_t) {
            throw DBException(
                "Cannot set default value for a database "
                "column with foreign key data type");
        }

        switch (dt_) {
            case ColumnDataType::int32_t:
            case ColumnDataType::int64_t:
            case ColumnDataType::uint32_t:
            case ColumnDataType::uint64_t: {
                verifyDefaultValueIsInt_<DefaultValueT>();
                break;
            }

            case ColumnDataType::double_t: {
                verifyDefaultValueIsFloat_<DefaultValueT>();
                break;
            }

            default:
                break;
        }

        std::ostringstream ss;
        writeDefaultValue_(ss, val);
        default_val_string_ = ss.str();
        if (default_val_string_.empty()) {
            throw DBException("Unable to convert default value ")
                << val << " into a std::string";
        }
    }

    template <typename DefaultValueT>
    typename std::enable_if<std::is_floating_point<DefaultValueT>::value, void>::type
    writeDefaultValue_(std::ostringstream & oss, DefaultValueT val) const
    {
        oss << std::numeric_limits<long double>::digits10 + 1 << val;
    }

    template <typename DefaultValueT>
    typename std::enable_if<!std::is_floating_point<DefaultValueT>::value, void>::type
    writeDefaultValue_(std::ostringstream & oss, DefaultValueT val) const
    {
        oss << val;
    }

    void setDefaultValueString_(const std::string & val) {
        if (dt_ != ColumnDataType::string_t) {
            throw DBException("Unable to set default value string (data type mismatch)");
        }

        std::ostringstream ss;
        ss << val;
        default_val_string_ = ss.str();
        if (default_val_string_.empty()) {
            throw DBException("Unable to convert default value ")
                << val << " into a std::string";
        }
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value, void>::type
    verifyDefaultValueIsInt_() {
    }

    template <typename T>
    typename std::enable_if<!std::is_integral<T>::value, void>::type
    verifyDefaultValueIsInt_() {
        throw DBException("Default value type mismatch (expected integer type)");
    }

    template <typename T>
    typename std::enable_if<std::is_floating_point<T>::value, void>::type
    verifyDefaultValueIsFloat_() {
    }

    template <typename T>
    typename std::enable_if<!std::is_floating_point<T>::value, void>::type
    verifyDefaultValueIsFloat_() {
        throw DBException("Default value type mismatch (expected floating point type)");
    }

    // You can tell the database to create indexes on specific
    // table columns for faster queries later on. For example:
    //
    //    using dt = simdb::ColumnDataType;
    //    simdb::Schema schema;
    //
    //    schema.addTable("Customers")
    //        .addColumn("Last", dt::string_t)
    //            ->index();
    //
    // This results in fast lookup performance for queries like this:
    //
    //    SELECT * FROM Customers WHERE Last = 'Smith'
    //
    // If you want to build indexes based on multiple columns'
    // values, pass in those other Column objects like this:
    //
    //    schema.addTable("Customers")
    //        .addColumn("First", dt::string_t)
    //        .addColumn("Last", dt::string_t)
    //            ->indexAgainst("First");
    //
    // This results in fast lookup performance for queries like this:
    //
    //    SELECT * FROM Customers WHERE Last = 'Smith' AND Age > 40
    //
    void setIsIndexed_(const std::vector<Column*> & indexed_columns = {})
    {
        indexed_properties_.clear();
        indexed_properties_.emplace_back(this);

        indexed_properties_.insert(
            indexed_properties_.end(),
            indexed_columns.begin(),
            indexed_columns.end());

        indexed_properties_.erase(
            std::unique(indexed_properties_.begin(), indexed_properties_.end()),
            indexed_properties_.end());
    }

    std::string name_;
    ColumnDataType dt_;
    std::string default_val_string_;
    std::vector<Column*> indexed_properties_;

    friend class Table;
};

//! Table class used for creating SimDB schemas
class Table
{
public:
    //! \brief Construct a table with a name. The table name
    //! must not be empty.
    //!
    //! \param table_name Name of the database table
    //!
    //! \param compression Optionally request that the table
    //! contents be compressed in the database. Note that the
    //! specific implementation (DbConnProxy subclass) may not
    //! support compression, in which case this option would be
    //! ignored.
    Table(const std::string & table_name,
          const CompressionType compression = CompressionType::BEST_COMPRESSION_RATIO) :
        name_(table_name),
        compression_(compression)
    {
        if (name_.empty()) {
            throw DBException(
                "You cannot create a database table with no name");
        }
    }

    //! Construct without a name.
    Table() = default;

    //! Default copies, default assignments
    Table(const Table &) = default;
    Table & operator=(const Table &) = default;

    //! Equivalence against another Table
    bool operator==(const Table & rhs) const {
        if (getName() != rhs.getName()) {
            return false;
        }
        if (columns_.size() != rhs.columns_.size()) {
            return false;
        }
        for (size_t idx = 0; idx < columns_.size(); ++idx) {
            if (*columns_[idx] != *rhs.columns_[idx]) {
                return false;
            }
        }
        return true;
    }

    //! Equivalence against another Table
    bool operator!=(const Table & rhs) const {
        return !(*this == rhs);
    }

    //! Get this Table's name
    std::string getName() const {
        return name_;
    }

    //! \return Table's compression setting
    CompressionType getCompression() const {
        return compression_;
    }

    //! Add a Column to this Table.
    Table & addColumn(
        const std::string & name,
        const ColumnDataType dt)
    {
        columns_.emplace_back(new Column(name, dt));
        columns_by_name_[name] = columns_.back();
        return *this;
    }

    //! Assign a default value for the given column.
    template <typename T>
    Table & setColumnDefaultValue(const std::string & col_name, const T default_val) {
        auto iter = columns_by_name_.find(col_name);
        if (iter == columns_by_name_.end()) {
            throw DBException("No column named ") << col_name << " in table " << name_;
        }

        iter->second->setDefaultValue_(default_val);
        return *this;
    }

    //! Assign a default value for the given column.
    Table & setColumnDefaultValue(const std::string & col_name, const std::string & default_val) {
        auto iter = columns_by_name_.find(col_name);
        if (iter == columns_by_name_.end()) {
            throw DBException("No column named ") << col_name << " in table " << name_;
        }

        iter->second->setDefaultValueString_(default_val);
        return *this;
    }

    //! Index records by the given column.
    //! CREATE INDEX IndexName ON TableName(ColumnName)
    Table & createIndexOn(const std::string & col_name) {
        return createCompoundIndexOn(SQL_COLUMNS(col_name.c_str()));
    }

    //! Index records by the given columns.
    //! CREATE INDEX IndexName ON TableName(ColA,ColB,ColC)
    Table & createCompoundIndexOn(const SqlColumns & cols) {
        const auto & col_names = cols.getColNames();
        for (const auto & col_name : col_names) {
            if (columns_by_name_.find(col_name) == columns_by_name_.end()) {
                throw DBException("Column ") << col_name << " does not exist in table " << name_;
            }
        }

        std::ostringstream oss;
        oss << "CREATE INDEX " << name_  << "_Index"
            << index_creation_strs_.size() + 1
            << " ON " << name_ << "(";

        size_t idx = 0;
        auto iter = col_names.begin();
        while (iter != col_names.end()) {
            oss << *iter;
            if (idx != col_names.size() - 1) {
                oss << ",";
            }
            ++iter;
            ++idx;
        }
        oss << ")";

        index_creation_strs_.push_back(oss.str());
        return *this;
    }

    //! Iterator access
    std::vector<std::shared_ptr<Column>>::const_iterator begin() const {
        return columns_.begin();
    }

    //! Iterator access
    std::vector<std::shared_ptr<Column>>::const_iterator end() const {
        return columns_.end();
    }

    //! Ask if this Table has any Columns yet
    bool hasColumns() const {
        return !columns_.empty();
    }

private:
    std::string name_;
    std::string name_prefix_;
    CompressionType compression_ = CompressionType::BEST_COMPRESSION_RATIO;
    std::vector<std::shared_ptr<Column>> columns_;
    std::unordered_map<std::string, std::shared_ptr<Column>> columns_by_name_;
    std::vector<std::string> index_creation_strs_;

    friend class Schema;
    friend class SQLiteConnection;
};

//! Schema class used for creating SimDB databases
class Schema
{
public:
    Schema() = default;

    //! \brief Create a new Table in this Schema with the given name
    //!
    //! \param table_name Name of the database table
    //!
    //! \param compression Table-specific compression setting.
    //! This setting will be ignored if the DbConnProxy subclass
    //! being used does not support compression.
    //!
    //! \return Reference to the added table
    Table & addTable(
        const std::string & table_name,
        const CompressionType compression = CompressionType::BEST_COMPRESSION_RATIO)
    {
        tables_.emplace_back(table_name, compression);
        return tables_.back();
    }

    //! \brief Create a new Table in this Schema, copied from the
    //! Table object passed in.
    //!
    //! \param rhs Source table object intended to be added to the schema
    //!
    //! \return Returns a reference to the newly created table, or
    //! returns a reference to an existing table in this schema that
    //! matched the incoming table (same table name, same colum names,
    //! and the same column data types).
    Table & addTable(const Table & rhs) {
        if (Table * existing_table = getTableNamed(rhs.getName())) {
            if (*existing_table != rhs) {
                throw DBException("Cannot add table '")
                    << rhs.getName() << "' to schema. A table "
                    << "with that name already exists.";
            }
            return *existing_table;
        }

        tables_.emplace_back(rhs);
        return tables_.back();
    }

    /*!
     * \brief Combine this schema with the tables from another
     * schema. Any clashes between the new table(s) and the
     * existing table(s) will throw an exception:
     *
     *   1) Added schema has table called "Customers". The
     *      existing schema already has a table by the same
     *      name. However, the column configuration for both
     *      tables is identical. Will NOT throw. The table
     *      would be ignored.
     *
     *   2) Added schema has table called "Customers". The
     *      existing schema already has a table by the same
     *      name. The column configurations of the two tables
     *      are different. WILL throw. Columns are considered
     *      different if they have a different name (case-
     *      sensitive) and/or a different ColumnDataType.
     */
    Schema & operator+=(const Schema & rhs) {
        for (const auto & table : rhs) {
            addTable(table);
        }
        return *this;
    }

    /*!
     * \brief Get a pointer to the schema table with the
     * given name
     *
     * \param table_name Name of the table in this schema
     * you want to access
     *
     * \return Pointer to the table with the given name.
     * Returns null if no table by that name exists.
     */
    Table * getTableNamed(const std::string & table_name) {
        for (auto & lhs : tables_) {
            if (lhs.getName() == table_name) {
                return &lhs;
            }
        }
        return nullptr;
    }

    /*!
     * \brief Get a pointer to the schema table with the
     * given name
     *
     * \param table_name Name of the table in this schema
     * you want to access
     *
     * \return Pointer to the table with the given name.
     * Returns null if no table by that name exists.
     */
    const Table * getTableNamed(const std::string & table_name) const {
        for (auto & lhs : tables_) {
            if (lhs.getName() == table_name) {
                return &lhs;
            }
        }
        return nullptr;
    }

    //! Iterator access
    std::deque<Table>::const_iterator begin() const {
        return tables_.begin();
    }

    //! Iterator access
    std::deque<Table>::const_iterator end() const {
        return tables_.end();
    }

    //! Ask if this Schema has any Tables yet
    bool hasTables() const {
        return !tables_.empty();
    }

    //! Equivalence check
    bool operator==(const Schema & rhs) const {
        if (tables_.size() != rhs.tables_.size()) {
            return false;
        }
        for (size_t idx = 0; idx < tables_.size(); ++idx) {
            if (tables_[idx] != rhs.tables_[idx]) {
                return false;
            }
        }
        return true;
    }

    //! Equivalence check
    bool operator!=(const Schema & rhs) const {
        return !(*this == rhs);
    }

private:
    std::deque<Table> tables_;

    friend class DatabaseManager;
};

} // namespace simdb
