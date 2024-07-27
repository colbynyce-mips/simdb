// <MetaStructs> -*- C++ -*-

#pragma once

/*!
 * \brief Metaprogramming utilities for use
 * with SimDB schemas.
 */

#include "simdb/Errors.hpp"
#include "simdb/schema/ColumnTypedefs.hpp"
#include "simdb/schema/DatabaseTypedefs.hpp"
#include "simdb/schema/GeneralMetaStructs.hpp"
#include "simdb/utils/CompatUtils.hpp"

#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

namespace simdb
{

//! Base template for column_info structs
template <typename ColumnT, typename Enable = void>
struct column_info;

//! int32_t
template <>
struct column_info<int32_t> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::int32_t;
    }
    using value_type = int32_t;
    static constexpr bool is_fixed_size = utils::is_pod<int32_t>::value;
};

//! uint32_t
template <>
struct column_info<uint32_t> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::uint32_t;
    }
    using value_type = uint32_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint32_t>::value;
};

//! int64_t
template <>
struct column_info<int64_t> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::int64_t;
    }
    using value_type = int64_t;
    static constexpr bool is_fixed_size = utils::is_pod<int64_t>::value;
};

//! uint64_t
template <>
struct column_info<uint64_t> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::uint64_t;
    }
    using value_type = uint64_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint64_t>::value;
};

//! double
template <>
struct column_info<double> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::double_t;
    }
    using value_type = double;
    static constexpr bool is_fixed_size = utils::is_pod<double>::value;
};

//! string
template <typename ColumnT>
struct column_info<
    ColumnT,
    typename std::enable_if<std::is_same<ColumnT, std::string>::value or
                            std::is_same<typename std::decay<ColumnT>::type, const char*>::value>::type> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::string_t;
    }
    using value_type = ColumnT;
    static constexpr bool is_fixed_size = utils::is_pod<std::string>::value;
};

//! Vectors of raw bytes are stored as blobs (void* / opaque)
template <typename ColumnT>
struct column_info<ColumnT, typename std::enable_if<is_container<ColumnT>::value>::type> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::blob_t;
    }
    using value_type = typename is_container<ColumnT>::value_type;
    static constexpr bool is_fixed_size = utils::is_pod<ColumnT>::value;
};

//! Blob descriptor
template <typename ColumnT>
struct column_info<ColumnT, typename std::enable_if<std::is_same<ColumnT, Blob>::value>::type> {
    static ColumnDataType data_type()
    {
        return ColumnDataType::blob_t;
    }
    using value_type = Blob;
    static constexpr bool is_fixed_size = utils::is_pod<ColumnT>::value;
};

} // namespace simdb
