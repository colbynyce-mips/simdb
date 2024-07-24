// <MetaStructs> -*- C++ -*-

#pragma once

/*!
 * \brief Metaprogramming utilities for use
 * with SimDB schemas.
 */

#include "simdb/schema/GeneralMetaStructs.hpp"
#include "simdb/schema/ColumnTypedefs.hpp"
#include "simdb/schema/DatabaseTypedefs.hpp"
#include "simdb/Errors.hpp"
#include "simdb/utils/CompatUtils.hpp"

#include <string>
#include <type_traits>
#include <vector>
#include <numeric>

namespace simdb {

//! Base template for column_info structs
template <typename ColumnT, typename Enable = void>
struct column_info;

//! int8_t
template <>
struct column_info<int8_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::int8_t;
    }
    using value_type = int8_t;
    static constexpr bool is_fixed_size = utils::is_pod<int8_t>::value;
};

//! uint8_t
template <>
struct column_info<uint8_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::uint8_t;
    }
    using value_type = uint8_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint8_t>::value;
};

//! int16_t
template <>
struct column_info<int16_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::int16_t;
    }
    using value_type = int16_t;
    static constexpr bool is_fixed_size = utils::is_pod<int16_t>::value;
};

//! uint16_t
template <>
struct column_info<uint16_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::uint16_t;
    }
    using value_type = uint16_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint16_t>::value;
};

//! int32_t
template <>
struct column_info<int32_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::int32_t;
    }
    using value_type = int32_t;
    static constexpr bool is_fixed_size = utils::is_pod<int32_t>::value;
};

//! uint32_t
template <>
struct column_info<uint32_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::uint32_t;
    }
    using value_type = uint32_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint32_t>::value;
};

//! int64_t
template <>
struct column_info<int64_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::int64_t;
    }
    using value_type = int64_t;
    static constexpr bool is_fixed_size = utils::is_pod<int64_t>::value;
};

//! uint64_t
template <>
struct column_info<uint64_t> {
    static ColumnDataType data_type() {
        return ColumnDataType::uint64_t;
    }
    using value_type = uint64_t;
    static constexpr bool is_fixed_size = utils::is_pod<uint64_t>::value;
};

//! float
template <>
struct column_info<float> {
    static ColumnDataType data_type() {
        return ColumnDataType::float_t;
    }
    using value_type = float;
    static constexpr bool is_fixed_size = utils::is_pod<float>::value;
};

//! double
template <>
struct column_info<double> {
    static ColumnDataType data_type() {
        return ColumnDataType::double_t;
    }
    using value_type = double;
    static constexpr bool is_fixed_size = utils::is_pod<double>::value;
};

//! string
template <typename ColumnT>
struct column_info<ColumnT, typename std::enable_if<
    std::is_same<ColumnT, std::string>::value or
    std::is_same<typename std::decay<ColumnT>::type, const char*>::value>::type>
{
    static ColumnDataType data_type() {
        return ColumnDataType::string_t;
    }
    using value_type = ColumnT;
    static constexpr bool is_fixed_size = utils::is_pod<std::string>::value;
};

//! Vectors of raw bytes are stored as blobs (void* / opaque)
template <typename ColumnT>
struct column_info<ColumnT, typename std::enable_if<
    is_container<ColumnT>::value>::type>
{
    static ColumnDataType data_type() {
        return ColumnDataType::blob_t;
    }
    using value_type = typename is_container<ColumnT>::value_type;
    static constexpr bool is_fixed_size = utils::is_pod<ColumnT>::value;
};

//! Blob descriptor
template <typename ColumnT>
struct column_info<ColumnT, typename std::enable_if<
    std::is_same<ColumnT, Blob>::value>::type>
{
    static ColumnDataType data_type() {
        return ColumnDataType::blob_t;
    }
    using value_type = Blob;
    static constexpr bool is_fixed_size = utils::is_pod<ColumnT>::value;
};

}

