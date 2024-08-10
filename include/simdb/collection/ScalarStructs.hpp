// <ScalarStructs> -*- C++ -*-

#pragma once

#include "simdb/async/AsyncTaskQueue.hpp"
#include "simdb/collection/CollectionBase.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include <cstring>

namespace simdb
{

/*!
 * \class ScalarStructCollection
 *
 * \brief This class is used to collect struct-like data structures commonly
 *        encountered in simulators, with support for enum and string fields. 
 *        If you want to collect vectors/deques of structs (or smart pointers 
 *        of structs), use the StructContainerCollection class.
 */
template <typename 

} // namespace simdb
