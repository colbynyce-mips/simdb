// <Constraints> -*- C++ -*-

#pragma once

#include "simdb/Errors.hpp"

#include <cstdint>
#include <iostream>

namespace simdb
{

enum class Constraints { EQUAL = 1, NOT_EQUAL = 2, LESS = 3, LESS_EQUAL = 4, GREATER = 5, GREATER_EQUAL = 6 };

enum class SetConstraints { IN_SET = 7, NOT_IN_SET = 8 };

inline std::ostream& operator<<(std::ostream& os, const Constraints constraint)
{
    switch (constraint) {
    case Constraints::EQUAL:
        os << " = ";
        break;
    case Constraints::NOT_EQUAL:
        os << " != ";
        break;
    case Constraints::LESS:
        os << " < ";
        break;
    case Constraints::LESS_EQUAL:
        os << " <= ";
        break;
    case Constraints::GREATER:
        os << " > ";
        break;
    case Constraints::GREATER_EQUAL:
        os << " >= ";
        break;
    }

    return os;
}

inline std::ostream& operator<<(std::ostream& os, const SetConstraints constraint)
{
    switch (constraint) {
    case SetConstraints::IN_SET:
        os << " IN ";
        break;
    case SetConstraints::NOT_IN_SET:
        os << " NOT IN ";
        break;
    }

    return os;
}

} // namespace simdb
