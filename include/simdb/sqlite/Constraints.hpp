// <Constraints> -*- C++ -*-

#pragma once

#include <iostream>

namespace simdb3
{

/// This enum holds all the comparators for WHERE clauses in queries (scalar target values).
enum class Constraints {
    // WHERE val =  5
    EQUAL,
    // WHERE val != 5
    NOT_EQUAL,
    // WHERE val <  5
    LESS,
    // WHERE val <= 5
    LESS_EQUAL,
    // WHERE val >  5
    GREATER,
    // WHERE val >= 5
    GREATER_EQUAL,
    // For internal use only
    __NUM_CONSTRAINTS__
};

/// This enum holds all the comparators for WHERE clauses in queries (multiple target values).
enum class SetConstraints {
    // WHERE val IN (4,5,6)
    IN_SET = (int)simdb3::Constraints::__NUM_CONSTRAINTS__,
    // WHERE val NOT IN (4,5,6)
    NOT_IN_SET,
    // For internal use only
    __NUM_CONSTRAINTS__
};

/// This enum holds all the comparators for WHERE clauses in queries (string target values).
enum class QueryOperator {
    AND,
    OR,
    __NUM_CONSTRAINTS__
};

/// Stringifier for the Constraints enum
inline std::string stringify(const simdb3::Constraints constraint)
{
    switch (constraint) {
        case simdb3::Constraints::EQUAL:
            return " =  ";
        case simdb3::Constraints::NOT_EQUAL:
            return " != ";
        case simdb3::Constraints::LESS:
            return " <  ";
        case simdb3::Constraints::LESS_EQUAL:
            return " <= ";
        case simdb3::Constraints::GREATER:
            return " >  ";
        case simdb3::Constraints::GREATER_EQUAL:
            return " >= ";
        default:
            return "INVALID";
    }
}

/// Stringifier for the SetConstraints enum
inline std::string stringify(const simdb3::SetConstraints constraint)
{
    switch (constraint) {
        case simdb3::SetConstraints::IN_SET:
            return " IN ";
        case simdb3::SetConstraints::NOT_IN_SET:
            return " NOT IN ";
        default:
            return "INVALID";
    }
}

} // namespace simdb3
