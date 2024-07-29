// <Errors> -*- C++ -*-

#pragma once

#include <cassert>
#include <exception>
#include <sstream>
#include <string>

namespace simdb
{

/// Used to construct and throw a standard C++ exception
class DBException : public std::exception
{
public:
    DBException() = default;

    /// Construct a DBException object
    DBException(const std::string& reason)
    {
        reason_ << reason;
    }

    /// Copy construct a DBException object
    DBException(const DBException& rhs)
    {
        reason_ << rhs.reason_.str();
    }

    /// Destroy!
    virtual ~DBException() noexcept override {}

    /**
     * \brief Overload from std::exception
     * \return Const char * of the exception reason
     */
    virtual const char* what() const noexcept override
    {
        reason_str_ = reason_.str();
        return reason_str_.c_str();
    }

    /**
     * \brief Append additional information to the message.
     */
    template <typename T>
    DBException& operator<<(const T& msg)
    {
        reason_ << msg;
        return *this;
    }

private:
    // The reason/explanation for the exception
    std::stringstream reason_;

    // Need to keep a local copy of the string formed in the
    // string stream for the 'what' call
    mutable std::string reason_str_;
};

/// General-purpose database exception used for interrupts
class DatabaseInterrupt : public DBException
{
public:
    const char* what() const noexcept override final
    {
        exception_message_ << "  [simdb] Database operation was interrupted";
        const std::string details = getExceptionDetails_();
        if (!details.empty()) {
            exception_message_ << " (" << details << ")";
        }
        reason_str_ = exception_message_.str();
        return reason_str_.c_str();
    }

private:
    //Subclasses should tack on some more information
    //about the specific exception.
    virtual std::string getExceptionDetails_() const = 0;

    mutable std::ostringstream exception_message_;
    mutable std::string reason_str_;
};

} // namespace simdb

#define ADD_FILE_INFORMATION(ex, file, line) ex << ": in file: '" << file << "', on line: " << std::dec << line;

#define simdb_throw(message)                                                                                           \
    {                                                                                                                  \
        std::stringstream msg;                                                                                         \
        msg << message;                                                                                                \
        simdb::DBException ex(std::string("abort: ") + msg.str());                                                     \
        ADD_FILE_INFORMATION(ex, __FILE__, __LINE__);                                                                  \
        throw ex;                                                                                                      \
    }
