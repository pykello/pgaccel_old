#pragma once

#include <optional>
#include <iostream>
#include <sstream>

/*
 * Result<T> type, which either contains a value or an error status.
 * Similar to apache arrow's result type, but implemented in-house in
 * case we want to drop dependency on arrow.
 */

namespace pgaccel
{

#define ASSIGN_OR_RAISE(var, result) \
    do {\
        const auto r = result; \
        if (r.ok()) \
            var = std::move(*r); \
        else \
            return r.status(); \
    } while(0);

#define RAISE_IF_FAILS(result) \
    do {\
        const auto r = result; \
        if (!r.ok()) \
            return r.status(); \
    } while(0);

enum class StatusCode  {
    OK,
    Invalid
};

class Status {
public:
    Status(StatusCode code, const std::string &msg) : code(code), msg(msg)
    {
        // nothing to do
    }

    Status() {}

    StatusCode Code() const
    {
        return code;
    }

    std::string Message() const
    {
        return msg;
    }

    template <typename... Args>
    static Status Invalid(Args&&... args) {
        return Status::FromArgs(StatusCode::Invalid, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status FromArgs(StatusCode code, Args&&... args) {
        std::ostringstream sstream;
        ConcatAsString(sstream, std::forward<Args>(args)...);
        return Status(code, sstream.str());
    }

private:
    StatusCode code;
    std::string msg;

    template <typename Head>
    static void ConcatAsString(std::ostream &stream, Head&& head) {
        stream << head;
    }

    template <typename Head, typename... Tail>
    static void ConcatAsString(std::ostream &stream, Head&& head, Tail&&... tail) {
        ConcatAsString(stream, std::forward<Head>(head));
        ConcatAsString(stream, std::forward<Tail>(tail)...);
    }
};

template<typename T>
class Result {
public:
    Result(Status status): status_(status) {}
    Result(T&& value): status_(StatusCode::OK, ""), value_(std::move(value)) {}

    bool ok() const { return status_.Code() == StatusCode::OK; }

    const T& operator*() const& { return *value_; }
    const T* operator->() const { return value_.operator->(); }

    Status status() const { return status_; }

private:
    Status status_;
    std::optional<T> value_;
};

};
