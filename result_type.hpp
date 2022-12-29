#pragma once

#include <optional>

namespace pgaccel
{

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
    static Status FromArgs(StatusCode status, Args&&... args) {
        return Status();
    }

private:
    StatusCode code;
    std::string msg;
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
