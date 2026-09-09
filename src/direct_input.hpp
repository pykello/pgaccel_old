#pragma once
#include <cstdint>
#include <istream>
#include <memory>
#include <string>

namespace pgaccel {
// Synchronous, seekable O_DIRECT input with one owned bounded bounce buffer.
// Unaligned logical reads never fall back to buffered payload I/O.
class DirectInput final : public std::istream {
    class Buffer;
    std::unique_ptr<Buffer> buffer_;
public:
    static constexpr size_t BufferBytes = 8 << 20;
    explicit DirectInput(const std::string &path);
    ~DirectInput();
    uint64_t bytesRead() const;
    uint64_t readCalls() const;
};
}
