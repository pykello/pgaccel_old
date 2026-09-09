#pragma once
#include <cstdint>
#include <istream>
#include <memory>
#include <string>

namespace pgaccel {
// Seekable O_DIRECT input with two bounded buffers and one read ahead.
// Unaligned logical reads never fall back to buffered payload I/O.
class DirectInput final : public std::istream {
    class Buffer;
    std::unique_ptr<Buffer> buffer_;
public:
    static constexpr size_t ReadBytes = 8 << 20;
    static constexpr size_t BufferBytes = 2 * ReadBytes;
    enum class Mode { Payload, Header };
    static constexpr size_t HeaderBytes = 4096;
    explicit DirectInput(const std::string &path, Mode mode = Mode::Payload);
    ~DirectInput();
    uint64_t bufferBytes() const;
    uint64_t bytesRead() const;
    uint64_t readCalls() const;
};
}
