#include "direct_input.hpp"
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace pgaccel {
class DirectInput::Buffer final : public std::streambuf {
    static constexpr size_t blockSize = DirectInput::BufferBytes;
    int fd = -1;
    char *buffer = nullptr;
    uint64_t length = 0, start = 0, next = 0;
    uint64_t position() const {
        return eback() ? start + uint64_t(gptr() - eback()) : next;
    }
protected:
    int_type underflow() override {
        uint64_t offset = position();
        if (offset >= length) return traits_type::eof();
        start = offset / blockSize * blockSize;
        ssize_t n;
        do { n = pread(fd, buffer, blockSize, off_t(start)); } while (n < 0 && errno == EINTR);
        if (n < 0) throw std::runtime_error("Direct read failed: " + std::string(std::strerror(errno)));
        ++calls; bytes += n;
        if (uint64_t(n) != std::min<uint64_t>(blockSize, length - start))
            throw std::runtime_error("Short direct read or snapshot changed during read");
        setg(buffer, buffer + offset - start, buffer + n);
        return traits_type::to_int_type(*gptr());
    }
    pos_type seekoff(off_type offset, std::ios_base::seekdir way,
                     std::ios_base::openmode mode) override {
        if (!(mode & std::ios_base::in)) return pos_type(off_type(-1));
        __int128 base = way == std::ios_base::beg ? 0 :
            way == std::ios_base::cur ? position() : length;
        __int128 target = base + offset;
        if (target < 0 || target > length) return pos_type(off_type(-1));
        uint64_t p = uint64_t(target);
        if (eback() && p >= start && p <= start + uint64_t(egptr() - eback()))
            setg(buffer, buffer + (p - start), egptr());
        else { next = p; setg(nullptr, nullptr, nullptr); }
        return pos_type(off_type(p));
    }
    pos_type seekpos(pos_type offset, std::ios_base::openmode mode) override {
        return seekoff(off_type(offset), std::ios_base::beg, mode);
    }
public:
    uint64_t bytes = 0, calls = 0;
    explicit Buffer(const std::string &path) {
        fd = open(path.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("Cannot open direct input: " + std::string(std::strerror(errno)));
        struct stat st{};
        if (fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size < 0) {
            close(fd); fd = -1;
            throw std::runtime_error("Direct input requires a regular snapshot file");
        }
        length = uint64_t(st.st_size);
        // 4 KiB addresses and 8 MiB offsets/lengths meet this backend's
        // alignment contract. Unsupported device/filesystem alignment fails
        // explicitly at pread; no cached descriptor is opened as a fallback.
        buffer = static_cast<char *>(std::aligned_alloc(4096, blockSize));
        if (!buffer) { close(fd); fd = -1; throw std::bad_alloc(); }
    }
    ~Buffer() override { if (fd >= 0) close(fd); std::free(buffer); }
};
DirectInput::DirectInput(const std::string &path) : std::istream(nullptr), buffer_(std::make_unique<Buffer>(path)) {
    rdbuf(buffer_.get());
    exceptions(std::ios_base::badbit);
}
DirectInput::~DirectInput() = default;
uint64_t DirectInput::bytesRead() const { return buffer_->bytes; }
uint64_t DirectInput::readCalls() const { return buffer_->calls; }
}
