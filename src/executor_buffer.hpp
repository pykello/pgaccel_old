#pragma once
#include <memory>
#include <memory_resource>
#include <limits>
#include <type_traits>
#include <utility>

namespace pgaccel {
// Budgeted overwrite storage for trivial records. Every element must be
// written before it is read. Default construction starts object lifetimes
// without a serial value-initialization pass over a large scatter buffer.
template<class T> class OverwriteBuffer {
    static_assert(std::is_trivially_default_constructible_v<T>);
    static_assert(std::is_trivially_destructible_v<T>);
    std::pmr::memory_resource *memory_;
    T *data_ = nullptr;
    size_t size_ = 0;
  public:
    OverwriteBuffer(size_t size, std::pmr::memory_resource &memory) : memory_(&memory), size_(size) {
        if (size > std::numeric_limits<size_t>::max() / sizeof(T)) throw std::bad_array_new_length();
        if (size) {
            data_ = static_cast<T *>(memory_->allocate(size * sizeof(T), alignof(T)));
            std::uninitialized_default_construct_n(data_, size);
        }
    }
    OverwriteBuffer(const OverwriteBuffer &) = delete;
    OverwriteBuffer &operator=(const OverwriteBuffer &) = delete;
    OverwriteBuffer(OverwriteBuffer &&other) noexcept
        : memory_(other.memory_), data_(std::exchange(other.data_, nullptr)), size_(std::exchange(other.size_, 0)) {}
    ~OverwriteBuffer() {
        if (data_) {
            std::destroy_n(data_, size_);
            memory_->deallocate(data_, size_ * sizeof(T), alignof(T));
        }
    }
    T *data() const { return data_; }
    size_t size() const { return size_; }
    T &operator[](size_t index) const { return data_[index]; }
};
} // namespace pgaccel
