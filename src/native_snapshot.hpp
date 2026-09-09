#pragma once
#include <array>
#include <cstdint>
#include <compare>
namespace pgaccel {
// Captured from the opened payload descriptor: device, inode, size,
// modification seconds/nanoseconds and change seconds/nanoseconds.
using NativeFileStamp=std::array<uint64_t,7>;
struct NativeSnapshotIdentity {
    std::array<uint64_t,2> uuid;
    NativeFileStamp file;
    auto operator<=>(const NativeSnapshotIdentity &) const = default;
};
}
