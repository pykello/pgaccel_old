#pragma once
#include <cstdint>

namespace pgaccel {
namespace {
// Exact sufficient state for different-supplier EXISTS and late anti-EXISTS.
// The consumer proves the correlation and date predicates before using it.
struct WaitSummary {
    int64_t key,firstSupplier,firstLate=0;uint64_t lateCount=0;
    bool different=false,lateDifferent=false;
    WaitSummary(int64_t key,int64_t supplier):key(key),firstSupplier(supplier) {}
    void observe(int64_t supplier,bool late) {
        different|=supplier!=firstSupplier;
        if(late) {
            if(lateCount)lateDifferent|=supplier!=firstLate;else firstLate=supplier;
            ++lateCount;
        }
    }
    void merge(const WaitSummary &other) {
        different|=other.different || firstSupplier!=other.firstSupplier;
        if(other.lateCount) {
            if(lateCount)lateDifferent|=firstLate!=other.firstLate;else firstLate=other.firstLate;
            lateDifferent|=other.lateDifferent;lateCount+=other.lateCount;
        }
    }
};
}
}
