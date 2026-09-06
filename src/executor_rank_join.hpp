#pragma once
#include "executor_state.hpp"

namespace pgaccel {
namespace {
// Exact integer membership plus rank maps sparse keys to compact row IDs.
// No hashes, false positives, or key-to-row uniqueness assumptions are used.
class RankJoinIndex {
    int64_t first,last;
    uint64_t cardinality=0;
    std::pmr::vector<uint64_t> bits,rows;
    std::pmr::vector<uint32_t> prefix;
public:
    RankJoinIndex(int64_t lo,int64_t hi,Budget &budget):first(lo),last(hi),bits(&budget),rows(&budget),prefix(&budget) {
        Wide width=Wide(hi)-lo+1;
        if(width<=0 || width>Wide(SIZE_MAX)*64) throw std::invalid_argument("Invalid rank index domain");
        bits.resize(size_t((width+63)/64),0);
    }
    template<bool Concurrent=false> bool insert(int64_t key) {
        if(!prefix.empty())throw std::logic_error("Cannot insert into a finalized rank index");
        if(key<first || key>last) throw std::invalid_argument("Rank index key outside domain");
        uint64_t offset=uint64_t(Wide(key)-first),mask=uint64_t(1)<<(offset%64);
        auto &word=bits[offset/64];
        if constexpr(Concurrent) return !(std::atomic_ref<uint64_t>(word).fetch_or(mask,std::memory_order_relaxed)&mask);
        else {if(word&mask)return false;word|=mask;return true;}
    }
    void finalize() {
        prefix.resize(bits.size());uint64_t offset=0;
        for(size_t i=0;i<bits.size();++i) {
            if(offset>UINT32_MAX)throw std::invalid_argument("Rank index cardinality exceeds 32 bits");
            prefix[i]=uint32_t(offset);offset+=__builtin_popcountll(bits[i]);
        }
        if(offset>UINT32_MAX)throw std::invalid_argument("Rank index cardinality exceeds 32 bits");
        cardinality=offset;rows.resize(cardinality);
    }
    std::optional<uint32_t> rank(int64_t key) const {
        if(prefix.empty())throw std::logic_error("Rank index is not finalized");
        if(key<first || key>last) return {};
        uint64_t offset=uint64_t(Wide(key)-first),mask=uint64_t(1)<<(offset%64),word=bits[offset/64];
        if(!(word&mask)) return {};
        return prefix[offset/64]+__builtin_popcountll(word&(mask-1));
    }
    void setRow(int64_t key,uint64_t row) {
        auto index=rank(key);if(!index)throw std::invalid_argument("Missing rank index key");rows[*index]=row;
    }
    uint64_t row(uint32_t rank) const {return rows[rank];}
    uint64_t size() const {return cardinality;}
    int64_t minimum() const {return first;}
    int64_t maximum() const {return last;}
    const uint64_t *words() const {return bits.data();}
};
}
}
