#pragma once
#include "native_column_charge.hpp"
#include "native_snapshot.hpp"
#include <list>
#include <map>
namespace pgaccel {
// The direct loader validates identity and supplies original blocks. Cache
// ownership is separate from references pinned by a currently running query.
class NativeBlockCache {
public:
    struct Key {
        std::optional<NativeSnapshotIdentity> snapshot;
        uint64_t columnOffset=0;
        uint32_t stripe=0;
        TypeNum type=INVALID_TYPE;
        auto operator<=>(const Key &) const = default;
    };
    using Block=std::shared_ptr<const ColumnDataBase>;
private:
    struct Entry { Key key; Block block; uint64_t charge, endOffset; };
    using Queue=std::list<Entry>;
    using Index=std::map<Key,Queue::iterator>;
    Queue entries_;
    Index index_;
    uint64_t limit_,bytes_=0;
    void evict() {
        auto last=std::prev(entries_.end());
        bytes_-=last->charge;index_.erase(last->key);entries_.erase(last);
    }
public:
    explicit NativeBlockCache(uint64_t limit):limit_(limit){}
    NativeBlockCache(const NativeBlockCache &)=delete;
    NativeBlockCache &operator=(const NativeBlockCache &)=delete;
    static std::optional<uint64_t> Charge(const ColumnDataBase &column,const AccelType &type) {
        auto block=NativeColumnCharge(column,type);if(!block)return {};
        constexpr uint64_t overhead=sizeof(Entry)+2*sizeof(void*)+64+
            sizeof(Index::value_type)+4*sizeof(void*)+64;
        if(*block>UINT64_MAX-overhead)return {};
        return *block+overhead;
    }
    Block Find(const Key &key, uint64_t *endOffset = nullptr) {
        if(!key.snapshot)return {};
        auto found=index_.find(key);if(found==index_.end())return {};
        entries_.splice(entries_.begin(),entries_,found->second);
        if(endOffset)*endOffset=found->second->endOffset;
        return found->second->block;
    }
    bool Insert(const Key &key,const ColumnDataP &block,const AccelType &type,
                uint64_t endOffset = 0) {
        if(!key.snapshot || !block || key.type!=type.type_num())return false;
        auto charge=Charge(*block,type);if(!charge || *charge>limit_)return false;
        // An identity/offset/stripe names immutable content. Do not replace a
        // live entry using caller-provided data for the same source location.
        if(Find(key))return true;
        while(bytes_>limit_-*charge)evict();
        entries_.push_front({key,block,*charge,endOffset});
        try { index_.emplace(key,entries_.begin()); }
        catch (...) { entries_.pop_front();throw; }
        bytes_+=*charge;return true;
    }
    void SetLimit(uint64_t limit) {
        limit_=limit;while(bytes_>limit_)evict();
    }
    void Clear() { index_.clear();entries_.clear();bytes_=0; }
    uint64_t Bytes() const { return bytes_; }
    size_t Size() const { return entries_.size(); }
};
}
