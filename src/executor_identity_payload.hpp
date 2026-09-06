#pragma once
#include "executor_unique_key_graph.hpp"
#include "executor_rank_join.hpp"
#include "executor_bitmap_probe.hpp"
#include "executor_compact_unsigned.hpp"

namespace pgaccel {
namespace {
int64_t IdentityRead(const RawColumnDataBase &data,int row) {
    switch(data.bytesPerValue) {
    case 1:return reinterpret_cast<const int8_t *>(data.values)[row];
    case 2:return reinterpret_cast<const int16_t *>(data.values)[row];
    case 4:return reinterpret_cast<const int32_t *>(data.values)[row];
    case 8:return reinterpret_cast<const int64_t *>(data.values)[row];
    default:throw NotDense{};
    }
}
const RawColumnDataBase &IdentityRaw(const RowGroup &rg,const ColumnRef &column) {
    auto data=rg.columns[column.columnIdx].get();
    if(data->type!=ColumnDataBase::RAW_COLUMN_DATA || data->size!=rg.size) throw NotDense{};
    return *static_cast<const RawColumnDataBase *>(data);
}
struct IdentityPayload {
    int64_t first=0,last=0;
    uint64_t selected=0;
    std::optional<CompactUnsignedArray> dense;
    std::pmr::vector<uint64_t> bits;
    std::unique_ptr<RankJoinIndex> rank;
    explicit IdentityPayload(Budget &budget):bits(&budget) {}
    uint32_t lookup(int64_t key) const {
        if(key<first || key>last) return 0;
        if(rank) {auto id=rank->rank(key);return id?uint32_t(rank->row(*id)):0;}
        return dense->get(uint64_t(Wide(key)-first));
    }
    BitmapDomain domain() const {return {first,last,rank?rank->words():bits.data()};}
};

// Separate independent gathers from the dependent bitmap insertions and
// payload writes. Each full vector initializes its complete output region.
[[gnu::noinline]] void GatherIdentityBlock(const RawColumnDataBase &foreign,
    const CompactUnsignedArray &child,int64_t low,int64_t high,int begin,int n,
    const uint8_t *bitmap,uint32_t *values) {
    auto first=_mm512_set1_epi32(low),last=_mm512_set1_epi32(high);
    const int width=foreign.bytesPerValue;const auto base=foreign.values;
    for(int offset=0;offset<n;offset+=16) {
        int row=begin+offset;uint16_t mask=0xffff;
        if(bitmap)std::memcpy(&mask,bitmap+row/8,2);
        auto found=_mm512_setzero_si512();
        if(mask) {
            __m512i keys;
            if(width==1)keys=_mm512_cvtepi8_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i *>(base+row)));
            else if(width==2)keys=_mm512_cvtepi16_epi32(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(base+row*2)));
            else keys=_mm512_loadu_si512(base+row*4);
            mask&=_mm512_cmpge_epi32_mask(keys,first)&_mm512_cmple_epi32_mask(keys,last);
            if(mask)found=child.gather(_mm512_sub_epi32(keys,first),mask);
        }
        _mm512_storeu_si512(values+offset,found);
    }
}

std::vector<std::unique_ptr<IdentityPayload>> BuildIdentityPayloads(
    const QueryDesc &q,const UniqueKeyJoinGraph &plan,int fact,const std::vector<int> &order,
    const std::vector<int> &centerOf,const std::vector<const uint8_t *> &accepted,
    const std::vector<FilterNodeP> &filters,Budget &budget,bool avx,bool parallel) {
        std::vector<std::unique_ptr<IdentityPayload>> payloads(q.tables.size());
        for(int t:order) {
            int center=centerOf[t];
            const auto &table=*q.tables[t];auto &key=*plan.keys[t];
            auto payload=std::make_unique<IdentityPayload>(budget);
            std::pmr::vector<uint64_t> offsets(table.RowGroupCount(),0,&budget);
            if(key.sequence.active()) {
                payload->first=key.sequence.minimum();payload->last=key.sequence.maximum();
                payload->dense.emplace(key.sequence.size(),plan.keys[center]->sequence.size(),budget);
                key.sequence.forEachStripe([&](uint32_t g,uint64_t offset){offsets[g]=offset;});
            } else {
                payload->first=INT64_MAX;payload->last=INT64_MIN;uint64_t size=0;
                for(int g=0;g<table.RowGroupCount();++g) {
                    const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
                    const auto &raw=IdentityRaw(rg,key.column);size+=rg.size;
                    payload->first=std::min(payload->first,IdentityRead(raw,0));
                    payload->last=std::max(payload->last,IdentityRead(raw,rg.size-1));
                }
                Wide width=Wide(payload->last)-payload->first+1;
                if(width<=0 || width>Wide(size)*8 || width>(Wide(1)<<32))throw NotDense{};
                payload->rank=std::make_unique<RankJoinIndex>(payload->first,payload->last,budget);
            }
            struct Pair {int64_t key;uint32_t value;};
            int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
            auto parts=RunTasks(workers,[&](int worker) {
                ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+4096);
                std::pmr::vector<Pair> pairs(&budget);uint8_t bitmap[RowGroupSize/8];
                const auto *edge=plan.outgoing[t].empty()?nullptr:&plan.outgoing[t].front();
                for(int g=worker;g<table.RowGroupCount();g+=workers) {
                    const auto &rg=table.GetRowGroup(g);
                    int count=filters[t]?filters[t]->ExecuteSet(rg,bitmap):rg.size;
                    if(!count)continue;
                    const auto *foreign=edge?&IdentityRaw(rg,edge->source):nullptr;
                    const auto *primary=payload->rank?&IdentityRaw(rg,key.column):nullptr;
                    auto emit=[&](int row,uint32_t value) {
                        if(!value)return;
                        if(t==center) {
                            uint64_t logical=offsets[g]+row;
                            if(accepted[t] && !accepted[t][logical])return;
                            value=uint32_t(logical+1);
                        }
                        if(primary) {
                            int64_t k=IdentityRead(*primary,row);
                            payload->rank->insert<true>(k);pairs.push_back({k,value});
                        } else payload->dense->set(offsets[g]+row,value);
                    };
                    const auto *child=edge?payloads[edge->target].get():nullptr;
                    bool vectorable=avx && foreign && child->dense &&
                        (foreign->bytesPerValue==1 || foreign->bytesPerValue==2 || foreign->bytesPerValue==4) &&
                        child->first>=INT32_MIN && child->last<=INT32_MAX && Wide(child->last)-child->first<=INT32_MAX;
                    if(vectorable) {
                        auto lanes=_mm512_setr_epi32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
                        alignas(64)uint32_t selected[16],values[16],block[Block];int row=0;
                        while(row+16<=rg.size) {
                            int n=std::min(Block,(rg.size-row)&~15);
                            GatherIdentityBlock(*foreign,*child->dense,child->first,child->last,row,n,
                                count==rg.size?nullptr:bitmap,block);
                            for(int offset=0;offset<n;offset+=16) {
                                auto found=_mm512_load_si512(block+offset);
                                uint16_t mask=_mm512_cmpneq_epi32_mask(found,_mm512_setzero_si512());
                                if(!mask)continue;
                                _mm512_mask_compressstoreu_epi32(selected,mask,_mm512_add_epi32(lanes,_mm512_set1_epi32(row+offset)));
                                _mm512_mask_compressstoreu_epi32(values,mask,found);
                                int accepted=__builtin_popcount(mask);
                                for(int i=0;i<accepted;++i)emit(selected[i],values[i]);
                            }
                            row+=n;
                        }
                        for(;row<rg.size;++row)if(count==rg.size || (bitmap[row/8]>>(row%8)&1))
                            emit(row,child->lookup(IdentityRead(*foreign,row)));
                    } else Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                        emit(row,child?child->lookup(IdentityRead(*foreign,row)):1);
                    });
                }
                return pairs;
            });
            if(payload->rank) {
                payload->rank->finalize();payload->selected=payload->rank->size();
                RunTasks(workers,[&](int worker) {
                    for(const auto &pair:parts[worker])payload->rank->setRow(pair.key,pair.value);
                    return true;
                });
            } else if(std::any_of(plan.outgoing[fact].begin(),plan.outgoing[fact].end(),
                                 [&](const auto &edge){return edge.target==t;})) {
                // Intermediate dimensions are looked up through their payload
                // only. Membership bits are consumed by the fact's two arms.
                payload->bits.resize((payload->dense->size()+63)/64,0);
                for(size_t i=0;i<payload->dense->size();++i)if(payload->dense->get(i)) {
                    payload->bits[i/64]|=uint64_t(1)<<(i%64);++payload->selected;
                }
            }
            payloads[t]=std::move(payload);
        }
        return payloads;
}
}
}
