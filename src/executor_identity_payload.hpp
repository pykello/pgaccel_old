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
                ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+256);
                std::pmr::vector<Pair> pairs(&budget);uint8_t bitmap[RowGroupSize/8];
                const auto *edge=plan.outgoing[t].empty()?nullptr:&plan.outgoing[t].front();
                for(int g=worker;g<table.RowGroupCount();g+=workers) {
                    const auto &rg=table.GetRowGroup(g);
                    int count=filters[t]?filters[t]->ExecuteSet(rg,bitmap):rg.size;
                    if(!count)continue;
                    const auto *foreign=edge?&IdentityRaw(rg,edge->source):nullptr;
                    const auto *primary=payload->rank?&IdentityRaw(rg,key.column):nullptr;
                    Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                        uint32_t value=edge?payloads[edge->target]->lookup(IdentityRead(*foreign,row)):1;
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
