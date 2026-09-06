#pragma once
#include "executor_small_group_key.hpp"
namespace pgaccel {
namespace {
// Keep result-order normalization in an independent function.
[[gnu::noinline]] std::pmr::vector<uint32_t> OrderDistinctGroups(const QueryDesc &q,
    const SmallGroupKey &groups,const std::vector<int> &outputComponents,
    const std::pmr::vector<uint64_t> &counts,Budget &budget) {
    std::pmr::vector<uint32_t> ordered(&budget);
    for(size_t group=0;group<counts.size();++group)if(counts[group])ordered.push_back(group);
    auto value=[&](uint32_t group,size_t output)->uint64_t {
        int component=outputComponents[output];return component<0?counts[group]:groups.ordinal(group,component);
    };
        struct OrderPart {int component;unsigned bits;bool descending;};
        std::vector<OrderPart> parts;std::array<bool,5> seen{};
        uint64_t maximum=0;for(auto group:ordered)maximum=std::max(maximum,counts[group]);
        unsigned groupBits=std::bit_width(groups.slots()-1),totalBits=groupBits;
        auto appendOrder=[&](size_t output,bool descending) {
            int component=outputComponents[output],slot=component+1;if(seen[slot])return;seen[slot]=true;
            unsigned bits=component<0?std::bit_width(maximum):groups.componentBits(component);
            totalBits+=bits;parts.push_back({component,bits,descending});
        };
        for(const auto &o:q.orderBy)appendOrder(o.output,o.descending);
        for(size_t i=0;i<q.aggregateClauses.size();++i)appendOrder(i,false);
        if(totalBits<=64) {
            std::pmr::vector<uint64_t> keys(&budget);keys.reserve(ordered.size());
            for(auto group:ordered) {
                uint64_t key=0;
                for(const auto &part:parts) {
                    uint64_t ordinal=part.component<0?counts[group]:groups.ordinal(group,part.component);
                    if(!part.bits)continue;
                    if(part.descending)ordinal^=part.bits==64?UINT64_MAX:(uint64_t(1)<<part.bits)-1;
                    key=part.bits==64?ordinal:(key<<part.bits)|ordinal;
                }
                keys.push_back((key<<groupBits)|group);
            }
            std::sort(keys.begin(),keys.end());
            for(size_t i=0;i<keys.size();++i)ordered[i]=keys[i]&(groups.slots()-1);
        } else {
        std::sort(ordered.begin(),ordered.end(),[&](uint32_t a,uint32_t b) {
            for(const auto &o:q.orderBy) {
                auto x=value(a,o.output),y=value(b,o.output);
                if(x!=y)return o.descending?x>y:x<y;
            }
            for(size_t i=0;i<q.aggregateClauses.size();++i) {
                auto x=value(a,i),y=value(b,i);if(x!=y)return x<y;
            }
            return false;
        });
        }
    return ordered;
}
}
}
