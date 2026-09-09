#pragma once
#include "direct_literal_filters.hpp"
#include <map>
#include <set>

namespace pgaccel {
// A physical table can be shared by aliases and nested queries. Row removal is
// safe only when every use requires the removed rows to fail. For now restrict
// this rule to single-table scopes, preserving multi-table key sequences.
inline std::map<const ColumnarTable *,std::vector<FilterClause>>
DirectSingleTableFilters(const QueryDesc &root) {
    std::map<const ColumnarTable *,std::vector<FilterClause>> result;
    std::set<const QueryDesc *> visited;
    auto same=[](const FilterClause &a,const FilterClause &b) {
        return a.columnRef.Name()==b.columnRef.Name() && a.op==b.op &&
            a.value==b.value && a.values==b.values;
    };
    auto visit=[&](auto &&self,const QueryDesc &q)->void {
        if(!visited.insert(&q).second)return;
        auto filters=DirectPreparationFilters(q).filterClauses;
        for(size_t i=0;i<q.tables.size();++i) {
            std::vector<FilterClause> required;
            if(q.tables.size()==1 && q.leftJoins.empty() && q.existenceJoins.empty())
                for(const auto &f:filters)if(f.columnRef.tableIdx==int(i))required.push_back(f);
            auto [entry,inserted]=result.try_emplace(q.tables[i],required);
            if(!inserted)std::erase_if(entry->second,[&](const auto &f) {
                return std::none_of(required.begin(),required.end(),[&](const auto &other){return same(f,other);});
            });
        }
        for(const auto &s:q.scalarQueries)self(self,*s.query);
        for(const auto &m:q.membershipQueries)self(self,*m.query);
        for(const auto &m:q.materializedSources)self(self,*m.query);
    };
    visit(visit,root);
    return result;
}
}
