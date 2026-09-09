#pragma once
#include "direct_literal_filters.hpp"
#include <map>
#include <set>

namespace pgaccel {
// Conservative scope equality: identical bound tables, literal filters and
// direct equality joins. More complex predicates simply decline pruning.
inline bool DirectSharedJoinScopes(const QueryDesc &root) {
    auto samePredicate=[](const Expression &a,const Expression &b) {
        auto af=ExactLiteralFilter(a),bf=ExactLiteralFilter(b);
        if(af || bf) return af && bf && SameDirectLiteral(*af,*bf);
        return a.kind==Expression::EQ && b.kind==Expression::EQ &&
            a.left && a.right && b.left && b.right &&
            a.left->kind==Expression::COLUMN && a.right->kind==Expression::COLUMN &&
            b.left->kind==Expression::COLUMN && b.right->kind==Expression::COLUMN &&
            a.left->column && a.right->column && b.left->column && b.right->column &&
            a.left->column->tableIdx>=0 && a.right->column->tableIdx>=0 &&
            *a.left->column==*b.left->column && *a.right->column==*b.right->column;
    };
    std::set<const QueryDesc *> visited;
    auto visit=[&](auto &&self,const QueryDesc &q)->bool {
        if(!visited.insert(&q).second)return true;
        if(!q.leftJoins.empty() || !q.existenceJoins.empty() ||
           !q.membershipQueries.empty() || !q.materializedSources.empty() ||
           q.tables!=root.tables || q.filterClauses.size()!=root.filterClauses.size() ||
           q.predicates.size()!=root.predicates.size())return false;
        for(size_t i=0;i<q.filterClauses.size();++i)
            if(!SameDirectLiteral(q.filterClauses[i],root.filterClauses[i]))return false;
        for(size_t i=0;i<q.predicates.size();++i)
            if(!samePredicate(*q.predicates[i],*root.predicates[i]))return false;
        for(const auto &scalar:q.scalarQueries) {
            if(!scalar.outerKeys.empty() || !scalar.innerKeys.empty() || !scalar.keyOutputs.empty() ||
               !self(self,*scalar.query))return false;
        }
        return true;
    };
    return visit(visit,root);
}
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
