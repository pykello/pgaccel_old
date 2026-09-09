#pragma once
#include "executor_literal_filter.hpp"
namespace pgaccel {
inline bool SameDirectLiteral(const FilterClause &a,const FilterClause &b) {
    return a.columnRef==b.columnRef && a.op==b.op && a.value==b.value && a.values==b.values;
}
// Necessary literal conditions only: AND combines requirements; OR retains
// their intersection. Never promote a condition from just one disjunct.
inline std::vector<FilterClause> RequiredDirectLiterals(const Expression &e) {
    if(auto literal=ExactLiteralFilter(e))return {*literal};
    if(e.kind==Expression::NOT && e.left && e.left->kind==Expression::LIKE) {
        if(auto literal=ExactLiteralFilter(*e.left)){literal->op=FilterClause::FILTER_NOT_LIKE;return {*literal};}
    }
    if((e.kind!=Expression::AND && e.kind!=Expression::OR) || !e.left || !e.right)return {};
    auto a=RequiredDirectLiterals(*e.left),b=RequiredDirectLiterals(*e.right);
    auto contains=[](const auto &items,const auto &f){return std::any_of(items.begin(),items.end(),[&](const auto &x){return SameDirectLiteral(x,f);});};
    if(e.kind==Expression::AND) {for(const auto &f:b)if(!contains(a,f))a.push_back(f);}
    else std::erase_if(a,[&](const auto &f){return !contains(b,f);});
    return a;
}
inline QueryDesc DirectPreparationFilters(const QueryDesc &query) {
    QueryDesc result=query;
    for(const auto &e:query.predicates)for(const auto &filter:RequiredDirectLiterals(*e))
        if(std::none_of(result.filterClauses.begin(),result.filterClauses.end(),[&](const auto &f){return SameDirectLiteral(f,filter);}))
            result.filterClauses.push_back(filter);
    return result;
}
}
