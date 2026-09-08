#pragma once
#include "executor_literal_filter.hpp"
#include "executor.h"
namespace pgaccel {
// A string can lose its original spelling only when every use in every scope
// observes the same literal predicate, never its value, ordering or identity.
using PredicateColumnMap = std::map<const ColumnarTable *,std::map<std::string,FilterClause>>;
inline PredicateColumnMap PredicateOnlyColumns(const QueryDesc &root) {
    struct Use { bool value=false; std::optional<FilterClause> predicate; };
    std::map<const ColumnarTable *,std::map<std::string,Use>> uses;
    std::set<const QueryDesc *> visited;
    auto visit=[&](auto &&self,const QueryDesc &q)->void {
        if(!visited.insert(&q).second)return;
        auto use=[&](const ColumnRef &c,const FilterClause *filter=nullptr) {
            if(c.tableIdx<0 || c.Type()->type_num()!=STRING_TYPE)return;
            auto &u=uses.at(q.tables.at(c.tableIdx))[c.Name()];
            if(!filter){u.value=true;return;}
            if(u.predicate && (u.predicate->op!=filter->op || u.predicate->value!=filter->value ||
                               u.predicate->values!=filter->values))u.value=true;
            else u.predicate=*filter;
        };
        for(auto table:q.tables)uses.try_emplace(table);
        auto expression=[&](auto &&walk,const std::shared_ptr<Expression> &e,bool predicate)->void {
            if(!e)return;
            if(predicate && e->left && e->left->kind==Expression::COLUMN) {
                if(auto f=ExactLiteralFilter(*e)) {use(f->columnRef,&*f);return;}
            }
            if(e->column)use(*e->column);
            bool boolean=predicate && (e->kind==Expression::AND || e->kind==Expression::OR || e->kind==Expression::NOT);
            walk(walk,e->left,boolean);walk(walk,e->right,boolean);walk(walk,e->condition,false);
        };
        for(const auto &f:q.filterClauses)use(f.columnRef,&f);
        for(const auto &e:q.predicates)expression(expression,e,true);
        for(const auto &j:q.leftJoins)for(const auto &e:j.predicates)expression(expression,e,true);
        for(const auto &j:q.existenceJoins)for(const auto &e:j.predicates)expression(expression,e,true);
        for(const auto &c:q.groupBy)use(c);
        for(const auto &e:q.groupExpressions)expression(expression,e,false);
        for(const auto &a:q.aggregateClauses){if(a.columnRef)use(*a.columnRef);expression(expression,a.expression,false);}
        for(const auto &h:q.having)expression(expression,h.bound,false);
        for(const auto &m:q.membershipQueries){use(m.column);self(self,*m.query);}
        for(const auto &s:q.scalarQueries) {
            for(const auto &c:s.outerKeys)use(c);
            self(self,*s.query);
            for(const auto &c:s.innerKeys)if(c.tableIdx>=0 && c.Type()->type_num()==STRING_TYPE)
                uses[s.query->tables.at(c.tableIdx)][c.Name()].value=true;
        }
        for(const auto &m:q.materializedSources)self(self,*m.query);
    };
    visit(visit,root);
    PredicateColumnMap result;
    for(const auto &[table,columns]:uses)for(const auto &[name,u]:columns)
        if(!u.value && u.predicate)result[table].emplace(name,*u.predicate);
    return result;
}
inline Result<ColumnDataP> PredicateDictionary(const ColumnDataP &source,
                                             FilterClause filter,bool avx) {
    if(source->type!=ColumnDataBase::DICT_COLUMN_DATA || filter.columnRef.Type()->type_num()!=STRING_TYPE)
        return Status::Invalid("Predicate dictionary needs native string data");
    const auto &input=static_cast<const DictColumnData<StringType>&>(*source);
    filter.columnRef.tableIdx=0;filter.columnRef.columnIdx=0;
    auto node=CreateFilterNode({filter},avx);
    RowGroup batch;batch.columns={source};batch.size=source->size;
    uint8_t bitmap[RowGroupSize/8]{};node->ExecuteSet(batch,bitmap);
    auto output=std::make_shared<DictColumnData<StringType>>();
    output->type=ColumnDataBase::DICT_COLUMN_DATA;output->size=batch.size;output->valueType=input.valueType;
    int representatives[2]{-1,-1};
    for(int row=0;row<batch.size;++row) {
        int match=(bitmap[row/8]>>(row%8))&1;
        if(representatives[match]<0)representatives[match]=input.bytesPerValue()==1?input.values[row]
            :reinterpret_cast<const uint16_t *>(input.values)[row];
    }
    for(int code:representatives)if(code>=0)output->dict.push_back(input.dict[code]);
    std::sort(output->dict.begin(),output->dict.end());
    uint8_t codes[2]{};
    for(int i=0;i<2;++i)if(representatives[i]>=0)
        codes[i]=std::lower_bound(output->dict.begin(),output->dict.end(),input.dict[representatives[i]])-output->dict.begin();
    output->values=static_cast<uint8_t *>(AllocateAligned(512,batch.size));
    for(int row=0;row<batch.size;++row)output->values[row]=codes[(bitmap[row/8]>>(row%8))&1];
    return ColumnDataP(output);
}
}
