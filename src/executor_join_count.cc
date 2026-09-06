#include "executor_date_order.hpp"
#include "executor_buffer.hpp"
#include "executor_result.hpp"
#include "util.h"

namespace pgaccel {
namespace {
bool DictionaryCondition(const Expression &e,int table,std::optional<ColumnRef> &column) {
    if(e.kind==Expression::COLUMN) {
        if(!e.column || e.column->tableIdx!=table || e.type->type_num()!=STRING_TYPE || (column && *column!=*e.column))return false;
        column=e.column;return true;
    }
    if(e.kind==Expression::CONSTANT)return e.type->type_num()==STRING_TYPE || e.type->type_num()==INT64_TYPE;
    if(e.kind!=Expression::EQ && e.kind!=Expression::NE && e.kind!=Expression::AND &&
        e.kind!=Expression::OR && e.kind!=Expression::NOT && e.kind!=Expression::LIKE)return false;
    for(const auto &child:{e.left,e.right,e.condition})if(child && !DictionaryCondition(*child,table,column))return false;
    return true;
}
std::pair<int64_t,int64_t> CountKeyBounds(const RowGroup &rg,const ColumnRef &key) {
    auto data=rg.columns[key.columnIdx].get();if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
    if(key.Type()->type_num()==INT64_TYPE) {auto d=static_cast<const RawColumnData<Int64Type> *>(data);return {d->minValue,d->maxValue};}
    if(key.Type()->type_num()==INT32_TYPE) {auto d=static_cast<const RawColumnData<Int32Type> *>(data);return {d->minValue,d->maxValue};}
    throw NotDense{};
}
}
std::optional<Result<QueryOutput>> ExecuteJoinCount(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || !q.existenceJoins.empty() || !q.leftJoins.empty() || !q.scalarQueries.empty() ||
        !q.membershipQueries.empty() || !q.grouped || q.groupExpressions.size()!=1 ||
        q.groupExpressions[0]->kind!=Expression::COLUMN || q.aggregateClauses.size()>8)return {};
    auto group=*q.groupExpressions[0]->column;int probe=group.tableIdx,build=1-probe;
    if(probe<0 || probe>1 || (group.Type()->type_num()!=STRING_TYPE && group.Type()->type_num()!=DATE_TYPE))return {};
    std::optional<ColumnRef> buildKey,probeKey,conditionColumn;
    std::vector<std::shared_ptr<Expression>> comparisons;
    for(const auto &p:q.predicates) {
        if(p->kind==Expression::EQ && p->left->column && p->right->column && p->left->column->tableIdx!=p->right->column->tableIdx) {
            if(buildKey)return {};auto a=*p->left->column,b=*p->right->column;if(a.tableIdx!=build)std::swap(a,b);
            if(a.tableIdx!=build || b.tableIdx!=probe || Scale(a.Type().get())!=Scale(b.Type().get()))return {};
            buildKey=a;probeKey=b;
        } else if(DateOrderPredicate::supports(*p,probe))comparisons.push_back(p);
        else return {};
    }
    if(!buildKey || comparisons.size()>8)return {};
    std::array<std::shared_ptr<Expression>,8> conditions;
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.expression && a.expression->column && *a.expression->column==group)continue;
        if(a.type==AggregateClause::AGGREGATE_COUNT && !a.expression)continue;
        auto e=a.expression;
        if(a.type!=AggregateClause::AGGREGATE_SUM || !e || e->kind!=Expression::CASE || !e->condition || !e->left || !e->right ||
            e->left->kind!=Expression::CONSTANT || e->right->kind!=Expression::CONSTANT ||
            e->left->type->type_num()!=INT64_TYPE || e->right->type->type_num()!=INT64_TYPE ||
            e->left->literal!="1" || e->right->literal!="0" ||
            !DictionaryCondition(*e->condition,build,conditionColumn))return {};
        conditions[i]=e->condition;
    }
    for(const auto &f:q.filterClauses)if(f.columnRef.tableIdx!=probe)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);QueryResults results(q,budget);
        auto &bt=*q.tables[build],&pt=*q.tables[probe];int64_t low=INT64_MAX,high=INT64_MIN;uint64_t rows=0;
        for(int g=0;g<bt.RowGroupCount();++g) {
            const auto &rg=bt.GetRowGroup(g);if(!rg.size)continue;auto bounds=CountKeyBounds(rg,*buildKey);
            low=std::min(low,bounds.first);high=std::max(high,bounds.second);rows+=rg.size;
        }
        if(!rows)return results.finish();Wide width=Wide(high)-low+1;
        if(width<=0 || width>Wide(rows)*8 || width>(Wide(1)<<34))return {};
        OverwriteBuffer<uint64_t> present(size_t((width+63)/64),budget);
        std::array<std::optional<OverwriteBuffer<uint64_t>>,8> bits;
        for(size_t i=0;i<conditions.size();++i)if(conditions[i])bits[i].emplace(present.size(),budget);
        int workers=parallel?std::min(8,std::max(1,bt.RowGroupCount())):1;
        RunTasks(workers,[&](int worker) {
            size_t begin=present.size()*worker/workers,end=present.size()*(worker+1)/workers;
            std::fill(present.data()+begin,present.data()+end,0);
            for(auto &b:bits)if(b)std::fill(b->data()+begin,b->data()+end,0);return true;
        });
        RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,4096);
            for(int g=worker;g<bt.RowGroupCount();g+=workers) {
                const auto &rg=bt.GetRowGroup(g);if(!rg.size)continue;const DictColumnDataBase *dict=nullptr;int codeWidth=0;
                std::pmr::vector<uint8_t> labels(&budget);
                if(conditionColumn) {
                    auto data=rg.columns[conditionColumn->columnIdx].get();if(data->type!=ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
                    dict=static_cast<const DictColumnDataBase *>(data);codeWidth=dict->bytesPerValue();labels.resize(dict->dictSize());
                    for(size_t c=0;c<labels.size();++c)for(size_t i=0;i<conditions.size();++i)if(conditions[i]) {
                        auto label=Label(dict,c,STRING_TYPE);
                        if(EvaluateWith(*conditions[i],[&](const ColumnRef &){return label;}).number)labels[c]|=uint8_t(1)<<i;
                    }
                } else {
                    labels.resize(1);for(size_t i=0;i<conditions.size();++i)if(conditions[i] && Evaluate(*conditions[i],RowGroup{},0).number)labels[0]|=uint8_t(1)<<i;
                }
                uint64_t word=UINT64_MAX,seen=0;std::array<uint64_t,8> masks{};
                auto flush=[&] {
                    if(!seen)return;
                    if(std::atomic_ref<uint64_t>(present[word]).fetch_or(seen,std::memory_order_relaxed)&seen)throw NotDense{};
                    for(size_t i=0;i<bits.size();++i)if(masks[i])std::atomic_ref<uint64_t>((*bits[i])[word]).fetch_or(masks[i],std::memory_order_relaxed);
                    seen=0;masks.fill(0);
                };
                auto data=rg.columns[buildKey->columnIdx].get();auto raw=static_cast<const RawColumnDataBase *>(data);
                auto scan=[&]<class T>() {
                    const auto keys=reinterpret_cast<const T *>(raw->values);
                    for(int row=0;row<rg.size;++row) {
                        Wide delta=Wide(keys[row])-low;if(delta<0 || delta>=width)throw NotDense{};
                        uint64_t next=uint64_t(delta)/64,bit=uint64_t(1)<<(uint64_t(delta)%64);
                        if(next!=word){flush();word=next;}
                        if(seen&bit)throw NotDense{};seen|=bit;
                        int code=dict?(codeWidth==1?dict->values[row]:reinterpret_cast<const uint16_t *>(dict->values)[row]):0;
                        for(uint8_t mask=labels[code];mask;mask&=mask-1)masks[__builtin_ctz(mask)]|=bit;
                    }
                };
                switch(raw->bytesPerValue) {case 1:scan.template operator()<int8_t>();break;case 2:scan.template operator()<int16_t>();break;
                    case 4:scan.template operator()<int32_t>();break;case 8:scan.template operator()<int64_t>();break;default:throw NotDense{};}
                flush();
            }
            return true;
        });
        auto filter=q.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(q.filterClauses,avx);
        using Counts=std::array<uint64_t,8>;workers=parallel?std::min(8,std::max(1,pt.RowGroupCount())):1;
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,65536);
            std::pmr::unordered_map<Cell,Counts,Hash> totals(&budget);uint8_t bitmap[RowGroupSize/8];uint32_t selected[Block];
            for(int g=worker;g<pt.RowGroupCount();g+=workers) {
                const auto &rg=pt.GetRowGroup(g);int count=filter?filter->ExecuteSet(rg,bitmap):rg.size;if(!count)continue;
                auto data=rg.columns[group.columnIdx].get();if(data->type!=ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
                auto dict=static_cast<const DictColumnDataBase *>(data);if(dict->dictSize()>256)throw NotDense{};int codeWidth=dict->bytesPerValue();
                std::array<Counts,256> counts{};std::array<bool,256> visited{};
                std::vector<DateOrderPredicate> predicates;for(const auto &p:comparisons)predicates.emplace_back(*p,rg,budget);
                auto keyData=rg.columns[probeKey->columnIdx].get();
                auto raw=keyData->type==ColumnDataBase::RAW_COLUMN_DATA?static_cast<const RawColumnDataBase *>(keyData):nullptr;
                int n=0;auto consume=[&] {
                    for(const auto &p:predicates)n=p.select(selected,n,selected,avx);
                    int row=0;
                    if(avx && raw && raw->bytesPerValue==4 && low>=0 && high<=INT32_MAX && dict->dictSize()<=16)
                    for(;row+16<=n;row+=16) {
                        auto ids=_mm512_loadu_si512(selected+row),keys=_mm512_i32gather_epi32(ids,raw->values,4);
                        auto delta=_mm512_sub_epi32(keys,_mm512_set1_epi32(low)),positions=_mm512_srli_epi32(delta,5);
                        auto shifts=_mm512_and_si512(delta,_mm512_set1_epi32(31));
                        auto valid=_mm512_cmpge_epi32_mask(keys,_mm512_set1_epi32(low)) & _mm512_cmple_epi32_mask(keys,_mm512_set1_epi32(high));
                        auto words=_mm512_mask_i32gather_epi32(_mm512_setzero_si512(),valid,positions,present.data(),4);
                        auto live=valid & _mm512_test_epi32_mask(_mm512_srlv_epi32(words,shifts),_mm512_set1_epi32(1));
                        if(!live)continue;
                        std::array<__mmask16,8> matches{};
                        for(size_t i=0;i<q.aggregateClauses.size();++i) {
                            if(bits[i]) {
                                auto values=_mm512_mask_i32gather_epi32(_mm512_setzero_si512(),live,positions,bits[i]->data(),4);
                                matches[i]=live & _mm512_test_epi32_mask(_mm512_srlv_epi32(values,shifts),_mm512_set1_epi32(1));
                            } else if(q.aggregateClauses[i].type==AggregateClause::AGGREGATE_COUNT)matches[i]=live;
                        }
                        alignas(64)uint32_t codes[16];
                        for(int lane=0;lane<16;++lane)codes[lane]=codeWidth==1?dict->values[selected[row+lane]]:
                            reinterpret_cast<const uint16_t *>(dict->values)[selected[row+lane]];
                        auto groups=_mm512_load_si512(codes);
                        for(size_t code=0;code<dict->dictSize();++code) {
                            auto mask=live & _mm512_cmpeq_epi32_mask(groups,_mm512_set1_epi32(code));if(!mask)continue;
                            visited[code]=true;
                            for(size_t i=0;i<q.aggregateClauses.size();++i)counts[code][i]+=__builtin_popcount(mask & matches[i]);
                        }
                    }
                    for(;row<n;++row) {
                        Wide delta=Read(rg,*probeKey,selected[row]).number-low;if(delta<0 || delta>=width)continue;
                        uint64_t word=uint64_t(delta)/64,bit=uint64_t(1)<<(uint64_t(delta)%64);if(!(present[word]&bit))continue;
                        int code=codeWidth==1?dict->values[selected[row]]:reinterpret_cast<const uint16_t *>(dict->values)[selected[row]];visited[code]=true;
                        for(size_t i=0;i<q.aggregateClauses.size();++i) {
                            if(bits[i])counts[code][i]+=(bool)((*bits[i])[word]&bit);
                            else if(q.aggregateClauses[i].type==AggregateClause::AGGREGATE_COUNT)++counts[code][i];
                        }
                    }
                    n=0;
                };
                Selected(rg,count==rg.size?nullptr:bitmap,[&](int row){selected[n++]=row;if(n==Block)consume();});if(n)consume();
                for(size_t code=0;code<dict->dictSize();++code)if(visited[code]) {
                    auto &out=totals[Label(dict,code,group.Type()->type_num())];for(size_t i=0;i<out.size();++i)out[i]+=counts[code][i];
                }
            }
            return totals;
        });
        std::pmr::unordered_map<Cell,Counts,Hash> totals(&budget);
        for(const auto &part:parts)for(const auto &[label,counts]:part)for(size_t i=0;i<counts.size();++i)totals[label][i]+=counts[i];
        for(const auto &[label,counts]:totals)results.addLazy([&](size_t i) {
            FinalCell cell;const auto &a=q.aggregateClauses[i];cell.type=a.expression?a.expression->type.get():nullptr;
            if(a.type==AggregateClause::AGGREGATE_PROJECT)cell.value=label;else cell.value.number=counts[i];return cell;
        });
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
