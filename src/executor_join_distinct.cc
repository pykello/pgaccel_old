#include "executor_small_group_key.hpp"
#include "executor_packed_distinct.hpp"
#include "executor_boolean_bitmap.hpp"
#include "executor_literal_filter.hpp"
#include "executor_sequence_join.hpp"
#include "executor_membership.hpp"
#include "executor_result.hpp"
#include <atomic>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteJoinDistinct(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || !q.grouped || q.groupExpressions.empty() || q.groupExpressions.size()>4 ||
        !q.having.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.scalarQueries.empty() ||
        !q.materializedSources.empty() || q.membershipQueries.size()>1)return {};
    std::vector<ColumnRef> columns;
    for(const auto &e:q.groupExpressions){if(!e->column)return {};columns.push_back(*e->column);}
    int dimension=columns.front().tableIdx,fact=1-dimension,countOutput=-1;
    for(const auto &c:columns)if(c.tableIdx!=dimension)return {};
    ColumnRef counted,primary,foreign;
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef && a.expression && a.expression->kind==Expression::COLUMN &&
            std::find(columns.begin(),columns.end(),*a.columnRef)!=columns.end())continue;
        if(a.type!=AggregateClause::AGGREGATE_COUNT_DISTINCT || countOutput>=0 || !a.columnRef || !a.expression ||
            a.expression->kind!=Expression::COLUMN || a.columnRef->tableIdx!=fact)return {};
        countOutput=i;counted=*a.columnRef;
    }
    if(countOutput<0)return {};
    bool joined=false;std::vector<std::shared_ptr<Expression>> local;
    for(const auto &e:q.predicates) {
        if(e->kind==Expression::EQ && e->left->column && e->right->column && e->left->column->tableIdx!=e->right->column->tableIdx) {
            if(joined)return {};primary=*e->left->column;foreign=*e->right->column;
            if(primary.tableIdx!=dimension)std::swap(primary,foreign);joined=true;
        } else local.push_back(e);
    }
    if(!joined)return {};
    for(const auto &c:{primary,foreign,counted})if(c.Type()->type_num()!=INT32_TYPE && c.Type()->type_num()!=INT64_TYPE)return {};
    if(!q.membershipQueries.empty()) {
        const auto &m=q.membershipQueries.front();
        if(m.column!=counted || !m.resultType || (m.resultType->type_num()!=INT32_TYPE && m.resultType->type_num()!=INT64_TYPE))return {};
    }
    for(const auto &f:q.filterClauses)if(f.columnRef.tableIdx!=dimension)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        const auto &dim=*q.tables[dimension],&facts=*q.tables[fact];uint64_t rows=0,factRows=0;
        for(int g=0;g<dim.RowGroupCount();++g)rows+=dim.GetRowGroup(g).size;
        for(int g=0;g<facts.RowGroupCount();++g)factRows+=facts.GetRowGroup(g).size;
        SequenceJoinIndex sequence(&budget);if(!sequence.prepare(dim,primary,rows,avx))return {};
        SmallGroupKey groups(dim,columns,budget);
        BooleanBitmapProgram filter(budget,avx);int root=filter.truth();
        for(const auto &f:q.filterClauses)root=filter.combine(true,root,filter.leaf(f));
        for(const auto &e:local) {
            auto f=ExactLiteralFilter(*e);
            if(!f && e->kind==Expression::NOT && e->left && e->left->kind==Expression::LIKE) {
                f=ExactLiteralFilter(*e->left);if(f)f->op=FilterClause::FILTER_NOT_LIKE;
            }
            if(f && f->columnRef.tableIdx!=dimension)throw NotDense{};
            root=filter.combine(true,root,f?filter.leaf(*f):filter.compile(*e,dimension));
        }
        int workers=parallel?std::min(8,std::max(1,facts.RowGroupCount())):1;
        std::pmr::vector<uint32_t> groupIds(rows,UINT32_MAX,&budget);
        std::pmr::vector<uint64_t> representatives(groups.slots(),UINT64_MAX,&budget);
        auto selected=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);std::pmr::vector<uint64_t> bits((filter.bytes()+7)/8,0,&budget);
            std::array<int,64> counts;uint64_t selected=0;
            sequence.forEachStripe([&](uint32_t g,uint64_t offset) {
                if(g%workers!=worker)return;const auto &rg=dim.GetRowGroup(g);
                filter.evaluate(rg,reinterpret_cast<uint8_t *>(bits.data()),counts);
                auto bitmap=filter.bitmap(reinterpret_cast<uint8_t *>(bits.data()),root);
                Selected(rg,counts[root]==rg.size?nullptr:bitmap,[&](int row) {
                    auto id=groups.word(g,row);groupIds[offset+row]=id;++selected;
                    std::atomic_ref<uint64_t>(representatives[id]).store((uint64_t(g)<<32)|uint32_t(row),std::memory_order_relaxed);
                });
            });return selected;
        });
        MembershipSet membership(budget,0);uint64_t spillBytes=0;
        if(!q.membershipQueries.empty()) {
            auto child=*q.membershipQueries.front().query;child.numericMembershipOutput=true;
            child.memoryBudgetBytes=std::min(child.memoryBudgetBytes,budget.limit-budget.used.load());
            child.allowSpill=child.allowSpill && q.allowSpill;child.spillByteBudget=std::min(child.spillByteBudget,q.spillByteBudget);
            auto output=ExecuteQuery(child,avx,parallel);if(!output.ok())return Status::Invalid("Distinct membership: "+output.status().Message());
            if(!output->numericMembership)return {};
            spillBytes=output->spillBytes;if(spillBytes>q.spillByteBudget)return Status::Invalid("Subquery spill budget exceeded");
            Scratch childMemory(budget,output->peakMemoryBytes);
            membership.assignNumbers(*output->numericMembership,0,budget);
        }
        int64_t low=INT64_MAX,high=INT64_MIN;
        for(int g=0;g<facts.RowGroupCount();++g) {
            const auto &rg=facts.GetRowGroup(g);if(!rg.size)continue;
            for(const auto &c:{counted,foreign})if(rg.columns[c.columnIdx]->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
            auto data=rg.columns[counted.columnIdx].get();
            if(counted.Type()->type_num()==INT32_TYPE) {
                auto raw=static_cast<const RawColumnData<Int32Type> *>(data);low=std::min(low,int64_t(raw->minValue));high=std::max(high,int64_t(raw->maxValue));
            } else {auto raw=static_cast<const RawColumnData<Int64Type> *>(data);low=std::min(low,raw->minValue);high=std::max(high,raw->maxValue);}
        }
        if(low<=high && Wide(high)-low>UINT32_MAX)throw NotDense{};
        uint64_t selectedRows=0;for(auto n:selected)selectedRows+=n;
        auto inputs=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,2*Block*sizeof(int64_t)+4096);
            std::pmr::vector<uint64_t> pairs(&budget);
            pairs.reserve(std::min<uint64_t>(factRows,Wide(factRows)*selectedRows/rows)/workers);
            alignas(64)int64_t keys[Block],values[Block];
            auto decode=[&](const RowGroup &rg,const ColumnRef &column,int begin,int n,int64_t *out) {
                auto raw=static_cast<const RawColumnDataBase *>(rg.columns[column.columnIdx].get());
                switch(raw->bytesPerValue) {
                case 1:DecodeNumbers<int8_t,false>(raw,begin,n,out,avx);break;
                case 2:DecodeNumbers<int16_t,false>(raw,begin,n,out,avx);break;
                case 4:DecodeNumbers<int32_t,false>(raw,begin,n,out,avx);break;
                case 8:DecodeNumbers<int64_t,false>(raw,begin,n,out,avx);break;
                default:throw NotDense{};
                }
            };
            for(int g=worker;g<facts.RowGroupCount();g+=workers) {
                const auto &rg=facts.GetRowGroup(g);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int n=std::min(Block,rg.size-begin);decode(rg,foreign,begin,n,keys);decode(rg,counted,begin,n,values);
                    int i=0;
                    if(avx && (q.membershipQueries.empty() || !membership.bits.empty() || membership.values.empty())) {
                        alignas(64)uint64_t packed[8];
                        for(;i+8<=n;i+=8) {
                            auto key=_mm512_load_si512(keys+i),value=_mm512_load_si512(values+i);
                            auto slot=_mm512_sub_epi64(key,_mm512_set1_epi64(sequence.minimum()));
                            __mmask8 valid=_mm512_cmp_epu64_mask(slot,_mm512_set1_epi64(rows),_MM_CMPINT_LT);
                            auto group=_mm512_cvtepu32_epi64(_mm512_mask_i64gather_epi32(_mm256_set1_epi32(-1),valid,slot,groupIds.data(),4));
                            valid&=_mm512_cmpneq_epi64_mask(group,_mm512_set1_epi64(UINT32_MAX));
                            if(!q.membershipQueries.empty()) {
                                __mmask8 found=0;
                                if(!membership.bits.empty()) {
                                    auto delta=_mm512_sub_epi64(value,_mm512_set1_epi64(membership.first));
                                    auto in=valid & _mm512_cmpge_epi64_mask(value,_mm512_set1_epi64(membership.first)) &
                                        _mm512_cmple_epi64_mask(value,_mm512_set1_epi64(membership.last));
                                    auto words=_mm512_mask_i64gather_epi64(_mm512_setzero_si512(),in,
                                        _mm512_srli_epi64(delta,6),membership.bits.data(),8);
                                    found=in & _mm512_test_epi64_mask(_mm512_srlv_epi64(words,
                                        _mm512_and_si512(delta,_mm512_set1_epi64(63))),_mm512_set1_epi64(1));
                                }
                                valid&=q.membershipQueries.front().anti?__mmask8(~found):found;
                            }
                            auto pair=_mm512_or_si512(_mm512_slli_epi64(group,32),
                                _mm512_and_si512(_mm512_sub_epi64(value,_mm512_set1_epi64(low)),_mm512_set1_epi64(UINT32_MAX)));
                            _mm512_mask_compressstoreu_epi64(packed,valid,pair);
                            pairs.insert(pairs.end(),packed,packed+__builtin_popcount(valid));
                        }
                    }
                    for(;i<n;++i) {
                        uint64_t slot=uint64_t(keys[i])-uint64_t(sequence.minimum());if(slot>=rows)continue;
                        auto group=groupIds[slot];if(group==UINT32_MAX)continue;
                        if(!q.membershipQueries.empty() && membership.contains(Cell{Wide(values[i])})==q.membershipQueries.front().anti)continue;
                        pairs.push_back((uint64_t(group)<<32)|uint32_t(uint64_t(values[i])-uint64_t(low)));
                    }
                }
            }return pairs;
        });
        auto counts=CountPackedDistinct(inputs,groups.slots(),budget,parallel);
        std::vector<int> outputComponents;
        for(const auto &a:q.aggregateClauses)outputComponents.push_back(a.columnRef && a.type==AggregateClause::AGGREGATE_PROJECT?
            std::find(columns.begin(),columns.end(),*a.columnRef)-columns.begin():-1);
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
        QueryOutput output;output.spillBytes=spillBytes;
        size_t visible=q.outputColumnCount.value_or(q.aggregateClauses.size());
        for(size_t i=0;i<visible;++i)output.fieldNames.push_back(q.aggregateClauses[i].ToString());
        size_t begin=std::min<uint64_t>(q.offset,ordered.size()),end=ordered.size();
        if(q.limit)end=begin+std::min<uint64_t>(*q.limit,end-begin);
        budget.charge((end-begin)*(sizeof(Row)+visible*sizeof(std::string))+64);
        output.values.reserve(end-begin);
        for(size_t index=begin;index<end;++index) {
            uint32_t group=ordered[index];auto row=representatives[group];Row values;values.reserve(visible);
            for(size_t i=0;i<visible;++i) {
                if(i==size_t(countOutput)){budget.charge(128);values.push_back(Number(Wide(counts[group]),0));}
                else {
                    const auto &column=*q.aggregateClauses[i].columnRef;
                    auto cell=Read(dim.GetRowGroup(row>>32),column,uint32_t(row));
                    budget.charge(cell.string?cell.text.size()+32:128);
                    values.push_back(Format(FinalCell{std::move(cell),column.Type().get()}));
                }
            }
            output.values.push_back(std::move(values));
        }
        output.peakMemoryBytes=budget.peak.load();return output;
    } catch(const NotDense &){return {};}
    catch(const std::exception &e){return Status::Invalid(e.what());}
}
}
