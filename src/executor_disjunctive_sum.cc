#include "executor_boolean_bitmap.hpp"
#include "executor_bitmap_probe.hpp"
#include "executor_sequence_join.hpp"
#include "executor_buffer.hpp"
#include "executor_result.hpp"
#include "util.h"

namespace pgaccel {
namespace {
uint64_t DisjunctReferences(const Expression &e) {
    uint64_t mask=0;
    if(e.column) {if(e.column->tableIdx<0 || e.column->tableIdx>1)throw NotDense{};mask|=uint64_t(1)<<e.column->tableIdx;}
    for(const auto &p:{e.left,e.right,e.condition})if(p)mask|=DisjunctReferences(*p);
    return mask;
}
struct Disjunct {std::vector<std::shared_ptr<Expression>> local[2];};
std::vector<Disjunct> DisjunctProduct(const std::vector<Disjunct> &a,const std::vector<Disjunct> &b) {
    if(a.size()*b.size()>8)throw NotDense{};std::vector<Disjunct> result;
    for(const auto &x:a)for(const auto &y:b) {
        auto z=x;for(int t=0;t<2;++t)z.local[t].insert(z.local[t].end(),y.local[t].begin(),y.local[t].end());result.push_back(std::move(z));
    }
    return result;
}
std::vector<Disjunct> SplitDisjunct(const std::shared_ptr<Expression> &e,const ColumnRef &a,const ColumnRef &b,int probe,int depth=0) {
    if(depth>64)throw NotDense{};auto mask=DisjunctReferences(*e);
    if(mask!=3) {Disjunct result;result.local[mask==1?0:mask==2?1:probe].push_back(e);return {std::move(result)};}
    if(e->kind==Expression::EQ && e->left->column && e->right->column &&
        ((*e->left->column==a && *e->right->column==b) || (*e->left->column==b && *e->right->column==a)))return {Disjunct{}};
    if(e->kind!=Expression::AND && e->kind!=Expression::OR)throw NotDense{};
    auto left=SplitDisjunct(e->left,a,b,probe,depth+1),right=SplitDisjunct(e->right,a,b,probe,depth+1);
    if(e->kind==Expression::AND)return DisjunctProduct(left,right);
    if(left.size()+right.size()>8)throw NotDense{};left.insert(left.end(),std::make_move_iterator(right.begin()),std::make_move_iterator(right.end()));return left;
}
}
std::optional<Result<QueryOutput>> ExecuteDisjunctiveSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || q.grouped || !q.groupExpressions.empty() || !q.existenceJoins.empty() ||
        !q.leftJoins.empty() || !q.membershipQueries.empty() || !q.scalarQueries.empty() || q.aggregateClauses.size()>32)return {};
    std::optional<ColumnRef> a,b;
    for(const auto &p:q.predicates)if(p->kind==Expression::EQ && p->left->column && p->right->column && p->left->column->tableIdx!=p->right->column->tableIdx) {
        if(a)return {};a=*p->left->column;b=*p->right->column;
    }
    if(!a || a->tableIdx<0 || b->tableIdx<0 || a->tableIdx>1 || b->tableIdx>1 || Scale(a->Type().get())!=Scale(b->Type().get()))return {};
    try {
        int probe=-1;
        for(int t=0;t<2 && probe<0;++t) {
            bool valid=true;
            for(const auto &aggregate:q.aggregateClauses) {
                if(aggregate.type==AggregateClause::AGGREGATE_COUNT)valid &= !aggregate.expression;
                else valid &= aggregate.type==AggregateClause::AGGREGATE_SUM && aggregate.expression && !(DisjunctReferences(*aggregate.expression)&~(uint64_t(1)<<t));
            }
            if(valid)probe=t;
        }
        if(probe<0)return {};int build=1-probe;auto probeKey=a->tableIdx==probe?*a:*b,buildKey=a->tableIdx==build?*a:*b;
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);SequenceJoinIndex index(&budget);
        uint64_t buildRows=0;for(int g=0;g<q.tables[build]->RowGroupCount();++g)buildRows+=q.tables[build]->GetRowGroup(g).size;
        if(!index.prepare(*q.tables[build],buildKey,buildRows,avx))return {};
        std::vector<Disjunct> branches(1);
        for(const auto &p:q.predicates)branches=DisjunctProduct(branches,SplitDisjunct(p,*a,*b,probe));
        BooleanBitmapProgram buildProgram(budget,avx),probeProgram(budget,avx);
        std::vector<int> buildRoots,probeRoots;
        for(const auto &branch:branches) {
            int roots[2]={-1,-1};
            for(int t=0;t<2;++t) {
                auto &program=t==build?buildProgram:probeProgram;
                for(const auto &p:branch.local[t]) {int next=program.compile(*p,t);roots[t]=roots[t]<0?next:program.combine(true,roots[t],next);}
                for(const auto &f:q.filterClauses)if(f.columnRef.tableIdx==t) {int next=program.leaf(f);roots[t]=roots[t]<0?next:program.combine(true,roots[t],next);}
                if(roots[t]<0)roots[t]=program.truth();
            }
            buildRoots.push_back(roots[build]);probeRoots.push_back(roots[probe]);
        }
        std::pmr::vector<std::pmr::vector<uint64_t>> bitmaps(&budget);
        std::array<uint64_t,8> branchCounts{};
        for(size_t i=0;i<branches.size();++i){bitmaps.emplace_back();bitmaps.back().resize((index.size()+63)/64,0);}
        OverwriteBuffer<uint64_t> buildScratch((buildProgram.bytes()+7)/8,budget);std::array<int,64> counts{};
        index.forEachStripe([&](uint32_t g,uint64_t offset) {
            const auto &rg=q.tables[build]->GetRowGroup(g);auto scratch=reinterpret_cast<uint8_t *>(buildScratch.data());
            buildProgram.evaluate(rg,scratch,counts);
            for(size_t i=0;i<branches.size();++i) {
                branchCounts[i]+=counts[buildRoots[i]];if(!counts[buildRoots[i]])continue;
                const auto bitmap=buildProgram.bitmap(scratch,buildRoots[i]);
                if(offset%8==0 && (rg.size%8==0 || offset+rg.size==index.size()))std::memcpy(reinterpret_cast<uint8_t *>(bitmaps[i].data())+offset/8,bitmap,(rg.size+7)/8);
                else for(int row=0;row<rg.size;++row)if(bitmap[row/8]>>(row%8)&1)bitmaps[i][(offset+row)/64]|=uint64_t(1)<<((offset+row)%64);
            }
        });
        struct Partial {uint64_t count=0;std::array<Wide,32> sums{};};
        auto &table=*q.tables[probe];int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch stack(budget,32*Block*8+65536);
            OverwriteBuffer<uint64_t> scratchWords((probeProgram.bytes()+7)/8,budget);auto scratch=reinterpret_cast<uint8_t *>(scratchWords.data());
            std::array<int,64> counts{};uint8_t joined[RowGroupSize/8];uint32_t selected[Block];alignas(64)int64_t buffers[32*Block];Partial result;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
                probeProgram.evaluate(rg,scratch,counts);std::memset(joined,0,(rg.size+7)/8);int joinedCount=0;
                auto data=rg.columns[probeKey.columnIdx].get();if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
                auto raw=static_cast<const RawColumnDataBase *>(data);
                auto accept=[&](uint32_t row){uint8_t bit=1u<<(row%8);joinedCount+=!(joined[row/8]&bit);joined[row/8]|=bit;};
                for(size_t i=0;i<branches.size();++i) {
                    if(!branchCounts[i] || !counts[probeRoots[i]])continue;
                    auto bitmap=probeProgram.bitmap(scratch,probeRoots[i]);BitmapDomain domain{index.minimum(),index.maximum(),bitmaps[i].data()};
                    if(avx && raw->bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX) {
                        for(int begin=0;begin<rg.size;begin+=Block) {
                            int n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(raw->values),begin,std::min(Block,rg.size-begin),bitmap,selected);
                            for(int row=0;row<n;++row)accept(selected[row]);
                        }
                    } else for(int row=0;row<rg.size;++row)if(bitmap[row/8]>>(row%8)&1) {
                        auto key=Read(rg,probeKey,row).number;if(key>=domain.first && key<=domain.last && domain.contains(int64_t(key)))accept(row);
                    }
                }
                if(!joinedCount)continue;result.count+=joinedCount;Program arithmetic;std::array<int,32> roots;roots.fill(-1);
                for(size_t i=0;i<q.aggregateClauses.size();++i)if(q.aggregateClauses[i].type==AggregateClause::AGGREGATE_SUM) {
                    roots[i]=arithmetic.compile(*q.aggregateClauses[i].expression,rg);const auto &p=arithmetic.ops[roots[i]];
                    if(p.low*joinedCount<INT64_MIN || p.high*joinedCount>INT64_MAX)throw NotDense{};
                }
                std::array<int64_t,32> sums{};int n=0;
                auto consume=[&] {
                    arithmetic.evaluate<true>(0,n,buffers,avx,selected);
                    for(size_t i=0;i<roots.size();++i)if(roots[i]>=0)for(int row=0;row<n;++row)sums[i]+=buffers[roots[i]*Block+row];n=0;
                };
                Selected(rg,joined,[&](int row){selected[n++]=row;if(n==Block)consume();});if(n)consume();
                for(size_t i=0;i<sums.size();++i)result.sums[i]=Add(result.sums[i],sums[i]);
            }
            return result;
        });
        Partial total;for(const auto &part:parts){total.count+=part.count;for(size_t i=0;i<total.sums.size();++i)total.sums[i]=Add(total.sums[i],part.sums[i]);}
        QueryResults results(q,budget);results.addLazy([&](size_t i) {
            const auto &a=q.aggregateClauses[i];FinalCell cell;cell.type=a.expression?a.expression->type.get():nullptr;
            if(a.type==AggregateClause::AGGREGATE_COUNT)cell.value.number=total.count;
            else {cell.null=!total.count;cell.value.number=total.sums[i];}return cell;
        });return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
