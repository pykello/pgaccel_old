#include "executor_arithmetic_program.hpp"
#include "executor_result.hpp"
#include "executor_tasks.hpp"

namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteFilteredSum(const QueryDesc &q, bool avx, bool parallel) {
    if (q.tables.size()!=1 || q.grouped || !q.groupBy.empty() || !q.groupExpressions.empty() ||
        !q.predicates.empty() || !q.membershipQueries.empty() || !q.scalarQueries.empty() ||
        !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.materializedSources.empty() ||
        q.filterClauses.empty() || !q.having.empty() || q.aggregateClauses.size()!=1 ||
        q.aggregateClauses[0].type!=AggregateClause::AGGREGATE_SUM ||
        !q.aggregateClauses[0].expression) return {};
    try {
        Budget budget(q.memoryBudgetBytes); ScopedValueResource memory(budget);
        auto filter=CreateFilterNode(q.filterClauses,avx);
        auto &table=*q.tables[0];
        const auto &expression=*q.aggregateClauses[0].expression;
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial { Wide sum=0; uint64_t count=0; };
        auto partials=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);
            Scratch scratch(budget,32*Block*sizeof(int64_t)+Block*sizeof(uint32_t)+
                RowGroupSize/8+32*sizeof(Operation)+128);
            alignas(64) int64_t buffers[32*Block];
            uint32_t selected[Block]; uint8_t bitmap[RowGroupSize/8];
            Partial result;
            for (int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);
                int count=filter->ExecuteSet(rg,bitmap); if (!count) continue;
                Program program; int root=program.compile(expression,rg);
                const auto &op=program.ops[root];
                // Separate extrema bound every subset and every reduction lane.
                if (op.low*count<INT64_MIN || op.high*count>INT64_MAX) throw NotDense{};
                int64_t sum=0;
                auto consume=[&](int begin,int n,bool gather) {
                    if (gather) program.evaluate<true>(0,n,buffers,avx,selected);
                    else program.evaluate(begin,n,buffers,avx);
                    const auto values=buffers+root*Block; int row=0;
                    if (avx) for (;row+8<=n;row+=8)
                        sum+=_mm512_reduce_add_epi64(_mm512_load_si512(values+row));
                    for (;row<n;++row) sum+=values[row];
                };
                if (count==rg.size) {
                    for (int begin=0;begin<rg.size;begin+=Block)
                        consume(begin,std::min(Block,rg.size-begin),false);
                } else {
                    int n=0;
                    Selected(rg,bitmap,[&](int row) {
                        selected[n++]=row;
                        if (n==Block) { consume(0,n,true); n=0; }
                    });
                    if (n) consume(0,n,true);
                }
                result.sum=Add(result.sum,sum); result.count+=count;
            }
            return result;
        });
        Partial total;
        for (const auto &p:partials) { total.sum=Add(total.sum,p.sum); total.count+=p.count; }
        QueryResults results(q,budget);
        results.add(FinalRow{{Cell{total.sum},expression.type.get(),!total.count}});
        return results.finish();
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
