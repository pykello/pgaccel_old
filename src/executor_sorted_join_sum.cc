#include <atomic>
#include "executor_sorted_sum.hpp"
#include "executor_literal_filter.hpp"
#include "executor_sequence_join.hpp"
#include "executor_membership.hpp"
namespace pgaccel {
namespace {
bool SameSumTree(const std::shared_ptr<Expression> &a,const std::shared_ptr<Expression> &b,int depth=0) {
    if (!a || !b) return !a && !b;
    if (depth>64 || a->kind!=b->kind || a->literal!=b->literal || bool(a->column)!=bool(b->column) ||
        a->type->type_num()!=b->type->type_num() || Scale(a->type.get())!=Scale(b->type.get())) return false;
    if (a->column && *a->column!=*b->column) return false;
    return SameSumTree(a->left,b->left,depth+1) && SameSumTree(a->right,b->right,depth+1) &&
        SameSumTree(a->condition,b->condition,depth+1);
}
bool SumOnFact(const Expression &e,int fact,int depth=0) {
    if (depth>64 || (e.column && e.column->tableIdx!=fact)) return false;
    for (const auto &child:{e.left,e.right,e.condition}) if (child && !SumOnFact(*child,fact,depth+1)) return false;
    return true;
}
bool TotalFormula(const Expression &e,int sum,int depth=0) {
    if (depth>64) return false;
    if (e.kind==Expression::COLUMN) return e.column && e.column->tableIdx==-1 && e.column->columnIdx==sum;
    if (e.kind==Expression::CONSTANT) return e.type->type_num()!=STRING_TYPE;
    return (e.kind==Expression::ADD || e.kind==Expression::SUBTRACT || e.kind==Expression::MULTIPLY || e.kind==Expression::DIVIDE) &&
        e.left && e.right && TotalFormula(*e.left,sum,depth+1) && TotalFormula(*e.right,sum,depth+1);
}
// Completed nonnegative stripes give a conservative scalar lower bound.
// Snapshot once per stripe; stale observations merely retain more groups.
struct GrowingSumBound {
    std::atomic<int64_t> &completed;
    const Expression *formula;
    int childSum;
    FilterClause::Op comparison;
    int boundScale,rowScale;
    auto begin() const {
        Wide total=completed.load(std::memory_order_relaxed);
        Wide bound=formula?EvaluateWith(*formula,[&](const ColumnRef &column) {
            if(column.tableIdx!=-1 || column.columnIdx!=childSum) throw NotDense{};
            return Cell{total};
        }).number:total;
        ScaledBound cutoff(comparison,bound,boundScale,rowScale);
        return [cutoff](int64_t,int64_t sum) {return cutoff.matches(sum);};
    }
    void complete(int64_t sum) const {
        int64_t previous=completed.load(std::memory_order_relaxed);
        for (;;) {
            // Saturation remains a lower bound on the exact wide final sum.
            int64_t next=sum>INT64_MAX-previous?INT64_MAX:previous+sum;
            if(completed.compare_exchange_weak(previous,next,std::memory_order_relaxed)) break;
        }
    }
};

}
std::optional<Result<QueryOutput>> ExecuteSortedJoinSum(const QueryDesc &q,bool avx,bool parallel) {
    if (q.tables.size()!=3 || !q.grouped || q.groupExpressions.size()!=1 || !q.groupExpressions[0]->column ||
        q.scalarQueries.size()!=1 || q.having.size()!=1 || q.aggregateClauses.size()!=2 ||
        !q.membershipQueries.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.materializedSources.empty()) return {};
    const auto &scalar=q.scalarQueries[0];const auto &child=*scalar.query;const auto &having=q.having[0];
    if (!scalar.outerKeys.empty() || !scalar.innerKeys.empty() || !scalar.keyOutputs.empty() ||
        child.memoryBudgetBytes<q.memoryBudgetBytes || child.tables!=q.tables || child.grouped ||
        !child.groupBy.empty() || !child.groupExpressions.empty() || !child.having.empty() ||
        !child.scalarQueries.empty() || !child.membershipQueries.empty() || !child.leftJoins.empty() ||
        !child.existenceJoins.empty() || !child.materializedSources.empty() || !child.orderBy.empty() || child.limit || child.offset ||
        !having.bound || !having.bound->column || having.bound->column->tableIdx!=-2 || having.bound->column->columnIdx!=0 ||
        child.filterClauses.size()!=q.filterClauses.size() || child.predicates.size()!=q.predicates.size()) return {};
    for (size_t i=0;i<q.tables.size();++i) for (size_t j=0;j<i;++j) if (q.tables[i]==q.tables[j]) return {};
    for (size_t i=0;i<q.filterClauses.size();++i) {
        const auto &a=q.filterClauses[i],&b=child.filterClauses[i];
        if (a.columnRef!=b.columnRef || a.op!=b.op || a.value!=b.value || a.values!=b.values) return {};
    }
    for (size_t i=0;i<q.predicates.size();++i) if (!SameSumTree(q.predicates[i],child.predicates[i])) return {};
    int key=-1,sum=-1,childSum=-1;auto groupKey=*q.groupExpressions[0]->column;int fact=groupKey.tableIdx;
    if (fact<0 || fact>=3) return {};
    for (size_t i=0;i<2;++i) {
        const auto &a=q.aggregateClauses[i];
        if (a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef && *a.columnRef==groupKey) key=i;
        else if (a.type==AggregateClause::AGGREGATE_SUM && a.expression && SumOnFact(*a.expression,fact)) sum=i;
        else return {};
    }
    if (key<0 || sum<0 || having.output!=size_t(sum)) return {};
    if (child.aggregateClauses.size()==1 && child.aggregateClauses[0].type==AggregateClause::AGGREGATE_SUM) childSum=0;
    else if (child.aggregateClauses.size()==2 && child.aggregateClauses[0].type==AggregateClause::AGGREGATE_FORMULA &&
        child.aggregateClauses[0].expression && child.aggregateClauses[1].type==AggregateClause::AGGREGATE_SUM &&
        TotalFormula(*child.aggregateClauses[0].expression,1)) childSum=1;
    if (childSum<0 || !SameSumTree(q.aggregateClauses[sum].expression,child.aggregateClauses[childSum].expression)) return {};
    struct Edge {ColumnRef a,b;};std::vector<Edge> edges;std::array<std::vector<FilterClause>,3> clauses;
    for (const auto &f:q.filterClauses) {
        if (f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=3) return {};
        clauses[f.columnRef.tableIdx].push_back(f);
    }
    for (const auto &e:q.predicates) {
        if (e->kind==Expression::EQ && e->left->column && e->right->column) {
            auto a=*e->left->column,b=*e->right->column;
            if (a.tableIdx<0 || a.tableIdx>=3 || b.tableIdx<0 || b.tableIdx>=3 || a.tableIdx==b.tableIdx) return {};
            for (const auto &c:{a,b}) if (c.Type()->type_num()!=INT32_TYPE && c.Type()->type_num()!=INT64_TYPE) return {};
            edges.push_back({a,b});
        } else if (auto f=ExactLiteralFilter(*e)) {
            if (f->columnRef.tableIdx<0 || f->columnRef.tableIdx>=3) return {};
            clauses[f->columnRef.tableIdx].push_back(std::move(*f));
        } else return {};
    }
    if (edges.size()!=2) return {};
    std::array<ColumnRef,2> primary,foreign;std::array<int,2> dimensions;
    int current=fact;uint32_t visited=uint32_t(1)<<fact;
    for (int depth=0;depth<2;++depth) {
        std::optional<Edge> next;
        for (auto e:edges) {
            if (e.a.tableIdx!=current) std::swap(e.a,e.b);
            if (e.a.tableIdx!=current || (visited & (uint32_t(1)<<e.b.tableIdx))) continue;
            if (next) return {};next=e;
        }
        if (!next) return {};
        foreign[depth]=next->a;primary[depth]=next->b;dimensions[depth]=current=next->b.tableIdx;visited|=uint32_t(1)<<current;
    }
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,8192);
        std::array<FilterNodeP,3> filters;
        for (int table=0;table<3;++table) if (!clauses[table].empty()) filters[table]=CreateFilterNode(clauses[table],avx);
        for (int depth=0;depth<2;++depth) {
            auto &table=*q.tables[dimensions[depth]];uint64_t count=0;
            for (int g=0;g<table.RowGroupCount();++g) count+=table.GetRowGroup(g).size;
            SequenceJoinIndex proof(&budget);if (!proof.prepare(table,primary[depth],count,avx)) throw NotDense{};
        }
        auto domain=[&](int depth,const MembershipSet *next) {
            auto &table=*q.tables[dimensions[depth]];int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
            auto partials=RunTasks(workers,[&](int worker) {
                ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+128);
                std::pmr::vector<Wide> keys(&budget);uint8_t bitmap[RowGroupSize/8];
                for (int g=worker;g<table.RowGroupCount();g+=workers) {
                    const auto &rg=table.GetRowGroup(g);const auto &filter=filters[dimensions[depth]];
                    if (filter) { if (!filter->ExecuteSet(rg,bitmap)) continue; }
                    else memset(bitmap,0xff,(rg.size+7)/8);
                    if (next) next->intersect(rg,foreign[depth+1],false,avx,bitmap);
                    Selected(rg,bitmap,[&](int row){keys.push_back(Read(rg,primary[depth],row).number);});
                }
                return keys;
            });
            size_t count=0;for (const auto &keys:partials) count+=keys.size();
            std::pmr::vector<Wide> keys(&budget);keys.reserve(count);
            for (const auto &part:partials) keys.insert(keys.end(),part.begin(),part.end());
            MembershipSet result(budget,0);result.assignNumbers(keys,0,budget);return result;
        };
        auto tail=domain(1,nullptr);auto bridge=domain(0,&tail);
        if (bridge.bits.empty() && bridge.values.empty()) { QueryResults result(q,budget);return result.finish(); }
        auto filterFact=[&](const RowGroup &rg,uint8_t *bitmap) {
            if (filters[fact]) { if (!filters[fact]->ExecuteSet(rg,bitmap)) return false; }
            else memset(bitmap,0xff,(rg.size+7)/8);
            bridge.intersect(rg,foreign[0],false,avx,bitmap);return true;
        };
        bool growing=having.op==FilterClause::FILTER_GT || having.op==FilterClause::FILTER_GTE;
        const Expression *formula=childSum==0?nullptr:child.aggregateClauses[0].expression.get();
        if(growing && formula) {
            const Expression *literal=nullptr,*total=nullptr;
            if(formula->kind==Expression::MULTIPLY) {
                literal=formula->right.get();total=formula->left.get();
                if(literal->kind!=Expression::CONSTANT) std::swap(literal,total);
            }
            growing=literal && literal->kind==Expression::CONSTANT && total->kind==Expression::COLUMN &&
                total->column && total->column->tableIdx==-1 && total->column->columnIdx==childSum &&
                ParseDecimal(Scale(literal->type.get()),literal->literal)>=0;
        }
        // Prove every stripe nonnegative before publishing any subtotal.
        // Negative or unsupported formulas retain the original scanner.
        if(growing) for(int g=0;g<q.tables[fact]->RowGroupCount();++g) {
            const auto &rg=q.tables[fact]->GetRowGroup(g);if(!rg.size) continue;
            Program proof;int root=proof.compile(*q.aggregateClauses[sum].expression,rg);
            if(proof.ops[root].low<0) {growing=false;break;}
        }
        std::atomic<int64_t> completed{0};
        auto scan=growing
            ? ScanSortedSums(*q.tables[fact],groupKey,*q.aggregateClauses[sum].expression,
                filterFact,budget,avx,parallel,GrowingSumBound{completed,formula,childSum,having.op,
                    Scale(scalar.resultType.get()),Scale(q.aggregateClauses[sum].expression->type.get())})
            : ScanSortedSums(*q.tables[fact],groupKey,*q.aggregateClauses[sum].expression,
                filterFact,budget,avx,parallel);
        QueryDesc effective=q;auto &bound=effective.having[0];bound.bound.reset();bound.nullBound=!scan.count;
        if (scan.count) {
            bound.value=childSum==0?scan.sum:EvaluateWith(*child.aggregateClauses[0].expression,
                [&](const ColumnRef &column) {
                    if (column.tableIdx!=-1 || column.columnIdx!=childSum) throw NotDense{};
                    return Cell{scan.sum};
                }).number;
            bound.scale=Scale(scalar.resultType.get());
        }
        return FinishSortedSums(effective,key,sum,scan,budget,parallel);
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
