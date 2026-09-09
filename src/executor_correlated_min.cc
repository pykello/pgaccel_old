#include "executor_literal_filter.hpp"
#include "executor_sequence_join.hpp"
#include "executor_rank_join.hpp"
#include "executor_membership.hpp"
#include "executor_result.hpp"
#include "executor_arithmetic_program.hpp"
#include "executor_tasks.hpp"

namespace pgaccel {
namespace {
struct MinEdge { ColumnRef a,b; };
bool SameMinEdge(const MinEdge &a,const MinEdge &b) {
    return (a.a==b.a && a.b==b.b) || (a.a==b.b && a.b==b.a);
}
bool MinInteger(const ColumnRef &c) {
    return c.Type()->type_num()==INT32_TYPE || c.Type()->type_num()==INT64_TYPE;
}
struct MinPlan {
    int fact=-1,part=-1;
    ColumnRef cost,partKey,factPart;
    std::array<int,3> chain{};
    std::array<ColumnRef,3> primary{},foreign{};
    std::array<std::vector<FilterClause>,5> outerFilters,innerFilters;
};
std::optional<MinPlan> PlanCorrelatedMin(const QueryDesc &q) {
    if (q.tables.size()!=5 || q.scalarQueries.size()!=1 || !q.membershipQueries.empty() ||
        q.grouped || !q.groupBy.empty() || !q.groupExpressions.empty() || !q.having.empty() ||
        !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.materializedSources.empty() || q.orderBy.empty()) return {};
    const auto &scalar=q.scalarQueries[0];const auto &child=*scalar.query;
    if (child.memoryBudgetBytes<q.memoryBudgetBytes || child.tables.size()!=4 || !child.grouped ||
        child.groupExpressions.size()!=1 || child.aggregateClauses.size()!=2 ||
        !child.scalarQueries.empty() || !child.membershipQueries.empty() || !child.leftJoins.empty() ||
        !child.existenceJoins.empty() || !child.materializedSources.empty() || !child.having.empty() ||
        child.limit || child.offset || !child.orderBy.empty() ||
        scalar.outerKeys.size()!=1 || scalar.innerKeys.size()!=1 || scalar.keyOutputs!=std::vector<size_t>{1}) return {};
    const auto &minimum=child.aggregateClauses[0], &key=child.aggregateClauses[1];
    if (minimum.type!=AggregateClause::AGGREGATE_MIN || !minimum.columnRef ||
        key.type!=AggregateClause::AGGREGATE_PROJECT || !key.columnRef ||
        *key.columnRef!=scalar.innerKeys[0] || !child.groupExpressions[0]->column ||
        *child.groupExpressions[0]->column!=scalar.innerKeys[0]) return {};
    for (const auto &a:q.aggregateClauses)
        if (a.type!=AggregateClause::AGGREGATE_PROJECT || !a.columnRef || a.columnRef->tableIdx<0 || a.columnRef->tableIdx>=5) return {};
    for (size_t a=0;a<q.tables.size();++a) for (size_t b=0;b<a;++b)
        if (q.tables[a]==q.tables[b]) return {};
    std::array<int,4> mapping;
    for (int i=0;i<4;++i) {
        auto it=std::find(q.tables.begin(),q.tables.end(),child.tables[i]);
        if (it==q.tables.end()) return {};
        mapping[i]=it-q.tables.begin();
        for (int j=0;j<i;++j) if (mapping[i]==mapping[j]) return {};
    }
    auto remap=[&](ColumnRef c) { c.tableIdx=mapping.at(c.tableIdx);return c; };
    if (minimum.columnRef->tableIdx<0 || minimum.columnRef->tableIdx>=4 ||
        scalar.innerKeys[0].tableIdx<0 || scalar.innerKeys[0].tableIdx>=4) return {};
    MinPlan plan;plan.cost=remap(*minimum.columnRef);plan.fact=plan.cost.tableIdx;
    plan.factPart=remap(scalar.innerKeys[0]);plan.partKey=scalar.outerKeys[0];plan.part=plan.partKey.tableIdx;
    auto costType=plan.cost.Type()->type_num();
    if (plan.factPart.tableIdx!=plan.fact || plan.part<0 || plan.part>=5 ||
        std::find(mapping.begin(),mapping.end(),plan.part)!=mapping.end() ||
        !MinInteger(plan.partKey) || !MinInteger(plan.factPart) ||
        (costType!=INT32_TYPE && costType!=INT64_TYPE && costType!=DECIMAL_TYPE)) return {};
    std::vector<MinEdge> outerEdges,innerEdges;bool compared=false;
    for (int side=0;side<2;++side) {
        const auto &query=side?child:q;auto &filters=side?plan.innerFilters:plan.outerFilters;
        auto &edges=side?innerEdges:outerEdges;
        for (auto f:query.filterClauses) {
            if (f.columnRef.tableIdx<0 || size_t(f.columnRef.tableIdx)>=query.tables.size()) return {};
            if (side) f.columnRef=remap(f.columnRef);
            filters[f.columnRef.tableIdx].push_back(std::move(f));
        }
        for (const auto &e:query.predicates) {
            if (e->kind==Expression::EQ && e->left->column && e->right->column) {
                auto a=*e->left->column,b=*e->right->column;
                if (!side && (a.tableIdx==-2 || b.tableIdx==-2)) {
                    if (a.tableIdx==-2) std::swap(a,b);
                    if (compared || a!=plan.cost || b.tableIdx!=-2 || b.columnIdx!=0 ||
                        Scale(e->left->type.get())!=Scale(e->right->type.get())) return {};
                    compared=true;continue;
                }
                if (a.tableIdx<0 || b.tableIdx<0 || a.tableIdx==b.tableIdx ||
                    size_t(a.tableIdx)>=query.tables.size() || size_t(b.tableIdx)>=query.tables.size() ||
                    !MinInteger(a) || !MinInteger(b)) return {};
                edges.push_back({side?remap(a):a,side?remap(b):b});
            } else if (auto f=ExactLiteralFilter(*e)) {
                if (f->columnRef.tableIdx<0 || size_t(f->columnRef.tableIdx)>=query.tables.size()) return {};
                if (side) f->columnRef=remap(f->columnRef);
                filters[f->columnRef.tableIdx].push_back(std::move(*f));
            } else return {};
        }
    }
    if (!compared || innerEdges.size()!=3 || outerEdges.size()!=4) return {};
    for (const auto &edge:innerEdges)
        if (std::none_of(outerEdges.begin(),outerEdges.end(),[&](const auto &other){return SameMinEdge(edge,other);})) return {};
    MinEdge partEdge{plan.partKey,plan.factPart};
    if (std::none_of(outerEdges.begin(),outerEdges.end(),[&](const auto &other){return SameMinEdge(partEdge,other);})) return {};
    int current=plan.fact;uint32_t visited=uint32_t(1)<<current;
    for (int depth=0;depth<3;++depth) {
        std::optional<MinEdge> next;
        for (auto edge:innerEdges) {
            if (edge.a.tableIdx!=current) std::swap(edge.a,edge.b);
            if (edge.a.tableIdx!=current || (visited & (uint32_t(1)<<edge.b.tableIdx))) continue;
            if (next) return {};next=edge;
        }
        if (!next) return {};
        plan.foreign[depth]=next->a;plan.primary[depth]=next->b;
        plan.chain[depth]=current=next->b.tableIdx;visited|=uint32_t(1)<<current;
    }
    return plan;
}
struct MinState { int64_t value=0; bool seen=false; };
struct MinCandidate { uint64_t row; uint32_t slot; };
}
std::optional<Result<QueryOutput>> ExecuteCorrelatedMin(const QueryDesc &q,bool avx,bool parallel) {
    auto planned=PlanCorrelatedMin(q);if (!planned) return {};
    const auto &plan=*planned;
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,8192);
        std::array<std::optional<SequenceJoinIndex>,5> sequences;
        auto sequence=[&](int table,const ColumnRef &key) {
            uint64_t rows=0;for (int g=0;g<q.tables[table]->RowGroupCount();++g) rows+=q.tables[table]->GetRowGroup(g).size;
            sequences[table].emplace(&budget);
            if (!sequences[table]->prepare(*q.tables[table],key,rows,avx)) throw NotDense{};
        };
        sequence(plan.part,plan.partKey);
        for (int depth=0;depth<3;++depth) sequence(plan.chain[depth],plan.primary[depth]);
        std::array<FilterNodeP,5> outerFilters,innerFilters;
        for (int table=0;table<5;++table) {
            if (!plan.outerFilters[table].empty()) outerFilters[table]=CreateFilterNode(plan.outerFilters[table],avx);
            if (!plan.innerFilters[table].empty()) innerFilters[table]=CreateFilterNode(plan.innerFilters[table],avx);
        }
        auto collect=[&](int table,const FilterNodeP &filter,const MembershipSet *next,const ColumnRef *foreign) {
            int groups=q.tables[table]->RowGroupCount();
            int workers=parallel?std::min(8,std::max(1,groups)):1;
            return RunTasks(workers,[&](int worker) {
                ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+128);
                uint8_t bitmap[RowGroupSize/8];std::pmr::vector<uint64_t> rows(&budget);
                for (int g=worker;g<groups;g+=workers) {
                    const auto &rg=q.tables[table]->GetRowGroup(g);
                    if (filter) { if (!filter->ExecuteSet(rg,bitmap)) continue; }
                    else memset(bitmap,0xff,(rg.size+7)/8);
                    if (next) next->intersect(rg,*foreign,false,avx,bitmap);
                    Selected(rg,bitmap,[&](int row){rows.push_back((uint64_t(g)<<32)|uint32_t(row));});
                }
                return rows;
            });
        };
        auto read=[&](int table,uint64_t row,const ColumnRef &column) {
            return Read(q.tables[table]->GetRowGroup(row>>32),column,uint32_t(row));
        };
        MembershipSet parts(budget,0);std::optional<RankJoinIndex> index;
        {
            auto selected=collect(plan.part,outerFilters[plan.part],nullptr,nullptr);
            size_t count=0;for (const auto &rows:selected) count+=rows.size();
            if (!count) { QueryResults results(q,budget);return results.finish(); }
            if (count>1000000) throw NotDense{};
            std::pmr::vector<Wide> keys(&budget);keys.reserve(count);
            for (const auto &rows:selected) for (auto row:rows) keys.push_back(read(plan.part,row,plan.partKey).number);
            auto [low,high]=std::minmax_element(keys.begin(),keys.end());
            if (*high-*low+1>(Wide(1)<<29)) throw NotDense{};
            parts.assignNumbers(keys,0,budget);index.emplace(int64_t(*low),int64_t(*high),budget);
            for (Wide key:keys) if (!index->insert(int64_t(key))) throw NotDense{};
            index->finalize();
            size_t position=0;
            for (const auto &rows:selected) for (auto row:rows) index->setRow(int64_t(keys[position++]),row);
        }
        auto domains=[&](const auto &filters) {
            std::pmr::vector<MembershipSet> sets(&budget);sets.reserve(3);
            for (int depth=0;depth<3;++depth) sets.emplace_back(budget,0);
            for (int depth=2;depth>=0;--depth) {
                int table=plan.chain[depth];
                auto selected=collect(table,filters[table],depth==2?nullptr:&sets[depth+1],
                    depth==2?nullptr:&plan.foreign[depth+1]);
                size_t count=0;for (const auto &rows:selected) count+=rows.size();
                std::pmr::vector<Wide> keys(&budget);keys.reserve(count);
                for (const auto &rows:selected) for (auto row:rows) keys.push_back(read(table,row,plan.primary[depth]).number);
                sets[depth].assignNumbers(keys,0,budget);
            }
            return sets;
        };
        // The parent and scalar may filter the dimension chain identically.
        // Share that proven domain, while keeping fact predicates independent.
        bool sameDomains=true;
        for(int table:plan.chain) {
            const auto &a=plan.outerFilters[table],&b=plan.innerFilters[table];
            if(a.size()!=b.size()) {sameDomains=false;break;}
            for(size_t i=0;i<a.size();++i)
                if(a[i].columnRef!=b[i].columnRef || a[i].op!=b[i].op ||
                   a[i].value!=b[i].value || a[i].values!=b[i].values) sameDomains=false;
        }
        auto outerDomains=domains(outerFilters);
        auto separateInner=sameDomains?std::pmr::vector<MembershipSet>(&budget):domains(innerFilters);
        const auto &innerDomains=sameDomains?outerDomains:separateInner;
        auto &facts=*q.tables[plan.fact];int workers=parallel?std::min(8,std::max(1,facts.RowGroupCount())):1;
        struct Partial {
            std::pmr::vector<MinState> states;
            std::pmr::vector<MinCandidate> candidates;
            Partial(size_t size,Budget &budget):states(size,&budget),candidates(&budget) {}
        };
        auto partials=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,3*RowGroupSize/8);
            uint8_t bitmap[RowGroupSize/8],outerBits[RowGroupSize/8],innerBits[RowGroupSize/8];
            Partial result(index->size(),budget);
            for (int g=worker;g<facts.RowGroupCount();g+=workers) {
                const auto &rg=facts.GetRowGroup(g);
                for (const auto &column:{plan.factPart,plan.foreign[0],plan.cost})
                    if (rg.columns[column.columnIdx]->type!=ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
                memset(bitmap,0xff,(rg.size+7)/8);parts.intersect(rg,plan.factPart,false,avx,bitmap);
                if (outerFilters[plan.fact]) outerFilters[plan.fact]->ExecuteSet(rg,outerBits);
                if (innerFilters[plan.fact]) innerFilters[plan.fact]->ExecuteSet(rg,innerBits);
                Selected(rg,bitmap,[&](int row) {
                    auto slot=index->rank(int64_t(Read(rg,plan.factPart,row).number));
                    if (!slot) return;
                    auto supplier=Read(rg,plan.foreign[0],row);
                    if ((!innerFilters[plan.fact] || (innerBits[row/8] & (1<<(row%8)))) && innerDomains[0].contains(supplier)) {
                        auto value=int64_t(Read(rg,plan.cost,row).number);auto &state=result.states[*slot];
                        if (!state.seen || value<state.value) state.value=value;
                        state.seen=true;
                    }
                    if ((!outerFilters[plan.fact] || (outerBits[row/8] & (1<<(row%8)))) && outerDomains[0].contains(supplier))
                        result.candidates.push_back({(uint64_t(g)<<32)|uint32_t(row),*slot});
                });
            }
            return result;
        });
        std::pmr::vector<MinState> minimum(index->size(),&budget);
        RunTasks(workers,[&](int worker) {
            for (size_t slot=index->size()*worker/workers;slot<index->size()*(worker+1)/workers;++slot)
                for (const auto &p:partials) {
                    const auto &input=p.states[slot];auto &state=minimum[slot];
                    if (input.seen && (!state.seen || input.value<state.value)) state=input;
                }
            return 0;
        });
        QueryResults results(q,budget);std::array<uint64_t,5> bound{};
        for (const auto &p:partials) for (const auto &candidate:p.candidates) {
            const auto &state=minimum[candidate.slot];
            if (!state.seen || read(plan.fact,candidate.row,plan.cost).number!=state.value) continue;
            bound[plan.fact]=candidate.row;bound[plan.part]=index->row(candidate.slot);
            for (int depth=0;depth<3;++depth) {
                auto key=read(plan.foreign[depth].tableIdx,bound[plan.foreign[depth].tableIdx],plan.foreign[depth]);
                auto row=sequences[plan.chain[depth]]->lookup(int64_t(key.number));
                if (!row) throw NotDense{};
                bound[plan.chain[depth]]=*row;
            }
            results.addLazy([&](size_t output) {
                const auto &column=*q.aggregateClauses[output].columnRef;
                return FinalCell{read(column.tableIdx,bound[column.tableIdx],column),column.Type().get()};
            });
        }
        return results.finish();
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
