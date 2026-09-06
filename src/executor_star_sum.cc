#include "executor_arithmetic_program.hpp"
#include "executor_sequence_join.hpp"
#include "executor_rank_join.hpp"
#include "executor_result.hpp"
#include "util.h"
#include <atomic>

namespace pgaccel {
namespace {
bool StarOnTable(const Expression &e,int table) {
    if(e.column && e.column->tableIdx!=table) return false;
    for(const auto &child:{e.left,e.right,e.condition})if(child && !StarOnTable(*child,table))return false;
    return true;
}
struct BitmapDomain {
    int64_t first,last;const uint64_t *bits;
    bool contains(int64_t key) const {
        if(key<first || key>last)return false;
        uint64_t offset=uint64_t(Wide(key)-first);return bits[offset/64]>>(offset%64)&1;
    }

};
void StarDecode(const RowGroup &rg,const ColumnRef &column,int n,int64_t *out,const uint32_t *selection,bool avx) {
    auto data=rg.columns[column.columnIdx].get();
    if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
    auto raw=static_cast<const RawColumnDataBase *>(data);
    switch(raw->bytesPerValue) {
    case 1:DecodeNumbers<int8_t,true>(raw,0,n,out,avx,selection);break;
    case 2:DecodeNumbers<int16_t,true>(raw,0,n,out,avx,selection);break;
    case 4:DecodeNumbers<int32_t,true>(raw,0,n,out,avx,selection);break;
    case 8:DecodeNumbers<int64_t,true>(raw,0,n,out,avx,selection);break;
    default:throw NotDense{};
    }
}
int StarSelect(const BitmapDomain &domain,const int64_t *keys,const uint32_t *input,int n,uint32_t *output,bool avx) {
    int row=0,count=0;
    const auto first=_mm512_set1_epi64(domain.first),last=_mm512_set1_epi64(domain.last);
    const auto words=domain.bits;
    if(avx)for(;row+8<=n;row+=8) {
        auto key=_mm512_load_si512(keys+row);
        auto delta=_mm512_sub_epi64(key,first);
        auto valid=_mm512_cmpge_epi64_mask(key,first) & _mm512_cmple_epi64_mask(key,last);
        auto values=_mm512_mask_i64gather_epi64(_mm512_setzero_si512(),valid,_mm512_srli_epi64(delta,6),words,8);
        auto mask=valid & _mm512_test_epi64_mask(_mm512_srlv_epi64(values,
            _mm512_and_si512(delta,_mm512_set1_epi64(63))),_mm512_set1_epi64(1));
        _mm512_mask_compressstoreu_epi32(output+count,mask,
            _mm512_zextsi256_si512(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(input+row))));
        count+=__builtin_popcount(mask);
    }
    for(;row<n;++row)if(domain.contains(keys[row]))output[count++]=input[row];
    return count;
}
}
std::optional<Result<QueryOutput>> ExecuteStarSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=3 || q.predicates.size()!=2 || !q.grouped || q.groupExpressions.empty() ||
        !q.existenceJoins.empty() || !q.leftJoins.empty() || !q.membershipQueries.empty() ||
        !q.scalarQueries.empty() || q.aggregateClauses.size()>8) return {};
    struct Edge {ColumnRef a,b;};std::vector<Edge> edges;int degree[3]{};
    for(const auto &p:q.predicates) {
        if(p->kind!=Expression::EQ || !p->left->column || !p->right->column)return {};
        auto a=*p->left->column,b=*p->right->column;
        if(a.tableIdx<0 || b.tableIdx<0 || a.tableIdx==b.tableIdx ||
            Scale(a.Type().get())!=Scale(b.Type().get()))return {};
        edges.push_back({a,b});++degree[a.tableIdx];++degree[b.tableIdx];
    }
    int bridge=-1;for(int i=0;i<3;++i)if(degree[i]==2)bridge=i;
    if(bridge<0)return {};
    int fact=-1,dimension=-1;
    ColumnRef factKey,bridgeKey,bridgeDimension,dimensionKey;
    for(int candidate=0;candidate<3;++candidate)if(degree[candidate]==1 && fact<0) {
        ColumnRef fk,bk,bd,dk;
        for(auto edge:edges) {
            if(edge.a.tableIdx!=bridge)std::swap(edge.a,edge.b);
            if(edge.b.tableIdx==candidate){fk=edge.b;bk=edge.a;}else{bd=edge.a;dk=edge.b;}
        }
        bool identity=false,valid=true;
        for(const auto &e:q.groupExpressions) {
            if(e->kind!=Expression::COLUMN){valid=false;break;}
            identity |= *e->column==fk || *e->column==bk;
            valid &= e->column->tableIdx==bridge || *e->column==fk;
        }
        for(const auto &a:q.aggregateClauses) {
            if(a.type==AggregateClause::AGGREGATE_SUM)valid &= a.expression && StarOnTable(*a.expression,candidate);
            else if(a.type==AggregateClause::AGGREGATE_COUNT)valid &= !a.expression;
            else if(a.type==AggregateClause::AGGREGATE_PROJECT)
                valid &= a.expression && (a.expression->kind==Expression::CONSTANT ||
                    (a.expression->kind==Expression::COLUMN && (a.expression->column->tableIdx==bridge || *a.expression->column==fk)));
            else valid=false;
        }
        if(valid && identity){fact=candidate;dimension=dk.tableIdx;factKey=fk;bridgeKey=bk;bridgeDimension=bd;dimensionKey=dk;}
    }
    if(fact<0)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        uint64_t sizes[3]{};
        for(int table=0;table<3;++table)for(int g=0;g<q.tables[table]->RowGroupCount();++g)sizes[table]+=q.tables[table]->GetRowGroup(g).size;
        if(sizes[bridge]>UINT32_MAX)return {};
        std::vector<FilterClause> clauses[3];
        for(const auto &f:q.filterClauses)clauses[f.columnRef.tableIdx].push_back(f);
        FilterNodeP filters[3];for(int table=0;table<3;++table)if(!clauses[table].empty())filters[table]=CreateFilterNode(clauses[table],avx);
        SequenceJoinIndex sequence(&budget);
        if(!sequence.prepare(*q.tables[dimension],dimensionKey,sizes[dimension],avx))return {};
        std::pmr::vector<uint64_t> dimensionBits((sequence.size()+63)/64,0,&budget);
        sequence.forEachStripe([&](uint32_t group,uint64_t offset) {
            const auto &rg=q.tables[dimension]->GetRowGroup(group);uint8_t bitmap[RowGroupSize/8];
            int count=filters[dimension]?filters[dimension]->ExecuteSet(rg,bitmap):rg.size;
            if(!count)return;
            if(count==rg.size)std::memset(bitmap,0xff,(rg.size+7)/8);
            if(offset%8==0 && (rg.size%8==0 || offset+rg.size==sequence.size()))std::memcpy(reinterpret_cast<uint8_t *>(dimensionBits.data())+offset/8,bitmap,(rg.size+7)/8);
            else Selected(rg,count==rg.size?nullptr:bitmap,[&](int row){dimensionBits[(offset+row)/64]|=uint64_t(1)<<((offset+row)%64);});
        });
        BitmapDomain dimensionDomain{sequence.minimum(),sequence.maximum(),dimensionBits.data()};
        struct Pair {int64_t key;uint64_t row;};
        auto &bridgeTable=*q.tables[bridge];
        int workers=parallel?std::min(8,std::max(1,bridgeTable.RowGroupCount())):1;
        auto build=[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,16384);
            std::pmr::vector<Pair> pairs(&budget);uint8_t bitmap[RowGroupSize/8];
            alignas(64)int64_t keys[Block];uint32_t selected[Block],matched[Block];
            for(int g=worker;g<bridgeTable.RowGroupCount();g+=workers) {
                const auto &rg=bridgeTable.GetRowGroup(g);
                int count=filters[bridge]?filters[bridge]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;int n=0;
                auto consume=[&] {
                    StarDecode(rg,bridgeDimension,n,keys,selected,avx);
                    int accepted=StarSelect(dimensionDomain,keys,selected,n,matched,avx);
                    StarDecode(rg,bridgeKey,accepted,keys,matched,avx);
                    for(int i=0;i<accepted;++i)pairs.push_back({keys[i],(uint64_t(g)<<32)|matched[i]});
                    n=0;
                };
                Selected(rg,count==rg.size?nullptr:bitmap,[&](int row){selected[n++]=row;if(n==Block)consume();});if(n)consume();
            }
            return pairs;
        };
        auto parts=RunTasks(workers,build);
        int64_t low=INT64_MAX,high=INT64_MIN;uint64_t selectedCount=0;
        for(const auto &part:parts)for(const auto &p:part){low=std::min(low,p.key);high=std::max(high,p.key);++selectedCount;}
        QueryResults results(q,budget);if(!selectedCount)return results.finish();
        Wide width=Wide(high)-low+1;
        if(width>Wide(sizes[bridge])*8 || width>(Wide(1)<<34))throw NotDense{};
        RankJoinIndex index(low,high,budget);
        auto fillBits=[&](int worker) {
            for(const auto &p:parts[worker])if(!index.insert<true>(p.key))return false;
            return true;
        };
        for(bool unique:RunTasks(parts.size(),fillBits))if(!unique)throw NotDense{};
        index.finalize();
        RunTasks(parts.size(),[&](int worker){for(const auto &p:parts[worker])index.setRow(p.key,p.row);return true;});
        parts.clear();parts.shrink_to_fit();
        const size_t outputs=q.aggregateClauses.size();
        std::array<int,8> stateIndex;stateIndex.fill(-1);size_t stateCount=0;
        for(size_t i=0;i<outputs;++i)if(q.aggregateClauses[i].type!=AggregateClause::AGGREGATE_PROJECT)stateIndex[i]=stateCount++;
        std::pmr::vector<int64_t> sums(index.size()*stateCount,0,&budget);
        std::pmr::vector<uint64_t> visited((index.size()+63)/64,0,&budget);
        BitmapDomain domain{index.minimum(),index.maximum(),index.words()};
        auto &factTable=*q.tables[fact];
        workers=parallel?std::min(8,std::max(1,factTable.RowGroupCount())):1;
        auto scan=[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+65536);
            alignas(64)int64_t buffers[32*Block],keys[Block];uint32_t selected[Block],matched[Block];
            uint8_t bitmap[RowGroupSize/8];
            for(int g=worker;g<factTable.RowGroupCount();g+=workers) {
                const auto &rg=factTable.GetRowGroup(g);
                int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;
                Program program;std::array<int,8> roots;roots.fill(-1);
                for(size_t i=0;i<outputs;++i)if(q.aggregateClauses[i].type==AggregateClause::AGGREGATE_SUM) {
                    roots[i]=program.compile(*q.aggregateClauses[i].expression,rg);
                    const auto &p=program.ops[roots[i]];
                    // This bounds every partial group, including concurrent
                    // updates, so signed 64-bit atomic addition remains exact.
                    if(p.low*sizes[fact]<INT64_MIN || p.high*sizes[fact]>INT64_MAX)throw NotDense{};
                }
                int n=0;
                auto consume=[&] {
                    StarDecode(rg,factKey,n,keys,selected,avx);
                    int accepted=StarSelect(domain,keys,selected,n,matched,avx);
                    if(accepted) {
                        StarDecode(rg,factKey,accepted,keys,matched,avx);
                        program.evaluate<true>(0,accepted,buffers,avx,matched);
                        for(int row=0;row<accepted;++row) {
                            auto rank=index.rank(keys[row]);if(!rank)throw std::logic_error("Bitmap rank mismatch");
                            std::atomic_ref<uint64_t>(visited[*rank/64]).fetch_or(uint64_t(1)<<(*rank%64),std::memory_order_relaxed);
                            for(size_t i=0;i<outputs;++i) {
                                if(roots[i]>=0)std::atomic_ref<int64_t>(sums[*rank*stateCount+stateIndex[i]]).fetch_add(buffers[roots[i]*Block+row],std::memory_order_relaxed);
                                else if(q.aggregateClauses[i].type==AggregateClause::AGGREGATE_COUNT)
                                    std::atomic_ref<int64_t>(sums[*rank*stateCount+stateIndex[i]]).fetch_add(1,std::memory_order_relaxed);
                            }
                        }
                    }
                    n=0;
                };
                Selected(rg,count==rg.size?nullptr:bitmap,[&](int row){selected[n++]=row;if(n==Block)consume();});if(n)consume();
            }
            return true;
        };
        RunTasks(workers,scan);
        for(size_t word=0;word<visited.size();++word)for(uint64_t bits=visited[word];bits;bits&=bits-1) {
            uint32_t rank=word*64+__builtin_ctzll(bits);
            results.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i];FinalCell cell;
                cell.type=a.expression?a.expression->type.get():nullptr;
                if(a.type==AggregateClause::AGGREGATE_COUNT){cell.type=nullptr;cell.value.number=sums[rank*stateCount+stateIndex[i]];}
                else if(a.type==AggregateClause::AGGREGATE_SUM)cell.value.number=sums[rank*stateCount+stateIndex[i]];
                else if(a.expression->kind==Expression::CONSTANT)cell.value=Evaluate(*a.expression,RowGroup{},0);
                else {
                    uint64_t row=index.row(rank);auto column=*a.expression->column;
                    if(column==factKey)column=bridgeKey;
                    cell.value=Read(bridgeTable.GetRowGroup(row>>32),column,uint32_t(row));
                }
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
