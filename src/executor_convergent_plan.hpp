#pragma once
#include "executor_sequence_join.hpp"
#include "executor_sorted_key.hpp"
#include "executor_arithmetic_program.hpp"

namespace pgaccel {
namespace {
// Two foreign-key chains meet at the grouped dimension. A further common
// chain may filter that dimension. Every target key is proved unique, and
// equality classes retain transitive predicates such as a.x=b.x=b.key.
struct ConvergentJoinPlan {
    struct Key {
        ColumnRef column;
        SequenceJoinIndex sequence;
        SortedKeyIndex sorted;
        Key(ColumnRef column, Budget &budget)
            : column(column), sequence(&budget), sorted(budget) {}
        bool prepare(const QueryDesc &q, bool avx, bool parallel) {
            const auto &table = *q.tables[column.tableIdx];
            uint64_t rows = 0;
            for (int g=0; g<table.RowGroupCount(); ++g) rows += table.GetRowGroup(g).size;
            return sequence.prepare(table,column,rows,avx) || sorted.prepare(table,column,avx,parallel);
        }
    };
    struct Edge { ColumnRef source; int target; };
    std::vector<std::unique_ptr<Key>> keys;
    std::vector<std::vector<Edge>> outgoing;
    std::vector<int> order;

    bool prepare(const QueryDesc &q, int fact, int center, Budget &budget, bool avx, bool parallel) {
        const int n=q.tables.size();
        if(n<3 || n>8 || fact==center || q.predicates.size()>16) return false;
        keys.resize(n); outgoing.resize(n); order.clear();
        std::vector<ColumnRef> columns;
        std::vector<int> parent;
        auto id=[&](const ColumnRef &column) {
            for(size_t i=0;i<columns.size();++i) if(columns[i]==column) return int(i);
            int i=columns.size(); columns.push_back(column); parent.push_back(i); return i;
        };
        auto root=[&](int i) { while(parent[i]!=i) i=parent[i]; return i; };
        for(const auto &p:q.predicates) {
            if(p->kind!=Expression::EQ || !p->left || !p->right ||
               !p->left->column || !p->right->column) return false;
            const auto &a=*p->left->column, &b=*p->right->column;
            auto integer=[](const ColumnRef &c) {
                auto type=c.Type()->type_num(); return type==INT32_TYPE || type==INT64_TYPE;
            };
            if(a.tableIdx<0 || a.tableIdx>=n || b.tableIdx<0 || b.tableIdx>=n ||
               a.tableIdx==b.tableIdx || !integer(a) || !integer(b)) return false;
            int ai=id(a),bi=id(b); parent[root(ai)]=root(bi);
        }
        std::vector<std::vector<int>> classes(columns.size());
        for(size_t i=0;i<columns.size();++i) classes[root(i)].push_back(i);
        for(auto &members:classes) if(!members.empty()) {
            // Prefer small dimensions when more than one column is unique.
            auto rows=[&](int i) {
                uint64_t count=0;const auto &t=*q.tables[columns[i].tableIdx];
                for(int g=0;g<t.RowGroupCount();++g) count+=t.GetRowGroup(g).size;
                return count;
            };
            std::stable_sort(members.begin(),members.end(),[&](int a,int b){return rows(a)<rows(b);});
            int chosen=-1;
            std::unique_ptr<Key> key;
            for(int i:members) if(columns[i].tableIdx!=fact) {
                auto candidate=std::make_unique<Key>(columns[i],budget);
                if(candidate->prepare(q,avx,parallel)){chosen=i;key=std::move(candidate);break;}
            }
            if(chosen<0) return false;
            int target=columns[chosen].tableIdx;
            if(keys[target]) return false;
            keys[target]=std::move(key);
            for(int i:members) if(i!=chosen) {
                int source=columns[i].tableIdx;
                if(source==target) return false;
                outgoing[source].push_back({columns[i],target});
            }
        }
        if(outgoing[fact].size()!=2) return false;
        for(int t=0;t<n;++t) if(t!=fact && (!keys[t] || outgoing[t].size()>1)) return false;
        if(!keys[center]->sequence.active() || keys[center]->sequence.size()>65536) return false;
        std::vector<bool> visited(n,false);visited[fact]=true;
        // Arms must be disjoint before the center. Comparing center identities
        // alone would lose an equality if they met earlier at another table.
        for(const auto &edge:outgoing[fact]) {
            int t=edge.target;
            while(t!=center) {
                if(visited[t] || outgoing[t].size()!=1) return false;
                visited[t]=true;t=outgoing[t].front().target;
            }
        }
        int t=center;
        while(true) {
            if(visited[t]) return false;
            visited[t]=true;
            if(outgoing[t].empty()) break;
            t=outgoing[t].front().target;
        }
        if(std::find(visited.begin(),visited.end(),false)!=visited.end()) return false;
        std::fill(visited.begin(),visited.end(),false);
        auto visit=[&](auto &&self,int table)->void {
            if(visited[table]) return;
            visited[table]=true;
            for(const auto &edge:outgoing[table]) self(self,edge.target);
            if(table!=fact) order.push_back(table);
        };
        visit(visit,fact);return true;
    }
};
}
}
