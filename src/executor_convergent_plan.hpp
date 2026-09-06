#pragma once
#include "executor_unique_key_graph.hpp"

namespace pgaccel {
namespace {
// Two disjoint arms first meet at the grouped dimension. A common chain
// beyond it can filter the dimension without weakening an equality.
struct ConvergentJoinPlan : UniqueKeyJoinGraph {
    std::vector<int> order;
    bool prepare(const QueryDesc &q,int fact,int center,Budget &budget,bool avx,bool parallel) {
        const int n=q.tables.size();order.clear();
        if(center<0 || center>=n || fact==center ||
           !UniqueKeyJoinGraph::prepare(q,fact,budget,avx,parallel))return false;
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
