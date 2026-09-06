#pragma once
#include "executor_unique_key_graph.hpp"
namespace pgaccel {
namespace {
// Independent dimension chains with query-local uniqueness proofs. Logical
// aliases remain separate nodes even when they read the same physical table.
struct IndependentJoinPlan : UniqueKeyJoinGraph {
    std::vector<int> order,armOf;
    bool prepare(const QueryDesc &q,int fact,Budget &budget,bool avx,bool parallel,int paired=-1) {
        order.clear();armOf.assign(q.tables.size(),-1);
        if(!UniqueKeyJoinGraph::prepare(q,fact,budget,avx,parallel) ||
           outgoing[fact].empty() || outgoing[fact].size()>4)return false;
        std::vector<bool> visited(q.tables.size(),false);visited[fact]=true;
        // A paired-join consumer proves this extra relation separately. It
        // must be a non-key leaf referencing two distinct dimension chains.
        if(paired!=-1) {
            if(paired<0 || paired>=int(q.tables.size()) || paired==fact || keys[paired] ||
               outgoing[paired].size()!=2 || outgoing[paired][0].target==outgoing[paired][1].target)return false;
            visited[paired]=true;
        }
        for(int arm=0;arm<int(outgoing[fact].size());++arm) {
            std::vector<int> path;int t=outgoing[fact][arm].target;
            while(true) {
                if(visited[t] || !keys[t] || outgoing[t].size()>1)return false;
                visited[t]=true;path.push_back(t);armOf[t]=arm;
                if(outgoing[t].empty())break;
                t=outgoing[t].front().target;
            }
            order.insert(order.end(),path.rbegin(),path.rend());
        }
        if(paired!=-1 && armOf[outgoing[paired][0].target]==armOf[outgoing[paired][1].target])return false;
        return std::find(visited.begin(),visited.end(),false)==visited.end();
    }
};
}
}
