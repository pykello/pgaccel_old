#pragma once
#include "executor_unique_key_graph.hpp"
#include <array>

namespace pgaccel {
namespace {
// Two independent foreign-key chains terminate at small grouped dimensions.
// A consumer can evaluate a bounded predicate over terminal row identities.
struct SeparateJoinPlan : UniqueKeyJoinGraph {
    std::vector<int> centerOf,order;
    bool prepare(const QueryDesc &q,int fact,std::array<int,2> centers,
                 Budget &budget,bool avx,bool parallel) {
        const int n=q.tables.size();order.clear();centerOf.assign(n,-1);
        if(centers[0]==centers[1])return false;
        for(int center:centers)if(center<0 || center>=n || center==fact)return false;
        if(!UniqueKeyJoinGraph::prepare(q,fact,budget,avx,parallel) || outgoing[fact].size()!=2)return false;
        for(int t=0;t<n;++t)if(t!=fact && (!keys[t] || outgoing[t].size()>1))return false;
        for(int center:centers)if(!outgoing[center].empty() || !keys[center]->sequence.active() ||
                                  keys[center]->sequence.size()>256)return false;
        std::vector<bool> visited(n,false);visited[fact]=true;
        for(const auto &edge:outgoing[fact]) {
            std::vector<int> path;int t=edge.target;
            while(true) {
                if(visited[t])return false;
                visited[t]=true;path.push_back(t);
                if(outgoing[t].empty())break;
                t=outgoing[t].front().target;
            }
            if(t!=centers[0] && t!=centers[1])return false;
            for(auto i=path.rbegin();i!=path.rend();++i){centerOf[*i]=t;order.push_back(*i);}
        }
        return std::find(visited.begin(),visited.end(),false)==visited.end();
    }
};
}
}
