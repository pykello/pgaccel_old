// Diagnostic: preserve the preparation boundary even if execution is killed.
#include "direct_query.hpp"
#include "query_resources.hpp"
#include "json_rows.hpp"
#include <iostream>
#include <fstream>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteChainSum(const QueryDesc &,bool,bool); }
using namespace pgaccel;
int main(int argc,char **argv) {
    if(argc!=4)return 2;
    try {
        std::ifstream cg("/proc/self/cgroup");std::string line;std::getline(cg,line);
        auto path=std::filesystem::path("/sys/fs/cgroup")/line.substr(4);
        auto counter=[&](const char *name){std::ifstream in(path/name);uint64_t n;if(!(in>>n))throw std::runtime_error("Missing counter");return n;};
        std::ifstream file(argv[2]);std::string sql((std::istreambuf_iterator<char>(file)),{});
        auto loaded=PrepareDirectQuery(argv[1],sql);if(!loaded.ok())throw std::runtime_error(loaded.status().Message());
        auto p=std::move(loaded).ValueUnsafe();SetQueryResourceLimits(p.query,uint64_t(3)<<30,0);
        std::cout<<"{\"phase\":\"prepared\",\"current\":"<<counter("memory.current")<<",\"peak\":"<<counter("memory.peak")<<",\"max\":"<<counter("memory.max")<<",\"swap_max\":"<<counter("memory.swap.max")<<"}"<<std::endl;
        for(const auto &[name,t]:p.tables){uint64_t rows=0;for(int g=0;g<t->RowGroupCount();++g)rows+=t->GetRowGroup(g).size;
            std::cout<<"{\"table\":\""<<name<<"\",\"rows\":"<<rows<<"}"<<std::endl;}
        auto result=std::string(argv[3])=="chain"?ExecuteChainSum(p.query,true,true):std::optional<Result<QueryOutput>>(ExecuteQuery(p.query,true,true));
        if(!result)throw std::runtime_error("Chain route declined");
        if(!result->ok())throw std::runtime_error(result->status().Message());
        std::cout<<"{\"phase\":\"executed\",\"peak\":"<<counter("memory.peak")<<",\"query_peak\":"<<(**result).peakMemoryBytes<<",\"rows\":"<<benchmark::JsonRows((**result).values)<<"}"<<std::endl;
    }catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
