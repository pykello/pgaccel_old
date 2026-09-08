#include "direct_table.hpp"
#include "executor.h"
#include "query_resources.hpp"
#include "json_rows.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
using namespace pgaccel;
int main(int argc,char **argv) {
    if(argc<5){std::cerr<<"usage: probe native table sql-file column...\n";return 2;}
    try {
        std::ifstream sqlFile(argv[3]);std::string sql((std::istreambuf_iterator<char>(sqlFile)),{});
        std::set<std::string> fields;for(int i=4;i<argc;++i)fields.insert(argv[i]);
        DirectScanStats io;auto begin=std::chrono::steady_clock::now();TableRegistry registry;
        std::map<std::string,std::set<std::string>> projected;
        bool multiple=std::string(argv[2])=="auto";
        for(const auto &field:fields) {
            if(!multiple){projected[argv[2]].insert(field);continue;}
            auto dot=field.find('.');if(dot==std::string::npos)throw std::runtime_error("Need table.column projection");
            projected[field.substr(0,dot)].insert(field.substr(dot+1));
        }
        for(const auto &[name,columns]:projected) {
            auto path=multiple?(std::filesystem::path(argv[1])/(name+".pgaccel")).string():std::string(argv[1]);
            DirectScanStats part;auto loaded=LoadDirectTable(name,path,columns,&part);
            if(!loaded.ok())throw std::runtime_error(loaded.status().Message());
            io.bytesRead+=part.bytesRead;io.readCalls+=part.readCalls;
            registry.emplace(name,std::move(loaded).ValueUnsafe());
        }
        double loadMs=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-begin).count();
        auto parsed=ParseSelect(sql,registry);if(!parsed.ok())throw std::runtime_error(parsed.status().Message());
        auto q=std::move(parsed).ValueUnsafe();SetQueryResourceLimits(q,uint64_t(512)<<20,0);q.allowSpill=false;
        std::ifstream cg("/proc/self/cgroup");std::string cgLine;std::getline(cg,cgLine);
        if(!cgLine.starts_with("0::/"))throw std::runtime_error("Need unified cgroup");
        auto cgPath=std::filesystem::path("/sys/fs/cgroup")/cgLine.substr(4);
        auto counter=[&](const char *name){std::ifstream in(cgPath/name);uint64_t v;
            if(!(in>>v))throw std::runtime_error("Missing cgroup limit/accounting");return v;};
        std::cout<<"{\"load_ms\":"<<loadMs<<",\"read_bytes\":"<<io.bytesRead
            <<",\"memory_max\":"<<counter("memory.max")<<",\"swap_max\":"<<counter("memory.swap.max")<<",\"trials\":[";
        for(int trial=0;trial<9;++trial) {
            auto start=std::chrono::steady_clock::now();auto result=ExecuteQuery(q,true,true);
            double ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-start).count();
            if(!result.ok())throw std::runtime_error(result.status().Message());
            if(trial)std::cout<<',';
            std::cout<<"{\"execution_ms\":"<<ms<<",\"query_peak_bytes\":"<<result->peakMemoryBytes<<",\"rows\":";
            // Use the same JSON escaping as the existing native runner.
            std::cout << benchmark::JsonRows(result->values);
            std::cout<<'}';
        }
        std::cout<<"],\"cgroup_peak_bytes\":"<<counter("memory.peak")<<"}\n";
    }catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
