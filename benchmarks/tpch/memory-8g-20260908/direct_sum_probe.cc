// Diagnostic single-table filtered SUM through the direct projected scanner.
// No saved answer or benchmark query identifier is consulted by execution.
#include "columnar_table.h"
#include "executor_arithmetic_program.hpp"
#include "executor_result.hpp"
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>
using namespace pgaccel;
int main(int argc,char **argv) {
    if(argc<5){std::cerr<<"usage: probe native table sql-file column...\n";return 2;}
    try {
        std::ifstream sqlFile(argv[3]);std::string sql((std::istreambuf_iterator<char>(sqlFile)),{});
        std::set<std::string> fields;for(int i=4;i<argc;++i)fields.insert(argv[i]);
        TableRegistry registry;std::optional<QueryDesc> query;FilterNodeP filter;
        Wide total=0;uint64_t rows=0;Budget budget(uint64_t(64)<<20);
        auto start=std::chrono::steady_clock::now();
        auto scan=ColumnarTable::ScanDirect(argv[1],fields,[&](const auto &schema,const RowGroup &batch)->Result<bool>{
            if(!query) {
                std::stringstream metadata,data;metadata<<schema.size()<<'\n';
                for(const auto &c:schema){metadata<<"0 0 "<<c.name<<' '<<c.type->type_num();
                    if(c.type->type_num()==DECIMAL_TYPE)metadata<<' '<<Scale(c.type.get());metadata<<'\n';}
                auto table=ColumnarTable::Load(argv[2],metadata,data);if(!table.ok())return table.status();
                registry.emplace(argv[2],std::move(table).ValueUnsafe());
                auto parsed=ParseSelect(sql,registry);if(!parsed.ok())return parsed.status();
                query=std::move(parsed).ValueUnsafe();
                if(query->tables.size()!=1 || query->grouped || !query->predicates.empty() ||
                   !query->scalarQueries.empty() || !query->membershipQueries.empty() ||
                   !query->materializedSources.empty() || !query->having.empty() || query->limit || query->offset ||
                   query->aggregateClauses.size()!=1 || query->aggregateClauses[0].type!=AggregateClause::AGGREGATE_SUM)
                    return Status::Invalid("Probe supports one filtered SUM only");
                if(!query->filterClauses.empty())filter=CreateFilterNode(query->filterClauses,true);
            }
            uint8_t bitmap[RowGroupSize/8];
            int count=filter?filter->ExecuteSet(batch,bitmap):batch.size;
            if(!count)return true;
            Program program;int root=program.compile(*query->aggregateClauses[0].expression,batch);
            if(program.ops[root].low*count<INT64_MIN || program.ops[root].high*count>INT64_MAX)
                return Status::Invalid("Probe SUM bounds exceed int64");
            alignas(64) int64_t buffers[32*Block];uint32_t selected[Block];int n=0;
            auto consume=[&]{program.evaluate<true>(0,n,buffers,true,selected);
                for(int i=0;i<n;++i)total=Add(total,buffers[root*Block+i]);n=0;};
            Selected(batch,count==batch.size?nullptr:bitmap,[&](int row){selected[n++]=row;if(n==Block)consume();});
            if(n)consume();rows+=count;return true;
        });
        if(!scan.ok()){std::cerr<<scan.status().Message()<<'\n';return 1;}
        if(!query){std::cerr<<"Empty-table probe not supported\n";return 1;}
        double ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-start).count();
        std::ifstream cg("/proc/self/cgroup");std::string cgLine;std::getline(cg,cgLine);
        if(!cgLine.starts_with("0::/"))throw std::runtime_error("Need unified cgroup accounting");
        auto cgPath=std::filesystem::path("/sys/fs/cgroup")/cgLine.substr(4);
        auto counter=[&](const char *name){std::ifstream in(cgPath/name);uint64_t value;
            if(!(in>>value))throw std::runtime_error(std::string("Missing numeric cgroup counter: ")+name);return value;};
        std::cout<<"{\"cgroup_peak_bytes\":"<<counter("memory.peak")
            <<",\"memory_max\":"<<counter("memory.max")<<",\"swap_max\":"<<counter("memory.swap.max")
            <<",\"elapsed_ms\":"<<ms<<",\"rows\":"<<scan->rows<<",\"selected\":"<<rows
            <<",\"read_bytes\":"<<scan->bytesRead<<",\"read_calls\":"<<scan->readCalls
            <<",\"io_buffers\":"<<scan->ioBufferBytes<<",\"answer\":\""
            <<(rows?Number(total,Scale(query->aggregateClauses[0].expression->type.get())):"NULL")<<"\"}\n";
    }catch(const NotDense &){std::cerr<<"Unsupported direct SUM arithmetic\n";return 1;}
    catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
