#include "columnar_table.h"
#include "parser.h"
#include "executor.h"
#include "util.h"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
using namespace pgaccel;
static std::string Json(const std::string &s) {
    std::string out="\""; const char *hex="0123456789abcdef";
    for(unsigned char c:s) {
        if(c=='"'||c=='\\') { out+='\\';out+=c; }
        else if(c<32) { out+="\\u00";out+=hex[c>>4];out+=hex[c&15]; }
        else out+=c;
    }
    return out+'"';
}
int main(int argc,char **argv) {
    if(argc<7) { std::cerr<<"query_runner parquet|native PATH TABLE SCHEMA|- PARALLEL AVX [REPEATS=1] [SAVE_PATH]\n";return 2; }
    using Clock=std::chrono::steady_clock;auto begin=Clock::now();
    std::map<std::string,std::string> schema;
    if(std::string(argv[4])!="-") { std::ifstream f(argv[4]);std::string n,t;while(f>>n>>t) schema[ToLower(n)]=t;if(schema.empty())return 2; }
    TableRegistry registry;ColumnarTableP table;
    if(std::string(argv[1])=="native") { auto r=ColumnarTable::Load(argv[3],argv[2]);if(!r.ok()){std::cerr<<r.status().Message();return 2;}table=std::move(r).ValueUnsafe(); }
    else table=ColumnarTable::ImportParquet(argv[3],argv[2],{},schema);
    if(!table)return 2;
    uint64_t count=0;for(int g=0;g<table->RowGroupCount();++g)count+=table->GetRowGroup(g).size;
    std::cout<<std::setprecision(12)<<"{\"status\":\"loaded\",\"rows\":"<<count<<",\"columns\":"<<table->ColumnCount()<<",\"load_ms\":"<<std::chrono::duration<double,std::milli>(Clock::now()-begin).count()<<"}"<<std::endl;
    if(argc>8){auto s=table->Save(argv[8]);if(!s.ok())return 2;std::cout<<"{\"status\":\"saved\"}"<<std::endl;}
    registry.emplace(ToLower(argv[3]),std::move(table));
    int id=0,repeats=argc>7?std::max(1,std::stoi(argv[7])):1;
    std::string sql;
    while(std::getline(std::cin,sql)) {
        if(sql.empty())continue;++id;
        auto parsed=ParseSelect(sql,registry);
        if(!parsed.ok()){std::cout<<"{\"id\":"<<id<<",\"status\":\"parse_failure\",\"error\":"<<Json(parsed.status().Message())<<"}"<<std::endl;continue;}
        auto q=std::move(parsed).ValueUnsafe();
        if(const char *b=std::getenv("PGACCEL_QUERY_MEMORY_BYTES"))q.memoryBudgetBytes=std::stoull(b);
        std::vector<double> times;QueryOutput output;std::string error;
        for(int i=0;i<repeats;++i) {
            auto start=Clock::now();auto r=ExecuteQuery(q,std::stoi(argv[6]),std::stoi(argv[5]));
            double ms=std::chrono::duration<double,std::milli>(Clock::now()-start).count();
            if(!r.ok()){error=r.status().Message();break;}output=std::move(r).ValueUnsafe();times.push_back(ms);
        }
        std::cout<<"{\"id\":"<<id<<",\"status\":"<<Json(error.empty()?"success":"execution_failure");
        if(!error.empty())std::cout<<",\"error\":"<<Json(error);
        std::cout<<",\"ms\":[";for(size_t i=0;i<times.size();++i){if(i)std::cout<<',';std::cout<<times[i];}
        std::cout<<"],\"rows\":[";
        for(size_t r=0;r<output.values.size();++r){if(r)std::cout<<',';std::cout<<'[';for(size_t c=0;c<output.values[r].size();++c){if(c)std::cout<<',';std::cout<<Json(output.values[r][c]);}std::cout<<']';}
        std::cout<<"]}"<<std::endl;
    }
}
