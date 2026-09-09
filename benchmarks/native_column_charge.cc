#include "columnar_table.h"
#include "native_column_charge.hpp"
#include "json_rows.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/resource.h>
using namespace pgaccel;
static std::string Json(std::string_view value) {
    std::string result;benchmark::AppendJsonString(result,value);return result;
}
int main(int argc,char **argv) {
    if(argc!=2){std::cerr<<"native_column_charge SNAPSHOT_DIR < table_column_pairs\n";return 2;}
    try {
        std::map<std::string,std::set<std::string>> requests;
        std::string table,column;
        while(std::cin>>table>>column)requests[table].insert(column);
        if(requests.empty())throw std::runtime_error("No columns requested");
        std::vector<ColumnDataP> retained;
        uint64_t total=0,reads=0;
        for(const auto &[name,fields]:requests) {
            std::map<std::string,uint64_t> charges,rows,blocks;
            auto scan=ColumnarTable::ScanDirect((std::filesystem::path(argv[1])/(name+".pgaccel")).string(),fields,
                [&](const auto &columns,const RowGroup &batch)->Result<bool>{
                    for(size_t i=0;i<columns.size();++i) {
                        auto charge=NativeColumnCharge(*batch.columns[i],*columns[i].type);
                        if(!charge)return Status::Invalid("Unsupported native ownership");
                        charges[columns[i].name]+=*charge;rows[columns[i].name]+=batch.size;
                        ++blocks[columns[i].name];total+=*charge;
                        retained.push_back(batch.columns[i]);
                    }
                    return true;
                });
            if(!scan.ok())throw std::runtime_error(scan.status().Message());
            reads+=scan->bytesRead;
            for(const auto &[field,bytes]:charges)
                std::cout<<"{\"table\":"<<Json(name)<<",\"column\":"<<Json(field)
                         <<",\"charge_bytes\":"<<bytes<<",\"rows\":"<<rows[field]<<",\"blocks\":"<<blocks[field]<<"}\n";
        }
        struct rusage usage{};getrusage(RUSAGE_SELF,&usage);
        std::cout<<"{\"summary\":true,\"column_charge_bytes\":"<<total
                 <<",\"pointer_capacity_bytes\":"<<retained.capacity()*sizeof(ColumnDataP)
                 <<",\"peak_rss_bytes\":"<<uint64_t(usage.ru_maxrss)*1024
                 <<",\"read_bytes\":"<<reads<<"}\n";
        return std::cout.good()?0:1;
    } catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
