#include "columnar_table.h"
#include "query_columns.hpp"
#include "predicate_columns.hpp"
#include "date_predicate_columns.hpp"
#include "json_rows.hpp"
#include "util.h"
#include <filesystem>
#include <iostream>

using namespace pgaccel;
static std::string Json(std::string_view value) {
    std::string result;benchmark::AppendJsonString(result,value);return result;
}
int main(int argc,char **argv) {
    if(argc!=2){std::cerr<<"native_inventory SNAPSHOT_DIRECTORY < queries.sql\n";return 2;}
    try {
        TableRegistry schemas;
        for(const auto &entry:std::filesystem::directory_iterator(argv[1])) {
            if(!entry.is_regular_file() || entry.path().extension()!=".pgaccel")continue;
            auto name=ToLower(entry.path().stem().string());
            auto schema=ColumnarTable::LoadSchema(name,entry.path().string());
            if(!schema.ok())throw std::runtime_error(schema.status().Message());
            if(!schemas.emplace(name,std::move(schema).ValueUnsafe()).second)
                throw std::runtime_error("Duplicate snapshot table");
        }
        if(schemas.empty())throw std::runtime_error("No native schemas");
        std::string sql;int query=0;
        while(std::getline(std::cin,sql)) {
            if(sql.empty())continue;++query;
            auto parsed=ParseSelect(sql,schemas);
            if(!parsed.ok())throw std::runtime_error(parsed.status().Message());
            auto required=RequiredQueryColumns(*parsed);
            auto predicates=PredicateOnlyColumns(*parsed);
            auto dates=DatePredicateOnlyColumns(*parsed);
            std::cout<<"{\"query\":"<<query<<",\"columns\":[";bool comma=false;
            for(const auto &[name,table]:schemas) {
                auto fields=required.find(table.get());if(fields==required.end())continue;
                for(const auto &field:fields->second) {
                    bool predicate=predicates.contains(table.get()) && predicates.at(table.get()).contains(field);
                    bool date=false;
                    if(dates.contains(table.get()))for(const auto &pair:dates.at(table.get()))
                        date|=pair.first==field || pair.second==field;
                    if(comma)std::cout<<',';comma=true;
                    std::cout<<"{\"table\":"<<Json(name)<<",\"column\":"<<Json(field)
                             <<",\"string_predicate_only\":"<<(predicate?"true":"false")
                             <<",\"date_predicate_only\":"<<(date?"true":"false")<<'}';
                }
            }
            std::cout<<"]}\n";
        }
        return std::cout.good()?0:1;
    } catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
