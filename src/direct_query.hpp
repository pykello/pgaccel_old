#pragma once
#include "direct_table.hpp"
#include "query_columns.hpp"
#include <filesystem>

namespace pgaccel {
struct PreparedDirectQuery {
    TableRegistry tables; // owns every physical table referenced by query
    QueryDesc query;
    DirectScanStats loadIO;
};
// Preparation retains the projected working set. It must run under the caller's
// total memory limit; query.memoryBudgetBytes alone does not bound loaded data.
inline Result<PreparedDirectQuery> PrepareDirectQuery(const std::string &directory,
                                                     const std::string &sql) {
    try {
        TableRegistry schemas;
        std::map<const ColumnarTable *,std::filesystem::path> paths;
        for(const auto &entry:std::filesystem::directory_iterator(directory)) {
            if(!entry.is_regular_file() || entry.path().extension()!=".pgaccel")continue;
            auto name=entry.path().stem().string();
            auto schema=ColumnarTable::LoadSchema(name,entry.path().string());
            if(!schema.ok())return schema.status();
            paths[(*schema).get()]=entry.path();
            if(!schemas.emplace(name,std::move(schema).ValueUnsafe()).second)
                return Status::Invalid("Duplicate direct table name");
        }
        auto parsed=ParseSelect(sql,schemas);if(!parsed.ok())return parsed.status();
        auto needed=RequiredQueryColumns(*parsed);
        PreparedDirectQuery result;
        for(const auto &[name,schema]:schemas) {
            auto fields=needed.find(schema.get());if(fields==needed.end())continue;
            DirectScanStats io;
            auto table=LoadDirectTable(name,paths.at(schema.get()).string(),fields->second,&io);
            if(!table.ok())return table.status();
            result.loadIO.bytesRead+=io.bytesRead;result.loadIO.readCalls+=io.readCalls;
            result.loadIO.rows+=io.rows;result.loadIO.groups+=io.groups;
            result.loadIO.ioBufferBytes=std::max(result.loadIO.ioBufferBytes,io.ioBufferBytes);
            result.tables.emplace(name,std::move(table).ValueUnsafe());
        }
        auto rebound=ParseSelect(sql,result.tables);if(!rebound.ok())return rebound.status();
        result.query=std::move(rebound).ValueUnsafe();
        return result;
    }catch(const std::exception &e){return Status::Invalid(e.what());}
}
}
