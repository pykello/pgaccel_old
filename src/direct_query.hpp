#pragma once
#include "direct_table.hpp"
#include "query_columns.hpp"
#include "util.h"
#include "executor.h"
#include <filesystem>
#include <cstring>

namespace pgaccel {
struct PreparedDirectQuery {
    TableRegistry tables; // owns every physical table referenced by query
    QueryDesc query;
    DirectScanStats loadIO;
};
// Preparation retains the projected working set. It must run under the caller's
// total memory limit; query.memoryBudgetBytes alone does not bound loaded data.
inline Result<PreparedDirectQuery> PrepareDirectQuery(const std::string &directory,
                                                     const std::string &sql, bool useAvx = true) {
    try {
        TableRegistry schemas;
        std::map<const ColumnarTable *,std::filesystem::path> paths;
        for(const auto &entry:std::filesystem::directory_iterator(directory)) {
            if(!entry.is_regular_file() || entry.path().extension()!=".pgaccel")continue;
            auto name=ToLower(entry.path().stem().string());
            auto schema=ColumnarTable::LoadSchema(name,entry.path().string());
            if(!schema.ok())return schema.status();
            paths[(*schema).get()]=entry.path();
            if(!schemas.emplace(name,std::move(schema).ValueUnsafe()).second)
                return Status::Invalid("Duplicate direct table name");
        }
        auto parsed=ParseSelect(sql,schemas);if(!parsed.ok())return parsed.status();
        auto needed=RequiredQueryColumns(*parsed);
        // A physical table may serve several aliases/scopes. Until selections
        // are owned per scope, only push filters in an ordinary inner query.
        bool push = parsed->leftJoins.empty() && parsed->existenceJoins.empty() &&
            parsed->scalarQueries.empty() && parsed->membershipQueries.empty() &&
            parsed->materializedSources.empty();
        std::set<const ColumnarTable *> unique;
        for (auto table:parsed->tables) if (!unique.insert(table).second) push=false;
        // Preserve dimension key sequences used by the current join kernels.
        // For multiple tables, compact only the sole SUM-input table. Sparse
        // dimension materialization needs a different row-directory contract.
        std::set<int> measures;
        auto measure = [&](auto &&self, const std::shared_ptr<Expression> &e)->void {
            if (!e) return;
            if (e->column) measures.insert(e->column->tableIdx);
            self(self,e->left); self(self,e->right); self(self,e->condition);
        };
        for (const auto &a:parsed->aggregateClauses)
            if (a.type==AggregateClause::AGGREGATE_SUM) measure(measure,a.expression);
        if (parsed->tables.size()==1) measures={0};
        const ColumnarTable *compactTable = measures.size()==1 && *measures.begin()>=0 &&
            size_t(*measures.begin())<parsed->tables.size() ? parsed->tables[*measures.begin()] : nullptr;
        PreparedDirectQuery result;
        for(const auto &[name,schema]:schemas) {
            auto fields=needed.find(schema.get());if(fields==needed.end())continue;
            DirectScanStats io;
            std::vector<FilterClause> filters;
            if (push && schema.get()==compactTable) for (const auto &filter:parsed->filterClauses)
                if (parsed->tables[filter.columnRef.tableIdx]==schema.get()) filters.push_back(filter);
            FilterNodeP node;
            auto select = [&](const auto &columns, const RowGroup &batch)->Result<std::vector<uint16_t>> {
                if (!node) {
                    for (auto &filter:filters) {
                        auto found=std::find_if(columns.begin(),columns.end(),[&](const auto &c){
                            return c.name==filter.columnRef.Name();
                        });
                        if (found==columns.end()) return Status::Invalid("Missing direct filter column");
                        filter.columnRef.tableIdx=0;
                        filter.columnRef.columnIdx=found-columns.begin();
                    }
                    node=CreateFilterNode(filters,useAvx);
                }
                uint8_t bitmap[RowGroupSize/8]{};
                int count=node->ExecuteSet(batch,bitmap);
                std::vector<uint16_t> rows; rows.reserve(count);
                if (!count) return rows;
                for (int base=0; base<batch.size; base+=64) {
                    uint64_t bits=0; int n=std::min(64,batch.size-base);
                    memcpy(&bits,bitmap+base/8,(n+7)/8);
                    if(n<64)bits&=(uint64_t(1)<<n)-1;
                    while(bits) { rows.push_back(base+__builtin_ctzll(bits)); bits&=bits-1; }
                }
                if (rows.size()!=size_t(count)) return Status::Invalid("Inconsistent direct selection count");
                return rows;
            };
            auto table=filters.empty()
                ? LoadDirectTable(name,paths.at(schema.get()).string(),fields->second,&io)
                : ColumnarTable::LoadDirectSelected(name,paths.at(schema.get()).string(),fields->second,select,&io);
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
