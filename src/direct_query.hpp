#pragma once
#include "direct_table.hpp"
#include "query_columns.hpp"
#include "util.h"
#include "executor.h"
#include "predicate_columns.hpp"
#include "date_predicate_columns.hpp"
#include "compact_column.hpp"
#include "direct_join_membership.hpp"
#include "direct_literal_filters.hpp"
#include "direct_scope_filters.hpp"
#include <filesystem>
#include <cstring>
#include <numeric>

namespace pgaccel {
struct PreparedDirectQuery {
    TableRegistry tables; // statement-specific data; execute only the bound query
    QueryDesc query;
    DirectScanStats loadIO;
};
// Preparation retains the projected working set. It must run under the caller's
// total memory limit; query.memoryBudgetBytes alone does not bound loaded data.
inline Result<PreparedDirectQuery> PrepareDirectQuery(const std::string &directory,
                                                     const std::string &sql, bool useAvx = true,
                                                     bool parallel = false) {
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
        auto preparation=DirectPreparationFilters(*parsed);
        auto needed=RequiredQueryColumns(*parsed);
        auto predicateColumns=PredicateOnlyColumns(*parsed);
        auto datePredicates=DatePredicateOnlyColumns(*parsed);
        auto scopeFilters=DirectSingleTableFilters(*parsed);
        // Scalar scopes may share physical pruning only after every use is
        // proved to require the same inner-join domain. Execution keeps the
        // original scalar queries and recomputes their results each time.
        bool push = parsed->scalarQueries.empty()
            ? parsed->leftJoins.empty() && parsed->existenceJoins.empty() &&
              parsed->membershipQueries.empty() && parsed->materializedSources.empty()
            : DirectSharedJoinScopes(*parsed);
        if(push)preparation.scalarQueries.clear();
        // Preserve dimension key sequences used by the current join kernels.
        // For multiple tables, compact only one SUM-input table. Sparse
        // dimension materialization needs a different row-directory contract.
        std::set<int> measures;
        auto measure = [&](auto &&self, const std::shared_ptr<Expression> &e)->void {
            if (!e) return;
            if (e->column) measures.insert(e->column->tableIdx);
            self(self,e->left); self(self,e->right);
            // A CASE condition may read a dimension while both contributed
            // values come from one fact table. Only its values identify the
            // candidate; the original WHERE predicates still govern removal.
            if (e->kind!=Expression::CASE) self(self,e->condition);
        };
        for (const auto &a:parsed->aggregateClauses)
            if (a.type==AggregateClause::AGGREGATE_SUM) measure(measure,a.expression);
        // Conditional counts have constant CASE values, so they contribute no
        // measure column above. The grouped input is the probe of the grouped
        // count join; only that input may lose rows, leaving dimensions intact.
        if(measures.empty() && parsed->tables.size()==2 && parsed->groupExpressions.size()==1) {
            const auto &group=parsed->groupExpressions[0];
            bool conditional=false;
            for(const auto &a:parsed->aggregateClauses) {
                const auto &e=a.expression;
                if(a.type==AggregateClause::AGGREGATE_SUM && e && e->kind==Expression::CASE &&
                   e->left && e->right && e->left->kind==Expression::CONSTANT &&
                   e->right->kind==Expression::CONSTANT)conditional=true;
            }
            if(conditional && group->kind==Expression::COLUMN && group->column &&
               (group->type->type_num()==STRING_TYPE || group->type->type_num()==DATE_TYPE))
                measures.insert(group->column->tableIdx);
        }
        if (parsed->tables.size()==1) measures={0};
        // Use snapshot stripe counts to choose the largest aggregate input.
        // Only that table loses rows; dimension key sequences remain intact.
        const ColumnarTable *compactTable=nullptr;int largest=-1;
        for(int index:measures)if(index>=0 && size_t(index)<parsed->tables.size()) {
            auto table=parsed->tables[index];int groups=table->SourceRowGroupCount();
            if(groups>largest){largest=groups;compactTable=table;}
        }
        // Other dimensions can have several aliases without sharing the
        // candidate's rows. Only aliases of the compacted table prevent this
        // scope-local selection from being applied to its physical storage.
        if (std::count(parsed->tables.begin(),parsed->tables.end(),compactTable)!=1) push=false;
        PreparedDirectQuery result;
        std::vector<std::string> loadOrder;
        for(const auto &[name,schema]:schemas)if(needed.count(schema.get()))loadOrder.push_back(name);
        if(push)std::stable_sort(loadOrder.begin(),loadOrder.end(),[&](const auto &a,const auto &b) {
            return (schemas.at(a).get()==compactTable)<(schemas.at(b).get()==compactTable);
        });
        for(const auto &name:loadOrder) {
            const auto &schema=schemas.at(name);
            auto fields=needed.find(schema.get());if(fields==needed.end())continue;
            DirectScanStats io;
            std::vector<FilterClause> filters=scopeFilters[schema.get()];
            if (push && schema.get()==compactTable) for (const auto &filter:preparation.filterClauses)
                if (parsed->tables[filter.columnRef.tableIdx]==schema.get() &&
                    std::none_of(filters.begin(),filters.end(),[&](const auto &f){
                        return f.columnRef.Name()==filter.columnRef.Name() && f.op==filter.op &&
                            f.value==filter.value && f.values==filter.values;
                    })) filters.push_back(filter);
            std::vector<DirectJoinMembership> memberships;
            if(push && schema.get()==compactTable) {
                int fact=std::find(parsed->tables.begin(),parsed->tables.end(),compactTable)-parsed->tables.begin();
                memberships=PrepareDirectMemberships(preparation,fact,result.tables,useAvx);
            }
            FilterNodeP node;
            auto selectRows = [&](const auto &columns, const RowGroup &batch)->Result<std::vector<uint16_t>> {
                if(filters.empty()) {
                    std::vector<uint16_t> rows(batch.size);std::iota(rows.begin(),rows.end(),0);return rows;
                }
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
            auto dates=datePredicates.find(schema.get());
            std::map<std::string,ColumnDataP> dateColumns;
            std::vector<uint16_t> selectedRows;
            auto select=[&](const auto &columns,const RowGroup &batch)->Result<std::vector<uint16_t>> {
                dateColumns.clear();
                if(dates!=datePredicates.end()) {
                    auto encoded=DatePredicateDictionaries(columns,batch,dates->second);
                    if(!encoded.ok())return encoded.status();dateColumns=std::move(encoded).ValueUnsafe();
                }
                auto selection=selectRows(columns,batch);if(!selection.ok())return selection.status();
                auto rows=std::move(selection).ValueUnsafe();
                IntersectDirectMemberships(memberships,columns,batch,rows);
                // Preparation pruning is optional: the original SQL still
                // checks its predicates. Avoid copying nearly complete large
                // stripes; the exact selected-load API remains unchanged.
                if(batch.size>=16384 && rows.size()<size_t(batch.size) &&
                   rows.size()*100>=size_t(batch.size)*95) {
                    rows.resize(batch.size);std::iota(rows.begin(),rows.end(),0);
                }
                if(dates!=datePredicates.end())selectedRows=rows;
                return rows;
            };
            auto predicates=predicateColumns.find(schema.get());
            ColumnTransform transform;
            if(predicates!=predicateColumns.end() || dates!=datePredicates.end())transform=[&](const ColumnDesc &column,const ColumnDataP &data)->Result<ColumnDataP> {
                auto date=dateColumns.find(column.name);
                if(date!=dateColumns.end()) {
                    if(date->second->size!=data->size)return CompactNativeColumn(*date->second,*column.type,selectedRows);
                    return ColumnDataP(date->second);
                }
                if(predicates==predicateColumns.end())return ColumnDataP(data);
                auto found=predicates->second.find(column.name);
                return found==predicates->second.end()?Result<ColumnDataP>(ColumnDataP(data)):PredicateDictionary(data,found->second,useAvx);
            };
            auto table=filters.empty() && memberships.empty() && !transform
                ? LoadDirectTable(name,paths.at(schema.get()).string(),fields->second,&io)
                : ColumnarTable::LoadDirectSelected(name,paths.at(schema.get()).string(),fields->second,select,&io,transform,parallel);
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
