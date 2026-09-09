#pragma once
#include "executor.h"
#include <numeric>
namespace pgaccel {
struct DirectJoinMembership {
    std::string column;
    int64_t low=0,high=-1;
    std::vector<uint64_t> bits;
    bool contains(int64_t key) const {
        if(key<low || key>high)return false;
        uint64_t offset=uint64_t(key)-uint64_t(low);
        return (bits[offset/64]>>(offset%64))&1;
    }
};
inline int64_t DirectJoinKey(const RawColumnDataBase &data,int row) {
    switch(data.bytesPerValue) {
    case 1:return reinterpret_cast<const int8_t *>(data.values)[row];
    case 2:return reinterpret_cast<const int16_t *>(data.values)[row];
    case 4:return reinterpret_cast<const int32_t *>(data.values)[row];
    case 8:return reinterpret_cast<const int64_t *>(data.values)[row];
    default:throw std::invalid_argument("Invalid direct join key width");
    }
}
// Necessary membership tests only. The original join still expands duplicates
// and evaluates every predicate; source tables retain their complete rows.
inline std::vector<DirectJoinMembership> PrepareDirectMembershipsAtDepth(const QueryDesc &q,int fact,
    const TableRegistry &loaded,bool avx,uint64_t &byteLimit,std::vector<int> path) {
    if(path.size()>=3 || std::find(path.begin(),path.end(),fact)!=path.end())return {};
    path.push_back(fact);
    if(fact<0 || size_t(fact)>=q.tables.size() || !q.leftJoins.empty() || !q.existenceJoins.empty() ||
       !q.scalarQueries.empty() || !q.membershipQueries.empty() || !q.materializedSources.empty() ||
       std::count(q.tables.begin(),q.tables.end(),q.tables[fact])!=1)return {};
    std::vector<ColumnRef> columns;std::vector<int> parents;
    auto index=[&](const ColumnRef &c) {
        auto it=std::find(columns.begin(),columns.end(),c);if(it!=columns.end())return int(it-columns.begin());
        int id=columns.size();columns.push_back(c);parents.push_back(id);return id;
    };
    auto root=[&](int n){while(parents[n]!=n)n=parents[n];return n;};
    for(const auto &e:q.predicates) {
        if(e->kind!=Expression::EQ || !e->left || !e->right || e->left->kind!=Expression::COLUMN ||
           e->right->kind!=Expression::COLUMN || !e->left->column || !e->right->column)continue;
        auto a=*e->left->column,b=*e->right->column;
        auto integer=[](const ColumnRef &c){return c.tableIdx>=0 && (c.Type()->type_num()==INT32_TYPE || c.Type()->type_num()==INT64_TYPE);};
        if(!integer(a) || !integer(b))continue;
        int ai=index(a),bi=index(b);parents[root(ai)]=root(bi);
    }
    std::vector<DirectJoinMembership> result;
    for(size_t f=0;f<columns.size();++f)if(columns[f].tableIdx==fact) {
        for(size_t source=0;source<columns.size();++source) {
            const auto &key=columns[source];
            if(std::find(path.begin(),path.end(),key.tableIdx)!=path.end() ||
               root(source)!=root(f) || q.tables[key.tableIdx]==q.tables[fact])continue;
            auto found=loaded.find(q.tables[key.tableIdx]->Name());if(found==loaded.end())continue;
            const auto &table=*found->second;std::vector<FilterClause> filters;
            for(auto filter:q.filterClauses)if(filter.columnRef.tableIdx==key.tableIdx) {
                auto col=table.ColumnIndex(filter.columnRef.Name());if(!col)throw std::invalid_argument("Missing membership filter column");
                filter.columnRef.columnIdx=*col;filter.columnRef.tableIdx=0;filters.push_back(std::move(filter));
            }
            auto constraints=PrepareDirectMembershipsAtDepth(q,key.tableIdx,loaded,avx,byteLimit,path);
            // Returned child bitmaps share the parent's allowance while live.
            // Release their charge on every exit from this iteration.
            uint64_t childBytes=0;for(const auto &c:constraints)childBytes+=c.bits.size()*sizeof(uint64_t);
            struct ReleaseChildCharge {uint64_t &remaining;uint64_t bytes;
                ~ReleaseChildCharge(){remaining+=bytes;}} release{byteLimit,childBytes};
            if(filters.empty() && constraints.empty())continue;
            std::vector<size_t> constraintColumns;
            for(const auto &c:constraints) {
                auto column=table.ColumnIndex(c.column);
                if(!column)throw std::invalid_argument("Missing chained membership column");
                constraintColumns.push_back(*column);
            }
            auto keyIndex=table.ColumnIndex(key.Name());if(!keyIndex)throw std::invalid_argument("Missing membership key");
            DirectJoinMembership membership;membership.column=columns[f].Name();
            int64_t low=INT64_MAX,high=INT64_MIN;
            for(int g=0;g<table.RowGroupCount();++g) {
                const auto &batch=table.GetRowGroup(g);if(!batch.size)continue;
                const auto &data=batch.columns[*keyIndex];
                if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw std::invalid_argument("Non-numeric membership key");
                if(key.Type()->type_num()==INT32_TYPE) {
                    const auto &d=static_cast<const RawColumnData<Int32Type>&>(*data);low=std::min(low,int64_t(d.minValue));high=std::max(high,int64_t(d.maxValue));
                }else {
                    const auto &d=static_cast<const RawColumnData<Int64Type>&>(*data);low=std::min(low,d.minValue);high=std::max(high,d.maxValue);
                }
            }
            if(low<=high) {
                __int128 width=__int128(high)-low+1,words=(width+63)/64;
                // Sparse/extreme domains simply decline this optional pruning.
                if(words*8>byteLimit)continue;
                membership.low=low;membership.high=high;membership.bits.resize(size_t(words));byteLimit-=size_t(words)*8;
                auto filter=filters.empty()?FilterNodeP{}:CreateFilterNode(filters,avx);uint8_t bitmap[RowGroupSize/8]{};
                for(int g=0;g<table.RowGroupCount();++g) {
                    const auto &batch=table.GetRowGroup(g);
                    if(filter) {if(!filter->ExecuteSet(batch,bitmap))continue;}
                    else memset(bitmap,0xff,(batch.size+7)/8);
                    for(size_t c:constraintColumns)if(batch.columns[c]->type!=ColumnDataBase::RAW_COLUMN_DATA)
                        throw std::invalid_argument("Invalid chained membership layout");
                    const auto &data=static_cast<const RawColumnDataBase&>(*batch.columns[*keyIndex]);
                    for(int row=0;row<batch.size;++row)if((bitmap[row/8]>>(row%8))&1) {
                        bool accepted=true;
                        for(size_t c=0;c<constraints.size();++c) {
                            const auto &foreign=static_cast<const RawColumnDataBase&>(*batch.columns[constraintColumns[c]]);
                            if(!constraints[c].contains(DirectJoinKey(foreign,row))) {accepted=false;break;}
                        }
                        if(!accepted)continue;
                        auto offset=uint64_t(DirectJoinKey(data,row))-uint64_t(low);
                        membership.bits[offset/64]|=uint64_t(1)<<(offset%64);
                    }
                }
            }
            result.push_back(std::move(membership));
        }
    }
    return result;
}
inline std::vector<DirectJoinMembership> PrepareDirectMemberships(const QueryDesc &q,int fact,
    const TableRegistry &loaded,bool avx,uint64_t byteLimit=uint64_t(256)<<20) {
    return PrepareDirectMembershipsAtDepth(q,fact,loaded,avx,byteLimit,{});
}
inline void IntersectDirectMemberships(const std::vector<DirectJoinMembership> &memberships,
    const std::vector<ColumnDesc> &schema,const RowGroup &batch,std::vector<uint16_t> &rows) {
    for(const auto &membership:memberships) {
        auto column=std::find_if(schema.begin(),schema.end(),[&](const auto &c){return c.name==membership.column;});
        if(column==schema.end())throw std::invalid_argument("Missing direct foreign key");
        const auto &data=batch.columns[column-schema.begin()];
        if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw std::invalid_argument("Invalid direct foreign key layout");
        const auto &raw=static_cast<const RawColumnDataBase&>(*data);
        std::erase_if(rows,[&](uint16_t row){return !membership.contains(DirectJoinKey(raw,row));});
    }
}
}
