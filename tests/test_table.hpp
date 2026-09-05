#pragma once
#include "columnar_table.h"
#include "executor.h"
#include <cstring>
namespace pgaccel::test {
inline TableRegistry Table(std::vector<std::vector<ColumnDataP>> groups,
                           const std::vector<std::pair<std::string,std::shared_ptr<AccelType>>> &schema) {
    std::stringstream data,meta;
    meta << schema.size() << '\n';
    for (size_t c=0;c<schema.size();++c) {
        meta << data.tellp() << ' ' << groups.size() << ' ' << schema[c].first << ' ' << schema[c].second->type_num();
        if (schema[c].second->type_num()==DECIMAL_TYPE) meta << ' ' << static_cast<DecimalType*>(schema[c].second.get())->scale;
        meta << '\n';
        for (auto &g:groups) g[c]->Save(data);
    }
    data.seekg(0); meta.seekg(0);
    auto loaded=ColumnarTable::Load("t",meta,data);
    if (!loaded.ok()) throw std::runtime_error(loaded.status().Message());
    TableRegistry registry; registry.emplace("t",std::move(loaded).ValueUnsafe()); return registry;
}
inline ColumnDataP Raw(const std::vector<int64_t> &values,int width=8) {
    auto d=std::make_shared<RawColumnData<Int64Type>>(); d->type=ColumnDataBase::RAW_COLUMN_DATA;
    d->size=values.size(); d->bytesPerValue=width;
    d->minValue=values.empty()?0:*std::min_element(values.begin(),values.end());
    d->maxValue=values.empty()?0:*std::max_element(values.begin(),values.end());
    d->values=(uint8_t*)AllocateAligned(512,values.size()*width);
    for(size_t i=0;i<values.size();++i) switch(width) {
    case 1: ((int8_t*)d->values)[i]=values[i];break;
    case 2: ((int16_t*)d->values)[i]=values[i];break;
    case 4: ((int32_t*)d->values)[i]=values[i];break;
    case 8: ((int64_t*)d->values)[i]=values[i];break;
    }
    return d;
}
template<class T> ColumnDataP Dict(const std::vector<typename T::c_type> &values) {
    auto d=std::make_shared<DictColumnData<T>>(); d->type=ColumnDataBase::DICT_COLUMN_DATA;
    d->valueType=std::make_shared<T>(); d->size=values.size(); d->dict=values;
    std::sort(d->dict.begin(),d->dict.end()); d->dict.erase(std::unique(d->dict.begin(),d->dict.end()),d->dict.end());
    d->values=(uint8_t*)AllocateAligned(512,values.size()*d->bytesPerValue());
    for(size_t i=0;i<values.size();++i) {
        int code=std::lower_bound(d->dict.begin(),d->dict.end(),values[i])-d->dict.begin();
        if(d->bytesPerValue()==1) d->values[i]=code; else ((uint16_t*)d->values)[i]=code;
    }
    return d;
}
}
