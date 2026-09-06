#pragma once
#include "executor_state.hpp"
#include "executor_arithmetic_program.hpp"

namespace pgaccel {
namespace {
// Zero is reserved for a rejected join. Dictionary codes are translated by
// their actual date values, independently for each stripe.
struct YearCodeDomain {
    int first=INT_MAX,last=INT_MIN;
    static int year(int64_t day) {
        if(day < -719528 || day > 2932896)throw NotDense{};
        return int(std::chrono::year_month_day{std::chrono::sys_days{std::chrono::days{day}}}.year());
    }
    YearCodeDomain(const ColumnarTable &table,const ColumnRef &column) {
        if(column.Type()->type_num()!=DATE_TYPE)throw NotDense{};
        for(int g=0;g<table.RowGroupCount();++g) {
            const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
            auto data=rg.columns[column.columnIdx].get();int64_t low,high;
            if(data->type==ColumnDataBase::RAW_COLUMN_DATA) {
                auto raw=static_cast<const RawColumnData<DateType> *>(data);
                low=raw->minValue;high=raw->maxValue;
            } else if(data->type==ColumnDataBase::DICT_COLUMN_DATA) {
                const auto &dict=static_cast<const DictColumnData<DateType> *>(data)->dict;
                if(dict.empty())throw NotDense{};low=dict.front();high=dict.back();
            } else throw NotDense{};
            first=std::min(first,year(low));last=std::max(last,year(high));
        }
        if(first>last || last-first>=64)throw NotDense{};
    }
    uint32_t size() const {return last-first+1;}
    uint32_t encode(int64_t day) const {
        int y=year(day);if(y<first || y>last)throw NotDense{};
        return y-first+1;
    }
};
class YearCodeStripe {
    const YearCodeDomain &domain;
    const ColumnDataBase *data;
    std::pmr::vector<uint8_t> labels;
public:
    YearCodeStripe(const YearCodeDomain &domain,const RowGroup &rg,const ColumnRef &column,Budget &budget)
        :domain(domain),data(rg.columns[column.columnIdx].get()),labels(&budget) {
        if(data->size!=rg.size)throw NotDense{};
        if(data->type==ColumnDataBase::DICT_COLUMN_DATA) {
            const auto &dict=static_cast<const DictColumnData<DateType> *>(data)->dict;
            labels.reserve(dict.size());for(auto day:dict)labels.push_back(domain.encode(day));
        } else if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
    }
    uint32_t get(int row) const {
        if(data->type==ColumnDataBase::DICT_COLUMN_DATA) {
            auto dict=static_cast<const DictColumnDataBase *>(data);
            auto code=dict->bytesPerValue()==1?dict->values[row]:reinterpret_cast<const uint16_t *>(dict->values)[row];
            return labels[code];
        }
        auto raw=static_cast<const RawColumnDataBase *>(data);int64_t day;
        switch(raw->bytesPerValue) {
        case 1:day=reinterpret_cast<const int8_t *>(raw->values)[row];break;
        case 2:day=reinterpret_cast<const int16_t *>(raw->values)[row];break;
        case 4:day=reinterpret_cast<const int32_t *>(raw->values)[row];break;
        case 8:day=reinterpret_cast<const int64_t *>(raw->values)[row];break;
        default:throw NotDense{};
        }
        return domain.encode(day);
    }
};
}
}
