#pragma once
#include "executor_state.hpp"
#include "executor_arithmetic_program.hpp"
#include <bit>
namespace pgaccel {
namespace {
// Query-local exact identities for a small product of dictionary/numeric
// domains. Equal local codes in different stripes are never assumed equal.
class SmallGroupKey {
    struct Frame {
        const ColumnDataBase *data;
        std::pmr::vector<uint32_t> labels;
        Frame(const ColumnDataBase *source,Budget &budget):data(source),labels(&budget) {}
    };
    struct Component {
        TypeNum type;unsigned shift=0,bits=0;int64_t minimum=0;
        std::pmr::vector<Frame> frames;
        Component(TypeNum t,Budget &budget):type(t),frames(&budget) {}
    };
    std::pmr::vector<Component> components;
    unsigned width=0;
    static uint32_t code(const DictColumnDataBase &data,int row) {
        return data.bytesPerValue()==1?data.values[row]:reinterpret_cast<const uint16_t *>(data.values)[row];
    }
    static int64_t number(const Component &component,const Frame &frame,int row) {
        if(frame.data->type==ColumnDataBase::DICT_COLUMN_DATA) {
            auto dictionary=static_cast<const DictColumnDataBase *>(frame.data);auto id=code(*dictionary,row);
            switch(component.type) {
            case INT32_TYPE:return static_cast<const DictColumnData<Int32Type> *>(dictionary)->dict[id];
            case DATE_TYPE:return static_cast<const DictColumnData<DateType> *>(dictionary)->dict[id];
            default:throw NotDense{};
            }
        }
        auto raw=static_cast<const RawColumnDataBase *>(frame.data);
        switch(raw->bytesPerValue) {
        case 1:return reinterpret_cast<const int8_t *>(raw->values)[row];
        case 2:return reinterpret_cast<const int16_t *>(raw->values)[row];
        case 4:return reinterpret_cast<const int32_t *>(raw->values)[row];
        case 8:return reinterpret_cast<const int64_t *>(raw->values)[row];
        default:throw NotDense{};
        }
    }
public:
    SmallGroupKey(const ColumnarTable &table,const std::vector<ColumnRef> &columns,Budget &budget):components(&budget) {
        if(columns.empty() || columns.size()>4)throw NotDense{};
        components.reserve(columns.size());
        for(const auto &column:columns) {
            components.emplace_back(column.Type()->type_num(),budget);auto &component=components.back();component.shift=width;
            if(component.type!=STRING_TYPE && component.type!=INT32_TYPE && component.type!=INT64_TYPE && component.type!=DATE_TYPE)throw NotDense{};
            component.frames.reserve(table.RowGroupCount());
            std::pmr::unordered_map<std::string_view,uint32_t> strings(&budget);
            int64_t minimum=INT64_MAX,maximum=INT64_MIN;
            for(int g=0;g<table.RowGroupCount();++g) {
                const auto &rg=table.GetRowGroup(g);auto data=rg.columns[column.columnIdx].get();
                component.frames.emplace_back(data,budget);auto &frame=component.frames.back();if(!rg.size)continue;
                if(data->size!=rg.size)throw NotDense{};
                if(component.type==STRING_TYPE) {
                    if(data->type!=ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
                    const auto &dictionary=static_cast<const DictColumnData<StringType> *>(data)->dict;
                    frame.labels.reserve(dictionary.size());
                    for(const auto &label:dictionary) {
                        auto [it,added]=strings.emplace(std::string_view(label),uint32_t(strings.size()));
                        if(strings.size()>65536)throw NotDense{};
                        frame.labels.push_back(it->second);
                    }
                } else {
                    if(component.type==INT64_TYPE && data->type==ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
                    int64_t low,high;
                    auto bounds=[&]<class T>() {
                        if(data->type==ColumnDataBase::DICT_COLUMN_DATA) {
                            const auto &dictionary=static_cast<const DictColumnData<T> *>(data)->dict;
                            if(dictionary.empty())throw NotDense{};low=dictionary.front();high=dictionary.back();
                        } else if(data->type==ColumnDataBase::RAW_COLUMN_DATA) {
                            auto raw=static_cast<const RawColumnData<T> *>(data);low=raw->minValue;high=raw->maxValue;
                        } else throw NotDense{};
                    };
                    switch(component.type) {
                    case INT32_TYPE:bounds.template operator()<Int32Type>();break;
                    case INT64_TYPE:bounds.template operator()<Int64Type>();break;
                    case DATE_TYPE:bounds.template operator()<DateType>();break;
                    default:throw NotDense{};
                    }
                    minimum=std::min(minimum,low);maximum=std::max(maximum,high);
                }
            }
            if(component.type==STRING_TYPE)component.bits=std::bit_width(strings.empty()?size_t(0):strings.size()-1);
            else {
                component.minimum=minimum;Wide difference=Wide(maximum)-minimum;
                if(minimum>maximum)component.bits=0;
                else {if(difference>UINT32_MAX)throw NotDense{};component.bits=std::bit_width(uint32_t(difference));}
            }
            if(component.bits>24-width)throw NotDense{};width+=component.bits;
        }
    }
    uint32_t slots() const {return uint32_t(1)<<width;}
    uint32_t word(int group,int row) const {
        uint32_t result=0;
        for(const auto &component:components) {
            const auto &frame=component.frames[group];uint64_t value;
            if(component.type==STRING_TYPE) {
                auto id=code(*static_cast<const DictColumnDataBase *>(frame.data),row);
                if(id>=frame.labels.size())throw NotDense{};value=frame.labels[id];
            } else value=uint64_t(number(component,frame,row))-uint64_t(component.minimum);
            if(value>=(uint64_t(1)<<component.bits))throw NotDense{};
            result|=uint32_t(value)<<component.shift;
        }
        return result;
    }
};
}
}
