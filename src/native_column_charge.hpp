#pragma once
#include "column_data.hpp"
#include <optional>
#include <limits>

namespace pgaccel {
// For unmodified blocks from the native loader; callers establish provenance.
// This is an admission charge, not content validation or an RSS measurement. Include
// capacities, aligned payload rounding and conservative allocation overhead.
// Auxiliary views require external owners and are deliberately unsupported.
inline std::optional<uint64_t> NativeColumnCharge(const ColumnDataBase &column,
                                                 const AccelType &logical,
                                                 uint64_t limit=UINT64_MAX) {
    if(column.size<0 || column.size>RowGroupSize || column.globalDictionary ||
       !column.globalIds.empty() || !column.localStringSignatures.empty() || column.globalRowOffset)
        return {};
    uint64_t bytes=0;
    auto add=[&](uint64_t count,uint64_t width,uint64_t overhead=0) {
        if(count && width>(UINT64_MAX-overhead)/count)return false;
        uint64_t n=count*width+overhead;
        if(n>limit-bytes)return false;
        bytes+=n;return true;
    };
    auto values=[&](const uint8_t *data,int width) {
        if(!data)return column.size==0;
        uint64_t n=std::max<uint64_t>(1,uint64_t(column.size)*width);
        return add((n+511)/512,512,64);
    };
    if(column.type==ColumnDataBase::RAW_COLUMN_DATA) {
        const auto *raw=dynamic_cast<const RawColumnDataBase*>(&column);
        if(!raw || (raw->bytesPerValue!=1 && raw->bytesPerValue!=2 &&
                    raw->bytesPerValue!=4 && raw->bytesPerValue!=8))return {};
        size_t object=0;
        switch(logical.type_num()) {
        case INT32_TYPE:
            if(dynamic_cast<const RawColumnData<Int32Type>*>(raw))object=sizeof(RawColumnData<Int32Type>);break;
        case INT64_TYPE:case TIMESTAMP_TYPE:
            if(dynamic_cast<const RawColumnData<Int64Type>*>(raw))object=sizeof(RawColumnData<Int64Type>);break;
        case DECIMAL_TYPE:
            if(dynamic_cast<const RawColumnData<DecimalType>*>(raw))object=sizeof(RawColumnData<DecimalType>);break;
        default:break;
        }
        if(!object || !add(1,object,64) || !values(raw->values,raw->bytesPerValue))return {};
    } else if(column.type==ColumnDataBase::DICT_COLUMN_DATA) {
        auto dictionary=[&]<class T>() {
            auto *dict=dynamic_cast<const DictColumnData<T>*>(&column);
            if(!dict || !dict->valueType || dict->valueType->type_num()!=logical.type_num() ||
               dict->dict.size()>RowGroupSize || (column.size && dict->dict.empty()))return false;
            if(!add(1,sizeof(*dict),128) || !values(dict->values,dict->bytesPerValue()) ||
               !add(dict->dict.capacity(),sizeof(typename T::c_type),64))return false;
            if constexpr(std::is_same_v<T,StringType>)for(const auto &s:dict->dict) {
                // Inline characters are already included in the string object.
                auto p=reinterpret_cast<uintptr_t>(s.data());
                auto begin=reinterpret_cast<uintptr_t>(&s);
                if(p>=begin && p-begin<sizeof(s))continue;
                if(!add(s.capacity(),1,65))return false;
            }
            return true;
        };
        bool ok=logical.type_num()==STRING_TYPE ? dictionary.template operator()<StringType>() :
                logical.type_num()==DATE_TYPE && dictionary.template operator()<DateType>();
        if(!ok)return {};
    } else return {};
    return bytes;
}
}
