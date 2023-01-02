#include "executor.h"

namespace pgaccel
{

template<class storageType>
int64_t
SumAllRaw(const RawColumnDataBase *columnData,
          const pgaccel::AccelType *type)
{
    int64_t result = 0;
    auto values = reinterpret_cast<const storageType *>(columnData->values);
    for (int i = 0; i < columnData->size; i++) {
        result += values[i];
    }
    return result;
}

int64_t
SumAllRaw(const RawColumnDataBase *columnData,
          const pgaccel::AccelType *type)
{
    switch (columnData->bytesPerValue)
    {
        case 1:
            return SumAllRaw<int8_t>(columnData, type);
        case 2:
            return SumAllRaw<int16_t>(columnData, type);
        case 4:
            return SumAllRaw<int32_t>(columnData, type);
        case 8:
            return SumAllRaw<int64_t>(columnData, type);
    }
    return 0;
}

int64_t
SumAll(const ColumnDataP& columnData,
       const pgaccel::AccelType *type)
{
    switch (columnData->type)
    {
        case ColumnDataBase::RAW_COLUMN_DATA:
        {
            auto rawColumnData = static_cast<RawColumnDataBase *>(columnData.get());
            return SumAllRaw(rawColumnData, type);
        }
        case ColumnDataBase::DICT_COLUMN_DATA:
            // std::cout << "Sum of dict columns not supported yet" << std::endl;
            break;
    }
    return 0;
}

int64_t
SumAll(const std::vector<ColumnDataP>& columnDataVec,
       const pgaccel::AccelType *type)
{
    uint64_t result = 0;
    for (auto &columnData: columnDataVec) {
        result += SumAll(columnData, type);
    }
    return result;
}

};
