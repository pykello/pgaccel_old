#include "columnar_table.h"
#include "util.h"

#include <execution>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cctype>
#include <string>

namespace pgaccel 
{

std::optional<int>
ColumnarTable::ColumnIndex(const std::string& name) const
{
    auto lcName = ToLower(name);
    for (int i = 0; i < schema_.size(); i++) {
        if (lcName == ToLower(schema_[i].name))
            return i;
    }

    return {};
}

Result<bool>
ColumnarTable::Save(const std::string &path)
{
    std::ofstream dataStream(path);
    std::ofstream metadataStream(path + ".metadata");
    return Save(metadataStream, dataStream);
}


Result<bool>
ColumnarTable::Save(std::ostream& metadataStream,
                    std::ostream& dataStream) const
{
    std::vector<uint64_t> column_positions;

    int numCols = schema_.size();

    uint64_t current_pos = 0;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        column_positions.push_back(dataStream.tellp());

        for (const auto &rowGroup: row_groups_)
        {
            RAISE_IF_FAILS(rowGroup.columns[colIdx]->Save(dataStream));
        }
    }

    metadataStream << numCols << std::endl;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        AccelType *type = schema_[colIdx].type.get();
        metadataStream << column_positions[colIdx];
        metadataStream << " " << row_groups_.size();
        metadataStream << " " << schema_[colIdx].name;
        metadataStream << " " << type->type_num();
        switch (type->type_num())
        {
            case TypeNum::DECIMAL_TYPE:
                auto decimalType = static_cast<DecimalType *>(type);
                metadataStream << " " << decimalType->scale;
                break;
        }

        metadataStream << std::endl;
    }

    return true;
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &tableName,
                    const std::string &path,
                    std::optional<std::set<std::string>> fields)
{
    std::ifstream dataStream(path);
    std::ifstream metadataStream(path + ".metadata");
    return Load(tableName, metadataStream, dataStream, fields);
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &tableName,
                    std::istream& metadataStream,
                    std::istream& dataStream,
                    std::optional<std::set<std::string>> maybeFields)
{
    auto result = std::unique_ptr<ColumnarTable>(new ColumnarTable);
    result->name_ = tableName;

    bool loadAll = false;
    std::set<std::string> fieldsToLoad;
    if (maybeFields.has_value())
    {
        for(auto f: maybeFields.value())
            fieldsToLoad.insert(ToLower(f));
    }
    else
    {
        loadAll = true;
    }

    std::vector<uint64_t> column_positions;
    std::vector<int> column_groups;
    std::vector<ColumnDesc> column_descs;

    int numCols;
    metadataStream >> numCols;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        uint64_t position;
        int groupCount;
        std::string columnName;
        int typeNum;

        metadataStream >> position >> groupCount >> columnName >> typeNum;

        ColumnDesc columnDesc;
        columnDesc.name = ToLower(columnName);

        switch (typeNum)
        {
            case TypeNum::INT32_TYPE:
                columnDesc.type = std::make_unique<Int32Type>();
                break;
            case TypeNum::INT64_TYPE:
                columnDesc.type = std::make_unique<Int64Type>();
                break;
            case TypeNum::STRING_TYPE:
                columnDesc.type = std::make_unique<StringType>();
                break;
            case TypeNum::DATE_TYPE:
                columnDesc.type = std::make_unique<DateType>();
                break;
            case TypeNum::DECIMAL_TYPE:
            {
                auto decimalType = std::make_unique<DecimalType>();
                int scale;
                metadataStream >> scale;
                decimalType->scale = scale;
                columnDesc.type = std::move(decimalType);
                break;
            }
            default:
                return Status::Invalid("Unknown type number: ", typeNum);
        }

        column_positions.push_back(position);
        column_groups.push_back(groupCount);
        column_descs.push_back(std::move(columnDesc));
    }

    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        ColumnDesc &columnDesc = column_descs[colIdx];

        if (!loadAll && !fieldsToLoad.count(columnDesc.name))
        {
            continue;
        }

        uint64_t position = column_positions[colIdx];
        int groupCount = column_groups[colIdx];
        dataStream.seekg(position);

        while(result->row_groups_.size() < groupCount)
            result->row_groups_.push_back({});

        for (int group = 0; group < groupCount; group++)
        {
            auto &rowGroup = result->row_groups_[group];
            auto columnData = ColumnDataBase::Load(dataStream, columnDesc.type.get());
            RAISE_IF_FAILS(columnData);
            rowGroup.columns.push_back(std::move(columnData).ValueUnsafe());
            rowGroup.size = rowGroup.columns.back()->size;
        }

        columnDesc.layout = result->row_groups_[0].columns.back()->type;

        result->schema_.push_back(std::move(column_descs[colIdx]));
    }

    return result;
}

};
