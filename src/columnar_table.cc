#include "columnar_table.h"
#include "util.h"

#include <execution>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cctype>
#include <string>
#include <bit>
#include <random>
#include <cstring>

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
    std::ofstream dataStream(path, std::ios::binary);
    std::ofstream metadataStream(path + ".metadata");
    return Save(metadataStream, dataStream);
}


Result<bool>
ColumnarTable::Save(std::ostream& metadataStream,
                    std::ostream& dataStream) const
{
    // Pair the two files and declare the native payload ABI explicitly.
    std::random_device random;
    uint64_t identity[2] = {
        (uint64_t(random()) << 32) | random(),
        (uint64_t(random()) << 32) | random()};
    dataStream.write("PGACCEL1", 8);
    dataStream.write(reinterpret_cast<const char *>(identity), sizeof(identity));
    metadataStream << "PGACCEL_SNAPSHOT 1 "
                   << (std::endian::native == std::endian::little ? "little" : "big")
                   << " " << sizeof(int) << " " << sizeof(int64_t)
                   << " " << sizeof(ColumnDataBase::Type) << " " << RowGroupSize
                   << " " << identity[0] << " " << identity[1] << "\n";

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

    if (!metadataStream || !dataStream) return Status::Invalid("Native save failed");
    return true;
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &tableName,
                    const std::string &path,
                    std::optional<std::set<std::string>> fields)
{
    std::ifstream dataStream(path, std::ios::binary);
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

    int numCols = 0;
    std::string first;
    metadataStream >> first;
    if (first == "PGACCEL_SNAPSHOT") {
        unsigned version = 0, integerBytes = 0, wideBytes = 0, enumBytes = 0, groupSize = 0;
        std::string endian;
        uint64_t expected[2] = {}, actual[2] = {};
        metadataStream >> version >> endian >> integerBytes >> wideBytes >> enumBytes >> groupSize
                       >> expected[0] >> expected[1];
        if (!metadataStream || version != 1 || integerBytes != sizeof(int) ||
            wideBytes != sizeof(int64_t) || enumBytes != sizeof(ColumnDataBase::Type) ||
            groupSize != RowGroupSize ||
            endian != (std::endian::native == std::endian::little ? "little" : "big"))
            return Status::Invalid("Unsupported snapshot version or native ABI");
        char magic[8] = {};
        dataStream.read(magic, sizeof(magic));
        dataStream.read(reinterpret_cast<char *>(actual), sizeof(actual));
        if (!dataStream || std::memcmp(magic, "PGACCEL1", 8) ||
            actual[0] != expected[0] || actual[1] != expected[1])
            return Status::Invalid("Snapshot data and metadata do not match");
        metadataStream >> numCols;
    } else {
        // Compatibility reader for snapshots created before versioning.
        std::istringstream legacy(first);
        if (!(legacy >> numCols) || !legacy.eof()) return Status::Invalid("Invalid native schema");
    }
    if (!metadataStream || numCols <= 0 || numCols > 16384) return Status::Invalid("Invalid native schema");
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        uint64_t position;
        int groupCount;
        std::string columnName;
        int typeNum;

        metadataStream >> position >> groupCount >> columnName >> typeNum;
        if (!metadataStream || groupCount < 0 || groupCount > 1000000 ||
            (!column_groups.empty() && groupCount != column_groups[0]))
            return Status::Invalid("Invalid native group count");

        ColumnDesc columnDesc{};
        columnDesc.name = ToLower(columnName);

        switch (typeNum)
        {
            case TypeNum::INT32_TYPE:
                columnDesc.type = std::make_shared<Int32Type>();
                break;
            case TypeNum::TIMESTAMP_TYPE:
                columnDesc.type = std::make_shared<TimestampType>();
                break;
            case TypeNum::INT64_TYPE:
                columnDesc.type = std::make_shared<Int64Type>();
                break;
            case TypeNum::STRING_TYPE:
                columnDesc.type = std::make_shared<StringType>();
                break;
            case TypeNum::DATE_TYPE:
                columnDesc.type = std::make_shared<DateType>();
                break;
            case TypeNum::DECIMAL_TYPE:
            {
                auto decimalType = std::make_shared<DecimalType>();
                int scale;
                metadataStream >> scale;
                if (!metadataStream || scale < 0 || scale > 18) return Status::Invalid("Invalid decimal scale");
                decimalType->scale = scale;
                columnDesc.type = decimalType;
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
            Result<ColumnDataP> columnData(Status::Invalid("Invalid native column"));
            try { columnData = ColumnDataBase::Load(dataStream,columnDesc.type.get()); }
            catch (const std::exception &e) { return Status::Invalid(e.what()); }
            RAISE_IF_FAILS(columnData);
            if (!rowGroup.columns.empty() && rowGroup.size != (*columnData)->size)
                return Status::Invalid("Inconsistent native column lengths");
            rowGroup.columns.push_back(std::move(columnData).ValueUnsafe());
            rowGroup.size = rowGroup.columns.back()->size;
        }

        columnDesc.layout = groupCount ? result->row_groups_[0].columns.back()->type :
            (columnDesc.type->type_num() == STRING_TYPE || columnDesc.type->type_num() == DATE_TYPE ?
             ColumnDataBase::DICT_COLUMN_DATA : ColumnDataBase::RAW_COLUMN_DATA);

        result->schema_.push_back(std::move(column_descs[colIdx]));
    }

    if (result->schema_.empty()) return Status::Invalid("No matching native columns");
    return result;
}

};
