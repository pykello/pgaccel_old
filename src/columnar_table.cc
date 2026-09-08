#include "compact_column.hpp"
#include "columnar_table.h"
#include "util.h"
#include "direct_input.hpp"

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
ColumnarTable::Save(const std::string &path, bool aligned)
{
    std::ofstream dataStream(path, std::ios::binary);
    std::ofstream metadataStream(path + ".metadata");
    return Save(metadataStream, dataStream, aligned);
}


Result<bool>
ColumnarTable::Save(std::ostream& metadataStream,
                    std::ostream& dataStream, bool aligned) const
{
    // Pair the two files and declare the native payload ABI explicitly.
    std::random_device random;
    uint64_t identity[2] = {
        (uint64_t(random()) << 32) | random(),
        (uint64_t(random()) << 32) | random()};
    dataStream.write(aligned ? "PGACCEL2" : "PGACCEL1", 8);
    dataStream.write(reinterpret_cast<const char *>(identity), sizeof(identity));
    metadataStream << "PGACCEL_SNAPSHOT " << (aligned ? 2 : 1) << " "
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
            RAISE_IF_FAILS(rowGroup.columns[colIdx]->Save(dataStream, aligned));
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
ColumnarTable::LoadSchema(const std::string &name, const std::string &path) {
    try {
        DirectInput data(path);
        std::ifstream metadata(path + ".metadata");
        return LoadImpl(name, metadata, data, std::nullopt, nullptr, {}, nullptr, true);
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}

Result<DirectScanStats>
ColumnarTable::ScanDirect(const std::string &path, const std::set<std::string> &fields,
                         const ScanConsumer &consumer) {
    if (!consumer || fields.empty()) return Status::Invalid("Direct scan needs columns and a consumer");
    try {
        DirectInput data(path);
        std::ifstream metadata(path + ".metadata");
        DirectScanStats stats;
        auto result = LoadImpl("scan", metadata, data, fields, &path, consumer, &stats);
        if (!result.ok()) return result.status();
        stats.bytesRead += data.bytesRead(); stats.readCalls += data.readCalls();
        stats.ioBufferBytes += 1 << 20;
        return stats;
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &tableName, std::istream &metadata,
                   std::istream &data, std::optional<std::set<std::string>> fields) {
    return LoadImpl(tableName, metadata, data, fields);
}

Result<ColumnarTableP>
ColumnarTable::LoadImpl(const std::string &tableName,
                    std::istream& metadataStream,
                    std::istream& dataStream,
                    std::optional<std::set<std::string>> maybeFields,
                    const std::string *directPath, const ScanConsumer &consumer,
                    DirectScanStats *stats, bool schemaOnly)
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
    bool aligned = false;
    std::string first;
    metadataStream >> first;
    if (first == "PGACCEL_SNAPSHOT") {
        unsigned version = 0, integerBytes = 0, wideBytes = 0, enumBytes = 0, groupSize = 0;
        std::string endian;
        uint64_t expected[2] = {}, actual[2] = {};
        metadataStream >> version >> endian >> integerBytes >> wideBytes >> enumBytes >> groupSize
                       >> expected[0] >> expected[1];
        if (!metadataStream || (version != 1 && version != 2) || integerBytes != sizeof(int) ||
            wideBytes != sizeof(int64_t) || enumBytes != sizeof(ColumnDataBase::Type) ||
            groupSize != RowGroupSize ||
            endian != (std::endian::native == std::endian::little ? "little" : "big"))
            return Status::Invalid("Unsupported snapshot version or native ABI");
        aligned = version == 2;
        char magic[8] = {};
        dataStream.read(magic, sizeof(magic));
        dataStream.read(reinterpret_cast<char *>(actual), sizeof(actual));
        if (!dataStream || std::memcmp(magic, aligned ? "PGACCEL2" : "PGACCEL1", 8) ||
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

    if (schemaOnly) {
        result->schema_only_ = true;
        for (size_t c = 0; c < column_descs.size(); ++c) {
            dataStream.seekg(column_positions[c]);
            if (!dataStream) return Status::Invalid("Invalid native schema column offset");
            auto &desc = column_descs[c];
            desc.layout = desc.type->type_num() == STRING_TYPE || desc.type->type_num() == DATE_TYPE
                ? ColumnDataBase::DICT_COLUMN_DATA : ColumnDataBase::RAW_COLUMN_DATA;
        }
        result->schema_ = std::move(column_descs);
        return result;
    }

    if (directPath) {
        std::vector<std::unique_ptr<DirectInput>> streams;
        for (int c = 0; c < numCols; ++c) {
            if (!fieldsToLoad.count(column_descs[c].name)) continue;
            auto stream = std::make_unique<DirectInput>(*directPath);
            stream->seekg(column_positions[c]);
            if (!*stream) return Status::Invalid("Invalid native column offset");
            auto desc = column_descs[c];
            desc.layout = desc.type->type_num() == STRING_TYPE || desc.type->type_num() == DATE_TYPE
                ? ColumnDataBase::DICT_COLUMN_DATA : ColumnDataBase::RAW_COLUMN_DATA;
            result->schema_.push_back(std::move(desc));
            streams.push_back(std::move(stream));
        }
        if (streams.size() != fieldsToLoad.size()) return Status::Invalid("Unknown direct scan column");
        stats->ioBufferBytes = streams.size() * (1 << 20);
        for (int g = 0; g < column_groups[0]; ++g) {
            RowGroup batch;
            for (size_t c = 0; c < streams.size(); ++c) {
                auto loaded = ColumnDataBase::Load(*streams[c], result->schema_[c].type.get(), aligned);
                if (!loaded.ok()) return loaded.status();
                if (c && batch.size != (*loaded)->size)
                    return Status::Invalid("Inconsistent native column lengths");
                batch.size = (*loaded)->size;
                batch.columns.push_back(std::move(loaded).ValueUnsafe());
            }
            stats->rows += batch.size; ++stats->groups;
            auto accepted = consumer(result->schema_, batch);
            if (!accepted.ok()) return accepted.status();
            if (!*accepted) break;
        }
        for (const auto &stream : streams) {
            stats->bytesRead += stream->bytesRead(); stats->readCalls += stream->readCalls();
        }
        return result;
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
            try { columnData = ColumnDataBase::Load(dataStream,columnDesc.type.get(),aligned); }
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

namespace pgaccel {
Result<ColumnarTableP> ColumnarTable::LoadDirectSelected(const std::string &name,
    const std::string &path, const std::set<std::string> &fields,
    const RowSelector &select, DirectScanStats *io, const ColumnTransform &transform) {
    auto schema = LoadSchema(name, path); if (!schema.ok()) return schema.status();
    auto result = ColumnarTableP(new ColumnarTable()); result->name_ = name;
    std::set<std::string> normalized;
    for (const auto &field:fields) normalized.insert(ToLower(field));
    for (const auto &column:(*schema)->Schema())
        if (normalized.count(column.name)) result->schema_.push_back(column);
    if (result->schema_.size()!=normalized.size()) return Status::Invalid("Unknown selected column");
    auto scan = ScanDirect(path, fields, [&](const auto &columns, const RowGroup &batch)->Result<bool> {
        auto selected = select(columns, batch); if (!selected.ok()) return selected.status();
        const auto &rows = *selected;
        for (size_t i=0; i<rows.size(); ++i)
            if (rows[i]>=batch.size || (i && rows[i]<=rows[i-1]))
                return Status::Invalid("Selection needs ordered unique valid rows");
        if (rows.empty()) return true;
        RowGroup compact; compact.size = rows.size();
        for (size_t c=0; c<columns.size(); ++c) {
            if (rows.size()==size_t(batch.size)) compact.columns.push_back(batch.columns[c]);
            else {
                auto column = CompactNativeColumn(*batch.columns[c], *columns[c].type, rows);
                if (!column.ok()) return column.status();
                compact.columns.push_back(std::move(column).ValueUnsafe());
            }
        }
        if(transform)for(size_t c=0;c<columns.size();++c) {
            auto column=transform(columns[c],compact.columns[c]);if(!column.ok())return column.status();
            if(!*column || (*column)->size!=compact.size)return Status::Invalid("Transform changed row count");
            compact.columns[c]=std::move(column).ValueUnsafe();
        }
        result->row_groups_.push_back(std::move(compact)); return true;
    });
    if (!scan.ok()) return scan.status();
    if (io) *io = *scan;
    return result;
}
}
