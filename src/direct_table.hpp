#pragma once
#include "columnar_table.h"
#include "direct_input.hpp"
#include <fstream>

namespace pgaccel {
// Load only explicitly requested native columns, retaining normal owned SIMD
// buffers. Payload reads bypass the file cache; this is not an eviction cache.
inline Result<ColumnarTableP> LoadDirectTable(const std::string &name,
    const std::string &path, const std::set<std::string> &fields,
    DirectScanStats *io = nullptr) {
    if (fields.empty()) return Status::Invalid("Direct resident load needs explicit columns");
    try {
        DirectInput data(path);
        std::ifstream metadata(path + ".metadata");
        auto result = ColumnarTable::Load(name, metadata, data, fields);
        if (!result.ok()) return result.status();
        std::set<std::string> requested;
        for (const auto &field : fields) {
            auto index = (*result)->ColumnIndex(field);
            if (!index) return Status::Invalid("Unknown direct resident column: ", field);
            requested.insert((*result)->Schema()[*index].name);
        }
        if (io) {
            *io = {};
            io->bytesRead = data.bytesRead(); io->readCalls = data.readCalls();
            io->ioBufferBytes = 1 << 20; io->groups = (*result)->RowGroupCount();
            for (int g=0;g<(*result)->RowGroupCount();++g)io->rows+=(*result)->GetRowGroup(g).size;
        }
        return std::move(result).ValueUnsafe();
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
