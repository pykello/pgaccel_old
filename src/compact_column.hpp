#pragma once
#include "column_data.hpp"
namespace pgaccel {
// Copy a strictly increasing selection into owned native buffers. Recompute
// bounds and prune dictionaries; auxiliary row/dictionary indexes are omitted.
Result<ColumnDataP> CompactNativeColumn(const ColumnDataBase &source,
    const AccelType &logical, const std::vector<uint16_t> &rows);
}
