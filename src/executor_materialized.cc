#include "executor.h"
#include "executor_state.hpp"

namespace pgaccel {
Result<QueryOutput> ExecuteMaterialized(const QueryDesc &query, bool avx, bool parallel) {
    try {
        Budget budget(query.memoryBudgetBytes);
        QueryDesc outer = query;
        outer.materializedSources.clear();
        std::vector<ColumnarTableP> tables;
        uint64_t spilled = 0;
        for (const auto &source : query.materializedSources) {
            auto child = *source.query;
            child.memoryBudgetBytes = std::min(child.memoryBudgetBytes, budget.limit - budget.used.load());
            child.allowSpill &= query.allowSpill;
            child.spillByteBudget = std::min(child.spillByteBudget, query.spillByteBudget - spilled);
            auto result = ExecuteQuery(child, avx, parallel);
            if (!result.ok()) return Status::Invalid("Materialized source: " + result.status().Message());
            spilled += result->spillBytes;
            if (spilled > query.spillByteBudget) throw std::runtime_error("Subquery spill budget exceeded");
            { Scratch priorPeak(budget, result->peakMemoryBytes); }
            uint64_t outputBytes = sizeof(QueryOutput) + 128 + result->values.capacity() * sizeof(Row);
            for (const auto &row : result->values) {
                outputBytes += row.capacity() * sizeof(std::string) + 64;
                for (const auto &value : row) outputBytes += value.capacity() + 17;
            }
            outputBytes += result->fieldNames.capacity() * sizeof(std::string) + 64;
            for (const auto &name : result->fieldNames) outputBytes += name.capacity() + 17;
            Scratch retainedOutput(budget, outputBytes);
            // Precharge aligned buffers, column objects, vector growth, schema,
            // and allocation overhead before constructing a native intermediate.
            uint64_t columns = source.schema->Schema().size();
            uint64_t groups = (result->values.size() + RowGroupSize - 1) / RowGroupSize;
            uint64_t bytes = 4096 + columns * 1024 + groups * 512 +
                columns * (uint64_t(result->values.size()) * 8 + groups * 1024);
            budget.charge(bytes);
            auto table = ColumnarTable::NumericIntermediate(source.schema->Name(), source.schema->Schema(), result->values);
            outer.tables.at(source.table) = table.get();
            tables.push_back(std::move(table));
        }
        outer.memoryBudgetBytes = budget.limit - budget.used.load();
        outer.spillByteBudget = query.spillByteBudget - spilled;
        auto result = ExecuteQuery(outer, avx, parallel);
        if (!result.ok()) return result;
        auto output = std::move(result).ValueUnsafe();
        { Scratch execution(budget, output.peakMemoryBytes); }
        output.peakMemoryBytes = budget.peak.load();
        output.spillBytes += spilled;
        return output;
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
