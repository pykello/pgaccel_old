#include "executor_result.hpp"
#include "executor_group_key.hpp"
#include <filesystem>
#include <fstream>
#include <queue>
#include <unistd.h>

namespace pgaccel
{
namespace
{
struct RowPosition
{
    uint32_t group, row;
};
struct SpillFiles
{
    std::filesystem::path directory;
    uint64_t written = 0, maximum, sequence = 0;
    SpillFiles(uint64_t maximum) : maximum(maximum)
    {
        auto name = (std::filesystem::temp_directory_path() / "pgaccel-spill-XXXXXX").string();
        if (!mkdtemp(name.data()))
            throw std::runtime_error("Cannot create spill directory");
        directory = name;
    }
    ~SpillFiles()
    {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    std::filesystem::path next() { return directory / std::to_string(sequence++); }
    void write(std::ostream &out, RowPosition row)
    {
        if (written > maximum || sizeof(row) > maximum - written)
            throw std::runtime_error("Query spill byte budget exceeded");
        out.write(reinterpret_cast<const char *>(&row), sizeof(row));
        if (!out)
            throw std::runtime_error("Spill write failed");
        written += sizeof(row);
    }
};
bool ReadPosition(std::istream &in, RowPosition &row)
{
    in.read(reinterpret_cast<char *>(&row), sizeof(row));
    if (!in.gcount() && in.eof())
        return false;
    if (in.gcount() != sizeof(row))
        throw std::runtime_error("Truncated spill run");
    return true;
}
} // namespace
Result<QueryOutput> ExecuteExternal(const QueryDesc &q, bool avx)
{
    try
    {
        if (q.memoryBudgetBytes < (1 << 20))
            return Status::Invalid("Query memory budget must be at least 1 MiB for spill");
        int distinctIndex = -1;
        bool aggregate = q.grouped;
        for (size_t i = 0; i < q.aggregateClauses.size(); ++i)
        {
            auto kind = q.aggregateClauses[i].type;
            aggregate |= kind != AggregateClause::AGGREGATE_PROJECT;
            if (kind == AggregateClause::AGGREGATE_COUNT_DISTINCT)
            {
                if (distinctIndex >= 0)
                    return Status::Invalid("Spill supports one exact DISTINCT argument per query");
                distinctIndex = i;
            }
        }
        if (!aggregate)
            return Status::Invalid("Row projection output exceeds query memory budget");
        Budget budget(q.memoryBudgetBytes);
    ScopedValueResource valuesMemory(budget);
        Scratch fixed(budget, 1 << 18); // bounded stream buffers, merge cursors, and run paths
        SpillFiles files(q.spillByteBudget);
        auto &table = *q.tables[0];
        GroupKeys keys{q};
        auto key = [&](RowPosition row) { return GroupKey{0,row.group,row.row}; };
        auto distinct = [&](RowPosition row)
        {
            return Evaluate(*q.aggregateClauses[distinctIndex].expression,
                            table.GetRowGroup(row.group), row.row);
        };
        auto less = [&](RowPosition a, RowPosition b)
        {
            auto left = key(a), right = key(b);
            if (!keys.equal(left,right))
                return keys.less(left,right);
            return distinctIndex >= 0 && Less(distinct(a), distinct(b));
        };
        constexpr size_t fanIn = 16;
        auto merge = [&](const std::vector<std::filesystem::path> &runs)
        {
            auto output = files.next();
            std::ofstream out(output, std::ios::binary);
            std::vector<std::ifstream> inputs;
            for (const auto &run : runs)
                inputs.emplace_back(run, std::ios::binary);
            using Entry = std::pair<RowPosition, size_t>;
            auto greater = [&](const Entry &a, const Entry &b) { return less(b.first, a.first); };
            std::priority_queue<Entry, std::vector<Entry>, decltype(greater)> heap(greater);
            for (size_t i = 0; i < inputs.size(); ++i)
            {
                RowPosition row;
                if (ReadPosition(inputs[i], row))
                    heap.push({row, i});
            }
            while (!heap.empty())
            {
                auto [row, input] = heap.top();
                heap.pop();
                files.write(out, row);
                if (ReadPosition(inputs[input], row))
                    heap.push({row, input});
            }
            out.close();
            if (!out)
                throw std::runtime_error("Spill close failed");
            inputs.clear();
            for (const auto &run : runs)
                std::filesystem::remove(run);
            return output;
        };
        // Leveled compaction keeps the number of open runs and path objects bounded.
        std::array<std::vector<std::filesystem::path>, 64> levels;
        std::function<void(std::filesystem::path, size_t)> addRun;
        addRun = [&](std::filesystem::path path, size_t level)
        {
            if (level == levels.size())
                throw std::runtime_error("Too many spill levels");
            auto &runs = levels[level];
            runs.push_back(std::move(path));
            if (runs.size() == fanIn)
            {
                auto combined = merge(runs);
                runs.clear();
                addRun(combined, level + 1);
            }
        };
        auto filter =
            q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
        {
            std::pmr::vector<RowPosition> chunk(&budget);
            size_t capacity =
                std::min<uint64_t>(q.memoryBudgetBytes / 2 / sizeof(RowPosition), 1 << 24);
            chunk.reserve(capacity);
            auto flush = [&]
            {
                if (chunk.empty())
                    return;
                std::sort(chunk.begin(), chunk.end(), less);
                auto run = files.next();
                std::ofstream out(run, std::ios::binary);
                for (auto row : chunk)
                    files.write(out, row);
                out.close();
                if (!out)
                    throw std::runtime_error("Spill close failed");
                chunk.clear();
                addRun(run, 0);
            };
            for (int g = 0; g < table.RowGroupCount(); ++g)
            {
                const auto &rg = table.GetRowGroup(g);
                uint8_t bitmap[RowGroupSize / 8];
                int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
                if (!count)
                    continue;
                Selected(rg, count == rg.size ? nullptr : bitmap,
                         [&](int row)
                         {
                             chunk.push_back({uint32_t(g), uint32_t(row)});
                             if (chunk.size() == capacity)
                                 flush();
                         });
            }
            flush();
        }
        std::vector<std::filesystem::path> remaining;
        for (auto &level : levels)
            for (auto &path : level)
            {
                remaining.push_back(path);
                if (remaining.size() == fanIn)
                {
                    auto combined = merge(remaining);
                    remaining = {combined};
                }
            }
        std::filesystem::path finalRun;
        if (!remaining.empty())
            finalRun = remaining.size() == 1 ? remaining[0] : merge(remaining);
        QueryResults result(q, budget);
        States state(q.aggregateClauses.size(), &budget);
        bool haveGroup = false, haveDistinct = false;
        GroupKey groupKey{};
        Cell lastDistinct;
        auto finish = [&]
        {
            FinalRow row;
            for (size_t i = 0; i < q.aggregateClauses.size(); ++i)
            {
                const auto &a = q.aggregateClauses[i];
                const auto &s = state[i];
                FinalCell cell;
                cell.type = a.expression ? a.expression->type.get() : nullptr;
                switch (a.type)
                {
                case AggregateClause::AGGREGATE_PROJECT:
                    cell.value = a.expression->kind==Expression::CONSTANT
                        ? Evaluate(*a.expression,RowGroup{},0)
                        : Evaluate(*a.expression,table.GetRowGroup(groupKey.group),groupKey.row);
                    break;
                case AggregateClause::AGGREGATE_COUNT:
                case AggregateClause::AGGREGATE_COUNT_DISTINCT:
                    cell.type = nullptr;
                    cell.value.number = s.count;
                    break;
                case AggregateClause::AGGREGATE_SUM:
                    cell.value.number = s.sum;
                    cell.null = !s.count;
                    break;
                case AggregateClause::AGGREGATE_AVG:
                    cell.average = true;
                    cell.null = !s.count;
                    if (s.count)
                        cell.mean =
                            (long double)s.sum / s.count / (long double)Power(Scale(cell.type));
                    break;
                default:
                    cell.value = s.extreme;
                    cell.null = !s.count;
                    break;
                }
                row.push_back(cell);
            }
            result.add(std::move(row));
        };
        if (!finalRun.empty())
        {
            std::ifstream input(finalRun, std::ios::binary);
            RowPosition row;
            while (ReadPosition(input, row))
            {
                auto currentKey = key(row);
                if (!haveGroup || !keys.equal(currentKey,groupKey))
                {
                    if (haveGroup)
                        finish();
                    for (auto &s : state)
                        s = State{};
                    groupKey = currentKey;
                    haveGroup = true;
                    haveDistinct = false;
                }
                for (size_t i = 0; i < state.size(); ++i)
                {
                    if (int(i) == distinctIndex)
                    {
                        auto value = distinct(row);
                        if (!haveDistinct || !(value == lastDistinct))
                            ++state[i].count;
                        lastDistinct = value;
                        haveDistinct = true;
                    }
                    else
                        Update(state[i], q.aggregateClauses[i], table.GetRowGroup(row.group),
                               row.row, budget);
                }
            }
        }
        if (haveGroup || (!q.grouped && q.groupBy.empty()))
            finish();
        auto output = result.finish();
        output.spillBytes = files.written;
        return output;
    }
    catch (const std::exception &e)
    {
        return Status::Invalid(e.what());
    }
}
} // namespace pgaccel
