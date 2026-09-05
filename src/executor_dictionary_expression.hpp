#pragma once
#include "executor_state.hpp"

namespace pgaccel {
namespace {
// Find one dictionary column that determines the complete expression. Lazy
// branches remain lazy because evaluation still uses the normal evaluator.
std::optional<ColumnRef> DictionaryExpressionSource(const Expression &expression) {
    std::optional<ColumnRef> source;
    auto visit = [&](auto &&self, const Expression &node) -> bool {
        if (node.kind == Expression::COLUMN) {
            if (source && *source != *node.column) return false;
            source = node.column;
        }
        for (const auto &child : {node.left, node.right, node.condition})
            if (child && !self(self, *child)) return false;
        return true;
    };
    if (!visit(visit, expression) || !source || expression.kind == Expression::COLUMN)
        return {};
    auto type = source->Type()->type_num();
    if (type != STRING_TYPE && type != DATE_TYPE) return {};
    return source;
}

class DictionaryExpression {
    const QueryDesc &query;
    ColumnRef source;
    Budget &budget;
    std::pmr::vector<std::pmr::vector<Cell>> stripes;
  public:
    const std::shared_ptr<Expression> expression;
    DictionaryExpression(const QueryDesc &q, std::shared_ptr<Expression> e,
                         ColumnRef column, Budget &memory)
        : query(q), source(std::move(column)), budget(memory), stripes(&memory),
          expression(std::move(e)) {
        stripes.resize(q.tables[0]->RowGroupCount());
    }
    void prepare(int group, const uint8_t *selection) {
        const auto &rg = query.tables[0]->GetRowGroup(group);
        auto data = rg.columns[source.columnIdx].get();
        if (data->type != ColumnDataBase::DICT_COLUMN_DATA) return;
        auto dictionary = static_cast<const DictColumnDataBase *>(data);
        auto &values = stripes[group];
        values.resize(dictionary->dictSize());
        std::pmr::vector<int> representative(dictionary->dictSize(), -1, &budget);
        Selected(rg, selection, [&](int row) {
            int code = Code(dictionary, row);
            if (representative[code] < 0) representative[code] = row;
        });
        // Do not evaluate unused labels: they may contain errors hidden by WHERE.
        for (int code = 0; code < dictionary->dictSize(); ++code)
            if (representative[code] >= 0)
                values[code] = Evaluate(*expression, rg, representative[code]);
    }
    Cell value(int group, int row) const {
        const auto &rg = query.tables[0]->GetRowGroup(group);
        if (stripes[group].empty()) return Evaluate(*expression, rg, row);
        auto dictionary = static_cast<const DictColumnDataBase *>(rg.columns[source.columnIdx].get());
        return stripes[group][Code(dictionary, row)];
    }
};
} // namespace
} // namespace pgaccel
