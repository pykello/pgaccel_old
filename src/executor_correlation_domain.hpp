#pragma once
#include "executor_state.hpp"
#include "nodes.h"

namespace pgaccel {
namespace {
// Build small, independently filtered key domains for correlated children.
// Required equality edges permit the same domain on matching outer inputs.
std::optional<QueryDesc> CorrelationDomains(const QueryDesc &source, bool avx) {
    if (source.scalarQueries.empty() || !source.leftJoins.empty() ||
        !source.existenceJoins.empty() || !source.materializedSources.empty()) return {};
    std::optional<QueryDesc> result;
    std::vector<ColumnRef> visited;
    for (const auto &scalar : source.scalarQueries) for (const auto &key : scalar.outerKeys) {
        if (key.tableIdx < 0 || size_t(key.tableIdx) >= source.tables.size() ||
            (key.Type()->type_num() != INT32_TYPE && key.Type()->type_num() != INT64_TYPE) ||
            std::find(visited.begin(), visited.end(), key) != visited.end()) continue;
        visited.push_back(key);
        std::vector<FilterClause> filters;
        for (const auto &f : source.filterClauses) if (f.columnRef.tableIdx == key.tableIdx) filters.push_back(f);
        // General WHERE parsing retains these conjuncts as expressions.
        // Lower only direct, exactly represented bounds for domain discovery.
        for (const auto &e : source.predicates) {
            if (!e->left || !e->right || !e->left->column ||
                e->left->column->tableIdx != key.tableIdx || e->right->kind != Expression::CONSTANT) continue;
            auto type = e->left->type->type_num(), bound = e->right->type->type_num();
            bool string = type == STRING_TYPE && bound == STRING_TYPE;
            bool integer = (type == INT32_TYPE || type == INT64_TYPE) && bound == INT64_TYPE;
            if (!string && !integer) continue;
            FilterClause f; f.columnRef = *e->left->column; f.value = e->right->literal;
            if (integer) {
                try { size_t end; std::stoll(f.value, &end); if (end != f.value.size()) continue; }
                catch (const std::exception &) { continue; }
            }
            switch (e->kind) {
            case Expression::EQ: f.op = FilterClause::FILTER_EQ; break;
            case Expression::NE: f.op = FilterClause::FILTER_NE; break;
            case Expression::LT: f.op = FilterClause::FILTER_LT; break;
            case Expression::LTE: f.op = FilterClause::FILTER_LTE; break;
            case Expression::GT: f.op = FilterClause::FILTER_GT; break;
            case Expression::GTE: f.op = FilterClause::FILTER_GTE; break;
            case Expression::LIKE: if (!string) continue; f.op = FilterClause::FILTER_LIKE; break;
            default: continue;
            }
            filters.push_back(std::move(f));
        }
        if (filters.empty()) continue;
        auto filter = CreateFilterNode(filters, avx);
        uint64_t count = 0, total = 0;
        auto table = source.tables[key.tableIdx];
        for (int g = 0; g < table->RowGroupCount(); ++g) {
            const auto &rg = table->GetRowGroup(g);
            total += rg.size; count += filter->ExecuteCount(rg);
            if (count > 1000000) break;
        }
        if (count > 1000000 || (count && count > total / 8)) continue;
        auto domain = std::make_shared<QueryDesc>();
        domain->tables = {table}; domain->filterClauses = std::move(filters);
        for (auto &f : domain->filterClauses) f.columnRef.tableIdx = 0;
        auto projected = key; projected.tableIdx = 0;
        AggregateClause output; output.type = AggregateClause::AGGREGATE_PROJECT;
        output.columnRef = projected; output.expression = std::make_shared<Expression>();
        output.expression->kind = Expression::COLUMN; output.expression->column = projected;
        output.expression->type = projected.Type();
        domain->aggregateClauses.push_back(std::move(output)); domain->outputColumnCount = 1;
        domain->memoryBudgetBytes = source.memoryBudgetBytes;
        domain->allowSpill = source.allowSpill; domain->spillByteBudget = source.spillByteBudget;
        std::vector<ColumnRef> equivalent{key};
        for (size_t i = 0; i < equivalent.size(); ++i) {
            for (const auto &e : source.predicates) {
                if (e->kind != Expression::EQ || !e->left->column || !e->right->column) continue;
                auto left = *e->left->column, right = *e->right->column;
                if (left != equivalent[i]) std::swap(left, right);
                if (left != equivalent[i] || right.tableIdx < 0 || size_t(right.tableIdx) >= source.tables.size()) continue;
                if (std::find(equivalent.begin(), equivalent.end(), right) == equivalent.end()) equivalent.push_back(right);
            }
        }
        if (!result) result = source;
        for (const auto &column : equivalent)
            result->membershipQueries.push_back({domain, column, projected.Type(), false});
    }
    return result;
}
}
}
