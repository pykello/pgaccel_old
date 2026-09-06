#pragma once
#include "parser.h"

namespace pgaccel {
namespace {
// Restrict a correlated membership output to keys the parent can use.
// Removing the parent's memberships creates a superset of its surviving
// keys; retaining every local predicate and inner join preserves that fact.
std::optional<QueryDesc> MembershipOutputDomain(const QueryDesc &parent,
    const QueryDesc::MembershipQuery &membership, const QueryDesc &child) {
    if (membership.anti || parent.tables.size() != 2 || parent.grouped ||
        !parent.groupBy.empty() || !parent.groupExpressions.empty() || !parent.having.empty() ||
        !parent.scalarQueries.empty() || !parent.leftJoins.empty() ||
        !parent.existenceJoins.empty() || !parent.materializedSources.empty() ||
        child.tables.size() != 1 || child.scalarQueries.size() != 1 ||
        child.membershipQueries.size() != 1 || child.grouped ||
        !child.groupBy.empty() || !child.groupExpressions.empty() || !child.having.empty() ||
        !child.leftJoins.empty() || !child.existenceJoins.empty() ||
        !child.materializedSources.empty() || child.limit || child.offset ||
        child.aggregateClauses.size() != 1) return {};
    const auto &output = child.aggregateClauses[0];
    if (output.type != AggregateClause::AGGREGATE_PROJECT || !output.columnRef ||
        (output.columnRef->Type()->type_num() != INT32_TYPE &&
         output.columnRef->Type()->type_num() != INT64_TYPE) ||
        std::find(child.scalarQueries[0].outerKeys.begin(), child.scalarQueries[0].outerKeys.end(),
            *output.columnRef) == child.scalarQueries[0].outerKeys.end()) return {};
    if (membership.column.tableIdx < 0 || membership.column.tableIdx >= 2 ||
        (membership.column.Type()->type_num() != INT32_TYPE &&
         membership.column.Type()->type_num() != INT64_TYPE)) return {};
    // Bound planning overhead and avoid building unselective outer domains.
    bool filteredSmallTable = false;
    for (int table = 0; table < 2; ++table) {
        uint64_t rows = 0;
        for (int g = 0; g < parent.tables[table]->RowGroupCount() && rows <= 64; ++g)
            rows += parent.tables[table]->GetRowGroup(g).size;
        if (rows > 64) continue;
        for (const auto &filter : parent.filterClauses)
            if (filter.columnRef.tableIdx == table) filteredSmallTable = true;
        for (const auto &e : parent.predicates)
            if (e->kind == Expression::EQ && e->left && e->right && e->left->column &&
                e->left->column->tableIdx == table && e->right->kind == Expression::CONSTANT)
                filteredSmallTable = true;
    }
    if (!filteredSmallTable) return {};
    auto domain = std::make_shared<QueryDesc>(parent);
    domain->membershipQueries.clear(); domain->orderBy.clear(); domain->limit.reset(); domain->offset = 0;
    domain->aggregateClauses.clear(); domain->outputColumnCount = 1;
    AggregateClause projected; projected.type = AggregateClause::AGGREGATE_PROJECT;
    projected.columnRef = membership.column; projected.expression = std::make_shared<Expression>();
    projected.expression->kind = Expression::COLUMN; projected.expression->column = membership.column;
    projected.expression->type = membership.column.Type();
    domain->aggregateClauses.push_back(std::move(projected));
    domain->memoryBudgetBytes = child.memoryBudgetBytes;
    domain->allowSpill = child.allowSpill; domain->spillByteBudget = child.spillByteBudget;
    auto restricted = child;
    restricted.membershipQueries.push_back({domain, *output.columnRef, membership.column.Type(), false});
    return restricted;
}
}
}
