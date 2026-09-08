#pragma once
#include "parser.h"
#include <set>

namespace pgaccel {
using RequiredColumnMap = std::map<const ColumnarTable *, std::set<std::string>>;
// Traverse bound references in their own scope. Rebind SQL after projection,
// since physical column indexes in a projected registry differ from schemas.
inline RequiredColumnMap RequiredQueryColumns(const QueryDesc &root) {
    RequiredColumnMap result;
    std::set<const QueryDesc *> visited;
    auto visit = [&](auto &&self, const QueryDesc &q) -> void {
        if (!visited.insert(&q).second) return;
        for (auto table : q.tables) result.try_emplace(table);
        auto column = [&](const ColumnRef &c) {
            if (c.tableIdx < 0) return; // aggregate/scalar virtual output
            if (size_t(c.tableIdx) >= q.tables.size() || c.columnIdx < 0 ||
                c.columnIdx >= q.tables[c.tableIdx]->ColumnCount())
                throw std::invalid_argument("Invalid bound column dependency");
            result[q.tables[c.tableIdx]].insert(q.tables[c.tableIdx]->Schema()[c.columnIdx].name);
        };
        auto expression = [&](auto &&walk, const std::shared_ptr<Expression> &e) -> void {
            if (!e) return;
            if (e->column) column(*e->column);
            walk(walk,e->left); walk(walk,e->right); walk(walk,e->condition);
        };
        for (const auto &f:q.filterClauses)column(f.columnRef);
        for (const auto &c:q.groupBy)column(c);
        for (const auto &e:q.groupExpressions)expression(expression,e);
        for (const auto &e:q.predicates)expression(expression,e);
        for (const auto &a:q.aggregateClauses) {
            if(a.columnRef)column(*a.columnRef);
            expression(expression,a.expression);
        }
        for (const auto &h:q.having)expression(expression,h.bound);
        for (const auto &j:q.leftJoins)for(const auto &e:j.predicates)expression(expression,e);
        for (const auto &j:q.existenceJoins)for(const auto &e:j.predicates)expression(expression,e);
        for (const auto &m:q.membershipQueries){column(m.column);self(self,*m.query);}
        for (const auto &s:q.scalarQueries){
            for(const auto &c:s.outerKeys)column(c);
            self(self,*s.query);
            for(const auto &c:s.innerKeys) {
                if(c.tableIdx>=0 && size_t(c.tableIdx)<s.query->tables.size())
                    result[s.query->tables[c.tableIdx]].insert(c.Name());
            }
        }
        for (const auto &m:q.materializedSources)self(self,*m.query);
    };
    visit(visit,root);
    // COUNT(*) and a table used only for multiplicity still need cardinality.
    // The current COUNT fast route reads the first loaded column's size.
    for(auto &[table,fields]:result)if(fields.empty() && table->ColumnCount())
        fields.insert(table->Schema()[0].name);
    return result;
}
}
