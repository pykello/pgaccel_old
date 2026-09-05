#pragma once
#include "parser.h"
#include <set>

namespace pgaccel {
// Apply an externally configured statement budget to every parsed child plan.
// Executors still subtract live parent allocations and accumulated spill before
// each child executes. Intentional per-child overrides can be applied afterward.
inline void SetQueryResourceLimits(QueryDesc &query, uint64_t memoryBytes, uint64_t spillBytes) {
    std::set<QueryDesc *> visited;
    auto configure = [&](auto &&self, QueryDesc &plan) -> void {
        if (!visited.insert(&plan).second) return;
        plan.memoryBudgetBytes = memoryBytes;
        plan.spillByteBudget = spillBytes;
        for (auto &source : plan.materializedSources) self(self, *source.query);
        for (auto &membership : plan.membershipQueries) self(self, *membership.query);
        for (auto &scalar : plan.scalarQueries) self(self, *scalar.query);
    };
    configure(configure, query);
}
}
