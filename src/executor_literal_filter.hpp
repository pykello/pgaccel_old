#pragma once
#include "parser.h"

namespace pgaccel {
namespace {
// Lower only literal comparisons whose native filter representation is exact.
// Decimal rescaling, computed operands and other expression shapes stay with
// the general predicate evaluator.
std::optional<FilterClause> ExactLiteralFilter(const Expression &e) {
    if (!e.left || !e.right || !e.left->column || e.right->kind!=Expression::CONSTANT) return {};
    auto type=e.left->type->type_num(),bound=e.right->type->type_num();
    bool string=type==STRING_TYPE && bound==STRING_TYPE;
    bool integer=(type==INT32_TYPE || type==INT64_TYPE) && bound==INT64_TYPE;
    if (!string && !integer) return {};
    FilterClause f; f.columnRef=*e.left->column; f.value=e.right->literal;
    if (integer) {
        try { size_t end; std::stoll(f.value,&end); if (end!=f.value.size()) return {}; }
        catch (const std::exception &) { return {}; }
    }
    switch (e.kind) {
    case Expression::EQ: f.op=FilterClause::FILTER_EQ; break;
    case Expression::NE: f.op=FilterClause::FILTER_NE; break;
    case Expression::LT: f.op=FilterClause::FILTER_LT; break;
    case Expression::LTE: f.op=FilterClause::FILTER_LTE; break;
    case Expression::GT: f.op=FilterClause::FILTER_GT; break;
    case Expression::GTE: f.op=FilterClause::FILTER_GTE; break;
    case Expression::LIKE: if (!string) return {}; f.op=FilterClause::FILTER_LIKE; break;
    default: return {};
    }
    return f;
}
}
}
