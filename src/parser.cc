#include "parser.h"
#include "util.h"

#include <vector>
#include <cstdlib>

namespace pgaccel
{

#define ENSURE_TOKEN(type) \
    do { \
        if (currentIdx == tokens.size()) \
            return Status::Invalid("Unexpected end of query while expecting a ", type); \
    } while(0);

struct UnresolvedAggregate
{
    AggregateClause::Type type;
    std::optional<std::string> col;
};

typedef std::vector<UnresolvedAggregate> UnresolvedAggV;

static std::vector<std::string> TokenizeQuery(const std::string &query);
static Result<bool> ParseToken(const std::string &kw,
                                 const std::vector<std::string> &tokens,
                                 int &currentIdx);
static Result<UnresolvedAggV> ParseAggregates(
    QueryDesc &queryDesc,
    const std::vector<std::string> &tokens,
    int &currentIdx);
static Result<bool> ParseTableRef(QueryDesc &queryDesc,
                                  const TableRegistry &registry,
                                  const std::vector<std::string> &tokens,
                                  int &currentIdx);
static Result<bool> ParseFilters(QueryDesc &queryDesc,
                                 const std::vector<std::string> &tokens,
                                 int &currentIdx);
static Result<bool> ParseFiltersDisj(QueryDesc &queryDesc,
                                    const std::vector<std::string> &tokens,
                                    int &currentIdx);
static Result<bool> ParseFiltersConj(QueryDesc &queryDesc,
                                     const std::vector<std::string> &tokens,
                                     int &currentIdx);
static Result<FilterClause> ParseFilterAtom(QueryDesc &queryDesc,
                                            const std::vector<std::string> &tokens,
                                            int &currentIdx);
static Result<bool> ParseGroupBy(QueryDesc &queryDesc,
                                 const std::vector<std::string> &tokens,
                                 int &currentIdx);
static Result<ColumnRef> ParseColumnRef(QueryDesc &queryDesc,
                                        const std::vector<std::string> &tokens,
                                        int &currentIdx);
static Result<ColumnRef> ResolveColumn(QueryDesc &queryDesc,
                                       const std::string &columnName);
static Result<std::string> ParseValue(const AccelType &type,
                                      const std::vector<std::string> &tokens,
                                      int &currentIdx);
static Result<bool> ResolveAggregates(QueryDesc &queryDesc,
                                      const UnresolvedAggV &unresolvedAggs);

Result<QueryDesc>
ParseSelect(const std::string &query, const TableRegistry &registry)
{
    QueryDesc queryDesc;
    auto tokens = TokenizeQuery(query);
    int idx = 0;
    RAISE_IF_FAILS(ParseToken("SELECT", tokens, idx));
    UnresolvedAggV unresolvedAggs;
    ASSIGN_OR_RAISE(unresolvedAggs, ParseAggregates(queryDesc, tokens, idx));
    RAISE_IF_FAILS(ParseToken("FROM", tokens, idx));
    RAISE_IF_FAILS(ParseTableRef(queryDesc, registry, tokens, idx));

    if (ParseToken("WHERE", tokens, idx).ok())
    {
        RAISE_IF_FAILS(ParseFilters(queryDesc, tokens, idx));
    }

    if (ParseToken("GROUP", tokens, idx).ok())
    {
        RAISE_IF_FAILS(ParseToken("BY", tokens, idx));
        RAISE_IF_FAILS(ParseGroupBy(queryDesc, tokens, idx));
    }

    if (idx != tokens.size())
    {
        return Status::Invalid("Unexpected token:", tokens[idx], ".");
    }

    RAISE_IF_FAILS(ResolveAggregates(queryDesc, unresolvedAggs));

    return queryDesc;
}

std::string ColumnRef::ToString() const
{
    std::ostringstream sout;
    sout << "(table=" << tableIdx << ",col=" << columnIdx << ",type=" << type->ToString() << ")";
    return sout.str();
}

std::string FilterClause::ToString() const
{
    std::ostringstream sout;
    sout << "(op='";
    switch (op)
    {
        case FilterClause::FILTER_EQ:
            sout << "=";
            break;
        case FilterClause::FILTER_NE:
            sout << "!=";
            break;
        case FilterClause::FILTER_LT:
            sout << "<";
            break;
        case FilterClause::FILTER_LTE:
            sout << "<=";
            break;
        case FilterClause::FILTER_GT:
            sout << ">";
            break;
        case FilterClause::FILTER_GTE:
            sout << ">=";
            break;
    }
    sout << "',columnRef=" << columnRef.ToString();
    sout << ",value='" << value << "'";
    sout << ")";
    return sout.str();
}

std::string AggregateClause::ToString() const
{
    std::ostringstream sout;
    switch(type)
    {
        case AggregateClause::AGGREGATE_COUNT:
            sout << "count";
            break;
        case AggregateClause::AGGREGATE_COUNT_DISTINCT:
            sout << "count-distinct";
            break;
        case AggregateClause::AGGREGATE_MAX:
            sout << "max";
            break;
        case AggregateClause::AGGREGATE_MIN:
            sout << "min";
            break;
        case AggregateClause::AGGREGATE_SUM:
            sout << "sum";
            break;
        case AggregateClause::AGGREGATE_AVG:
            sout << "avg";
            break;
        case AggregateClause::AGGREGATE_PROJECT:
            sout << "project";
            break;
    }
    if (columnRef.has_value())
    {
        sout << " " << columnRef->Name();
    }
    return sout.str();
}

std::string QueryDesc::ToString() const
{
    std::ostringstream sout;
    sout << "Tables: " << std::endl;
    for(auto table: tables) {
        sout << "  - " << table->Name() << std::endl;
    }

    sout << "Filter Clauses:" << std::endl;
    for(auto filterClause: filterClauses)
    {
        sout << "  - " << filterClause.ToString() << std::endl;
    }

    sout << "Group By:" << std::endl;
    for(auto col: groupBy)
    {
        sout << "  - " << col.ToString() << std::endl;
    }

    sout << "Aggregate Clauses:" << std::endl;
    for (auto agg: aggregateClauses)
    {
        sout << "  - " << agg.ToString() << std::endl;
    }

    return sout.str();
}

static bool
OperatorChar(char ch)
{
    return ch == '*' || ch == '/' || ch == '-' || ch == '+' || ch == '|' ||
           ch == '>' || ch == '<' || ch == '=' || ch == '!';
}

static std::vector<std::string>
TokenizeQuery(const std::string &query)
{
    std::vector<std::string> tokens;
    std::string current;

    bool inString = false;

    for (auto c: query) {
        if (inString) {
            if (c == '\'') {
                tokens.push_back(current);
                tokens.push_back("'");
                current = "";
                inString = false;
            }
            else {
                current += c;
            }
        }
        else if (isspace(c) || c == ';') {
            if (current.length())
                tokens.push_back(current);
            current = "";
        }
        else if (c == '(' || c == ')' || c == ',' || c == '\'') {
            if (current.length())
                tokens.push_back(current);
            current = "";
            std::string token;
            token += c;
            tokens.push_back(token);
            inString = (c == '\'');
        }
        else if (OperatorChar(c))
        {
            if (current.length() && !OperatorChar(current.back()))
            {
                tokens.push_back(current);
                current = "";
            }

            current += c;
        }
        else {
            if (current.length() && OperatorChar(current.back()))
            {
                tokens.push_back(current);
                current = "";
            }

            current += c;
        }
    }

    return tokens;
}

static Result<bool>
ParseToken(const std::string &kw,
           const std::vector<std::string> &tokens,
           int &currentIdx)
{
    ENSURE_TOKEN(kw);

    if (ToLower(tokens[currentIdx]) != ToLower(kw))
        return Status::Invalid("Expected '", kw, "', but found: ", tokens[currentIdx]);
    currentIdx++;

    return true;
}

static Result<std::vector<UnresolvedAggregate>>
ParseAggregates(QueryDesc &queryDesc,
                const std::vector<std::string> &tokens,
                int &currentIdx)
{
    std::vector<UnresolvedAggregate> result;

    while (true)
    {
        if (ParseToken("count", tokens, currentIdx).ok())
        {
            RAISE_IF_FAILS(ParseToken("(", tokens, currentIdx));
            if(ParseToken("*", tokens, currentIdx).ok())
            {
                UnresolvedAggregate agg;
                agg.type = AggregateClause::AGGREGATE_COUNT;
                result.push_back(agg);
            }
            else
            {
                // TODO: count(distinct col)
            }
            RAISE_IF_FAILS(ParseToken(")", tokens, currentIdx));
        }
        else
        {
            std::vector<std::pair<std::string, AggregateClause::Type>> supportedAggs =
                { 
                    {"sum", AggregateClause::AGGREGATE_SUM}
                };
            bool parsed = false;

            for (auto agg: supportedAggs)
            {
                auto aggName = agg.first;
                auto aggType = agg.second;

                if (ParseToken(aggName, tokens, currentIdx).ok())
                {
                    UnresolvedAggregate agg;
                    agg.type = aggType;

                    RAISE_IF_FAILS(ParseToken("(", tokens, currentIdx));
                    ENSURE_TOKEN("col name");
                    agg.col = tokens[currentIdx++];
                    RAISE_IF_FAILS(ParseToken(")", tokens, currentIdx));

                    result.push_back(agg);
                    parsed = true;
                    break;
                }
            }

            if (!parsed)
            {
                ENSURE_TOKEN("column name");
                UnresolvedAggregate agg;
                agg.type = AggregateClause::AGGREGATE_PROJECT;
                agg.col = tokens[currentIdx++];
                result.push_back(agg);
            }
        }
        
        if (!ParseToken(",", tokens, currentIdx).ok())
        {
            break;
        }
    }

    return result;
}

static Result<bool>
ParseTableRef(QueryDesc &queryDesc,
              const TableRegistry &registry,
              const std::vector<std::string> &tokens,
              int &currentIdx)
{
    ENSURE_TOKEN("table name");

    std::string tableName = ToLower(tokens[currentIdx]);
    auto findResult = registry.find(tableName);
    if (findResult == registry.end())
        return Status::Invalid("Table not found: ", tableName);

    currentIdx++;
    queryDesc.tables.push_back(findResult->second.get());

    return true;
}

static Result<bool>
ParseFilters(QueryDesc &queryDesc,
             const std::vector<std::string> &tokens,
             int &currentIdx)
{
    RAISE_IF_FAILS(ParseFiltersDisj(queryDesc, tokens, currentIdx));
    return true;
}

static Result<bool>
ParseFiltersDisj(QueryDesc &queryDesc,
                 const std::vector<std::string> &tokens,
                 int &currentIdx)
{
    // we don't support OR yet, so just assume this is a conjunctive expr
    RAISE_IF_FAILS(ParseFiltersConj(queryDesc, tokens, currentIdx));

    if (ParseToken("or", tokens, currentIdx).ok())
    {
        // TODO: support OR
        return Status::Invalid("OR filters are not supported yet.");
    }

    return true;
}

static Result<bool>
ParseFiltersConj(QueryDesc &queryDesc,
                 const std::vector<std::string> &tokens,
                 int &currentIdx)
{
    FilterClause filterClause;
    ASSIGN_OR_RAISE(filterClause, ParseFilterAtom(queryDesc, tokens, currentIdx));

    queryDesc.filterClauses.push_back(filterClause);

    if (ParseToken("and", tokens, currentIdx).ok())
        ParseFiltersConj(queryDesc, tokens, currentIdx);

    return true;
}

static Result<FilterClause>
ParseFilterAtom(QueryDesc &queryDesc,
                const std::vector<std::string> &tokens,
                int &currentIdx)
{
    int opCount = 6;
    struct {
        std::string token;
        FilterClause::Op op;
    } ops[opCount] = {
        { "=" , FilterClause::FILTER_EQ },
        { "!=" , FilterClause::FILTER_NE },
        { ">" , FilterClause::FILTER_GT },
        { ">=" , FilterClause::FILTER_GTE },
        { "<" , FilterClause::FILTER_LT },
        { "<=" , FilterClause::FILTER_LTE },
    };

    FilterClause result;
    ASSIGN_OR_RAISE(result.columnRef, ParseColumnRef(queryDesc, tokens, currentIdx));
    const auto &columnType = *result.columnRef.type;

    for (int i = 0; i < opCount; i++)
        if (ParseToken(ops[i].token, tokens, currentIdx).ok())
        {
            result.op = ops[i].op;
            ASSIGN_OR_RAISE(result.value, ParseValue(columnType, tokens, currentIdx));
            return result;
        }

    return Status::Invalid("Invalid operator: ", tokens[currentIdx]);
}

static Result<bool> ParseGroupBy(QueryDesc &queryDesc,
                                 const std::vector<std::string> &tokens,
                                 int &currentIdx)
{
    while (true)
    {
        ColumnRef columnRef;
        ASSIGN_OR_RAISE(columnRef, ParseColumnRef(queryDesc, tokens, currentIdx));
        queryDesc.groupBy.push_back(std::move(columnRef));

        if (!ParseToken(",", tokens, currentIdx).ok())
        {
            break;
        }
    }

    return true;
}

static Result<ColumnRef>
ParseColumnRef(QueryDesc &queryDesc,
               const std::vector<std::string> &tokens,
               int &currentIdx)
{
    ENSURE_TOKEN("column reference");

    std::string columnName = tokens[currentIdx++];
    return ResolveColumn(queryDesc, columnName);
}

static Result<ColumnRef>
ResolveColumn(QueryDesc &queryDesc,
              const std::string &columnName)
{
    std::optional<ColumnRef> maybeRef;

    for (int tableIdx = 0; tableIdx < queryDesc.tables.size(); tableIdx++)
    {
        auto table = queryDesc.tables[tableIdx];
        auto maybeFieldIdx = table->ColumnIndex(columnName);
        if (maybeFieldIdx.has_value())
        {
            int fieldIdx = *maybeFieldIdx;
            AccelType *type = table->Schema()[fieldIdx].type.get();
            maybeRef = ColumnRef { table, tableIdx, fieldIdx, type };
        }
    }

    if (!maybeRef.has_value())
        return Status::Invalid("Column not found: ", columnName);

    return std::move(*maybeRef);
}



static Result<std::string> 
ParseValue(const AccelType &type,
           const std::vector<std::string> &tokens,
           int &currentIdx)
{
    std::string result;
    bool strValue = (type.type_num() == STRING_TYPE ||
                     type.type_num() == DATE_TYPE);
    if (strValue)
    {
        RAISE_IF_FAILS(ParseToken("'", tokens, currentIdx));
        ENSURE_TOKEN("filter value");
        result = tokens[currentIdx++];
        RAISE_IF_FAILS(ParseToken("'", tokens, currentIdx));
    }
    else
    {
        ENSURE_TOKEN("filter value");
        result = tokens[currentIdx++];
    }

    return result;
}

static Result<bool>
ResolveAggregates(QueryDesc &queryDesc,
                  const UnresolvedAggV &unresolvedAggs)
{
    for(const auto &unresolvedAgg: unresolvedAggs)
    {
        AggregateClause agg;
        agg.type = unresolvedAgg.type;

        if (unresolvedAgg.col.has_value())
            ASSIGN_OR_RAISE(agg.columnRef,
                            ResolveColumn(queryDesc, *unresolvedAgg.col));

        queryDesc.aggregateClauses.push_back(agg);
    }
    return true;
}

};
