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

static std::vector<std::string> TokenizeQuery(const std::string &query);
static Result<bool> ParseToken(const std::string &kw,
                                 const std::vector<std::string> &tokens,
                                 int &currentIdx);
static Result<bool> ParseAggregates(QueryDesc &queryDesc,
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
static Result<ColumnRef> ParseColumnRef(QueryDesc &queryDesc,
                                        const std::vector<std::string> &tokens,
                                        int &currentIdx);
static Result<std::string> ParseValue(const AccelType &type,
                                      const std::vector<std::string> &tokens,
                                      int &currentIdx);

Result<QueryDesc>
ParseSelect(const std::string &query, const TableRegistry &registry)
{
    QueryDesc queryDesc;
    auto tokens = TokenizeQuery(query);
    int idx = 0;
    RAISE_IF_FAILS(ParseToken("SELECT", tokens, idx));
    RAISE_IF_FAILS(ParseAggregates(queryDesc, tokens, idx));
    RAISE_IF_FAILS(ParseToken("FROM", tokens, idx));
    RAISE_IF_FAILS(ParseTableRef(queryDesc, registry, tokens, idx));

    if (ParseToken("WHERE", tokens, idx).ok())
    {
        RAISE_IF_FAILS(ParseFilters(queryDesc, tokens, idx));
    }

    if (ParseToken("GROUP", tokens, idx).ok())
    {
        RAISE_IF_FAILS(ParseToken("BY", tokens, idx));
        // TODO: parse group by columns
    }

    if (idx != tokens.size())
    {
        return Status::Invalid("Unexpected token:", tokens[idx], ".");
    }

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
    sout << "(op=";
    switch (op)
    {
        case FilterClause::FILTER_EQ:
            sout << "=";
            break;
        case FilterClause::FILTER_LE:
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
        case FilterClause::FILTER_BETWEEN_00:
            sout << "between()";
            break;
        case FilterClause::FILTER_BETWEEN_01:
            sout << "between(]";
            break;
        case FilterClause::FILTER_BETWEEN_10:
            sout << "between[)";
            break;
        case FilterClause::FILTER_BETWEEN_11:
            sout << "between[]";
            break;
    }
    sout << ",columnRef=" << columnRef.ToString();
    sout << ",value=(" << value[0] << "," << value[1] << ")";
    sout << ")";
    return sout.str();
}

std::string AggregateClause::ToString() const
{
    std::ostringstream sout;
    sout << "(type=";
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
    }
    sout << ")";
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
        auto col = filterClause.columnRef;
        sout << "  - " << col.ToString() << std::endl;
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
        else if (c == '(' || c == ')' || c == '=' || c == ',' ||
                 c == '*' || c == '+' || c == '-' || c == '/' || c == '\'') {
            if (current.length())
                tokens.push_back(current);
            current = "";
            std::string token;
            token += c;
            tokens.push_back(token);
            inString = (c == '\'');
        }
        else {
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

static Result<bool>
ParseAggregates(QueryDesc &queryDesc,
                const std::vector<std::string> &tokens,
                int &currentIdx)
{
    if (ParseToken("count", tokens, currentIdx).ok())
    {
        RAISE_IF_FAILS(ParseToken("(", tokens, currentIdx));
        if(ParseToken("*", tokens, currentIdx).ok())
        {
            AggregateClause agg;
            agg.type = AggregateClause::AGGREGATE_COUNT;
            queryDesc.aggregateClauses.push_back(agg);
        }
        else
        {
            // TODO: count(distinct col)
        }
        RAISE_IF_FAILS(ParseToken(")", tokens, currentIdx));
    }
    else
    {
        // TODO: sum(...), max(...), min(...)
        ENSURE_TOKEN("aggregate name");
        std::string aggName = ToLower(tokens[currentIdx++]);
        if (aggName == "sum")
        {
            AggregateClause agg;
            agg.type = AggregateClause::AGGREGATE_SUM;

            RAISE_IF_FAILS(ParseToken("(", tokens, currentIdx));
            // ColumnRef colRef;
            // ASSIGN_OR_RAISE(colRef, ParseColumnRef(queryDesc, tokens, currentIdx));
            ENSURE_TOKEN("col name");
            currentIdx++;
            RAISE_IF_FAILS(ParseToken(")", tokens, currentIdx));

            queryDesc.aggregateClauses.push_back(agg);
        }
    }
    
    if (ParseToken(",", tokens, currentIdx).ok())
    {
        // recursively parse remaining aggregates
        RAISE_IF_FAILS(ParseAggregates(queryDesc, tokens, currentIdx));
    }

    return true;
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
    FilterClause result;
    ASSIGN_OR_RAISE(result.columnRef, ParseColumnRef(queryDesc, tokens, currentIdx));
    const auto &columnType = *result.columnRef.type;

    if (ParseToken("=", tokens, currentIdx).ok())
    {
        result.op = FilterClause::FILTER_EQ;
        ASSIGN_OR_RAISE(result.value[0], ParseValue(columnType, tokens, currentIdx));
    }
    else
    {
        return Status::Invalid("Invalid operator: ", tokens[currentIdx]);
    }

    return result;
}

static Result<ColumnRef>
ParseColumnRef(QueryDesc &queryDesc,
               const std::vector<std::string> &tokens,
               int &currentIdx)
{
    ENSURE_TOKEN("column reference");

    std::string columnName = tokens[currentIdx];
    std::optional<ColumnRef> maybeRef;

    for (int tableIdx = 0; tableIdx < queryDesc.tables.size(); tableIdx++)
    {
        auto table = queryDesc.tables[tableIdx];
        auto maybeFieldIdx = table->ColumnIndex(columnName);
        if (maybeFieldIdx.has_value())
        {
            int fieldIdx = *maybeFieldIdx;
            AccelType *type = table->Schema()[fieldIdx].type.get();
            maybeRef = ColumnRef { tableIdx, fieldIdx, type };
        }
    }

    if (!maybeRef.has_value())
        return Status::Invalid("Column not found: ", columnName);

    currentIdx++;

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

};
