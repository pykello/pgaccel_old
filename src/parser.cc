#include "parser.h"
#include "util.h"
#include <cctype>
#include <stdexcept>

namespace pgaccel
{
namespace
{
struct Token
{
    std::string text;
    bool quoted = false;
};
std::vector<Token> Lex(const std::string &sql)
{
    std::vector<Token> out;
    for (size_t i = 0; i < sql.size();)
    {
        unsigned char c = sql[i];
        if (std::isspace(c))
        {
            ++i;
            continue;
        }
        if (c == '\'')
        {
            ++i;
            std::string s;
            bool closed = false;
            while (i < sql.size())
            {
                if (sql[i] == '\'')
                {
                    ++i;
                    if (i < sql.size() && sql[i] == '\'')
                    {
                        s += '\'';
                        ++i;
                    }
                    else
                    {
                        closed = true;
                        break;
                    }
                }
                else
                    s += sql[i++];
            }
            if (!closed)
                throw std::invalid_argument("Unterminated string");
            out.push_back({s, true});
        }
        else if (std::isalnum(c) || c == '_' || c == '.')
        {
            size_t start = i++;
            while (i < sql.size() &&
                   (std::isalnum((unsigned char)sql[i]) || sql[i] == '_' || sql[i] == '.'))
                ++i;
            out.push_back({sql.substr(start, i - start)});
        }
        else
        {
            std::string s(1, sql[i++]);
            if (i < sql.size() && ((s == "<" && (sql[i] == '>' || sql[i] == '=')) ||
                                   ((s == ">" || s == "!") && sql[i] == '=')))
                s += sql[i++];
            out.push_back({s});
        }
    }
    return out;
}
struct Parser
{
    std::vector<Token> tokens;
    size_t at = 0;
    QueryDesc q;
    const TableRegistry &registry;
    bool eat(const std::string &s)
    {
        if (at < tokens.size() && !tokens[at].quoted && ToLower(tokens[at].text) == ToLower(s))
        {
            ++at;
            return true;
        }
        return false;
    }
    void need(const std::string &s)
    {
        if (!eat(s))
            throw std::invalid_argument("Expected " + s);
    }
    Token take()
    {
        if (at == tokens.size())
            throw std::invalid_argument("Unexpected end of SQL");
        return tokens[at++];
    }
    ColumnRef resolve(const std::string &s)
    {
        auto i = q.tables[0]->ColumnIndex(s);
        if (!i)
            throw std::invalid_argument("Unknown column: " + s);
        return {q.tables[0]->Schema()[*i], 0, *i};
    }
    std::shared_ptr<Expression> expr(int precedence = 0)
    {
        auto e = std::make_shared<Expression>();
        if (eat("("))
        {
            e = expr();
            need(")");
        }
        else if (eat("extract"))
        {
            e->kind=Expression::EXTRACT;
            need("("); e->literal=ToLower(take().text); need("from"); e->left=expr(); need(")");
            if (e->left->type->type_num()!=TIMESTAMP_TYPE)
                throw std::invalid_argument("EXTRACT requires a timestamp");
            if (e->literal!="minute" && e->literal!="hour" && e->literal!="second")
                throw std::invalid_argument("EXTRACT supports second, minute, and hour");
            e->type=std::make_shared<Int64Type>();
        }
        else if (eat("date_trunc"))
        {
            e->kind=Expression::DATE_TRUNC;
            need("("); auto unit=take();
            if (!unit.quoted) throw std::invalid_argument("DATE_TRUNC unit must be a string");
            e->literal=ToLower(unit.text); need(","); e->left=expr(); need(")");
            if (e->left->type->type_num()!=TIMESTAMP_TYPE)
                throw std::invalid_argument("DATE_TRUNC requires a timestamp");
            if (e->literal!="second" && e->literal!="minute" && e->literal!="hour" && e->literal!="day")
                throw std::invalid_argument("Unsupported DATE_TRUNC unit");
            e->type=std::make_shared<TimestampType>();
        }
        else if (eat("length"))
        {
            need("("); e->left=expr(); need(")");
            if (e->left->type->type_num()!=STRING_TYPE)
                throw std::invalid_argument("length requires a string");
            e->kind=Expression::LENGTH; e->type=std::make_shared<Int64Type>();
        }
        else if (eat("-"))
        {
            e->kind = Expression::SUBTRACT;
            e->left = std::make_shared<Expression>();
            e->left->type = std::make_shared<Int64Type>();
            e->right = expr(3);
            e->type = e->right->type;
        }
        else if (eat("+"))
            e = expr(3);
        else
        {
            auto t = take();
            if (t.quoted)
            {
                e->literal = t.text;
                e->type = std::make_shared<StringType>();
            }
            else if (!t.text.empty() && std::isdigit((unsigned char)t.text[0]))
            {
                e->literal = t.text;
                if (t.text.find('.') != std::string::npos)
                {
                    auto ty = std::make_shared<DecimalType>();
                    ty->scale = t.text.size() - t.text.find('.') - 1;
                    ParseDecimal(ty->scale, t.text);
                    e->type = ty;
                }
                else
                {
                    size_t consumed;
                    std::stoll(t.text, &consumed);
                    if (consumed != t.text.size())
                        throw std::invalid_argument("Invalid integer");
                    e->type = std::make_shared<Int64Type>();
                }
            }
            else
            {
                e->kind = Expression::COLUMN;
                e->column = resolve(t.text);
                e->type = e->column->Type();
            }
        }
        while (at < tokens.size())
        {
            auto op = tokens[at].text;
            int p = op == "+" || op == "-" ? 1 : op == "*" || op == "/" ? 2 : 0;
            if (!p || p <= precedence)
                break;
            ++at;
            auto parent = std::make_shared<Expression>();
            parent->left = e;
            parent->right = expr(p);
            parent->kind = op == "+"   ? Expression::ADD
                           : op == "-" ? Expression::SUBTRACT
                           : op == "*" ? Expression::MULTIPLY
                                       : Expression::DIVIDE;
            auto numeric = [](TypeNum t)
            { return t == INT32_TYPE || t == INT64_TYPE || t == DECIMAL_TYPE; };
            if (!numeric(e->type->type_num()) || !numeric(parent->right->type->type_num()))
                throw std::invalid_argument("Arithmetic requires numeric operands");
            auto scale = [](const auto &ty) {
                return ty->type_num() == DECIMAL_TYPE ? static_cast<DecimalType *>(ty.get())->scale
                                                      : 0;
            };
            int l = scale(e->type), r = scale(parent->right->type);
            int s = parent->kind == Expression::MULTIPLY ? l + r
                    : parent->kind == Expression::DIVIDE ? std::max(6, l)
                                                         : std::max(l, r);
            if (s > 18)
                throw std::invalid_argument("Expression scale exceeds 18");
            if (s)
            {
                auto ty = std::make_shared<DecimalType>();
                ty->scale = s;
                parent->type = ty;
            }
            else
                parent->type = std::make_shared<Int64Type>();
            e = parent;
        }
        return e;
    }
    std::string signature(size_t start, size_t end)
    {
        std::string s;
        for (size_t i = start; i < end; ++i)
            s += (tokens[i].quoted ? "'" + tokens[i].text + "'" : ToLower(tokens[i].text)) + " ";
        return s;
    }
    AggregateClause aggregate()
    {
        AggregateClause a;
        a.type = AggregateClause::AGGREGATE_PROJECT;
        size_t start = at;
        if (eat("count"))
            a.type = AggregateClause::AGGREGATE_COUNT;
        else if (eat("sum"))
            a.type = AggregateClause::AGGREGATE_SUM;
        else if (eat("avg"))
            a.type = AggregateClause::AGGREGATE_AVG;
        else if (eat("min"))
            a.type = AggregateClause::AGGREGATE_MIN;
        else if (eat("max"))
            a.type = AggregateClause::AGGREGATE_MAX;
        if (a.type != AggregateClause::AGGREGATE_PROJECT)
        {
            need("(");
            if (a.type == AggregateClause::AGGREGATE_COUNT && eat("distinct"))
                a.type = AggregateClause::AGGREGATE_COUNT_DISTINCT;
            if (a.type == AggregateClause::AGGREGATE_COUNT && eat("*"))
            {
            }
            else
                a.expression = expr();
            need(")");
        }
        else
            a.expression = expr();
        if (a.expression && a.expression->kind == Expression::COLUMN)
            a.columnRef = a.expression->column;
        if (a.expression &&
            (a.type == AggregateClause::AGGREGATE_SUM ||
             a.type == AggregateClause::AGGREGATE_AVG) &&
            (a.expression->type->type_num() == STRING_TYPE ||
             a.expression->type->type_num() == DATE_TYPE ||
             a.expression->type->type_num() == TIMESTAMP_TYPE))
            throw std::invalid_argument("SUM/AVG require numeric input");
        a.sql = signature(start, at);
        return a;
    }
    std::string filterValue(const ColumnRef &column)
    {
                std::string sign;
                if (eat("-"))
                    sign = "-";
                else
                    eat("+");
                auto literal = take();
                std::string value = sign + literal.text;
                auto type = column.Type()->type_num();
                if ((type == STRING_TYPE || type == DATE_TYPE || type == TIMESTAMP_TYPE) !=
                    literal.quoted)
                    throw std::invalid_argument("Incorrect literal type");
                if (type == DATE_TYPE)
                    ParseDate(value);
                if (type == TIMESTAMP_TYPE)
                    ParseTimestamp(value);
                if (type == DECIMAL_TYPE)
                    ParseDecimal(static_cast<DecimalType *>(column.Type().get())->scale,
                                 value);
                if (type == INT32_TYPE || type == INT64_TYPE)
                {
                    size_t n;
                    std::stoll(value, &n);
                    if (n != value.size())
                        throw std::invalid_argument("Invalid integer literal");
                }
        return value;
    }
    bool same(const Expression &a,const Expression &b)
    {
        if (a.kind!=b.kind || a.column!=b.column || a.literal!=b.literal) return false;
        if (bool(a.left)!=bool(b.left) || bool(a.right)!=bool(b.right)) return false;
        return (!a.left || same(*a.left,*b.left)) && (!a.right || same(*a.right,*b.right));
    }
    QueryDesc parse()
    {
        // Resolve the one source table before parsing SELECT expressions.
        size_t from = 0;
        int depth = 0;
        for (; from < tokens.size(); ++from)
        {
            if (tokens[from].quoted)
                continue;
            if (tokens[from].text == "(")
                ++depth;
            if (tokens[from].text == ")")
                --depth;
            if (!depth && ToLower(tokens[from].text) == "from")
                break;
        }
        if (from + 1 >= tokens.size())
            throw std::invalid_argument("Expected FROM table");
        auto table = registry.find(ToLower(tokens[from + 1].text));
        if (table == registry.end())
            throw std::invalid_argument("Unknown table");
        q.tables.push_back(table->second.get());
        need("select");
        do
        {
            if (eat("*"))
            {
                for (const auto &column:q.tables[0]->Schema())
                {
                    AggregateClause a; a.type=AggregateClause::AGGREGATE_PROJECT;
                    a.expression=std::make_shared<Expression>(); a.expression->kind=Expression::COLUMN;
                    a.columnRef=resolve(column.name); a.expression->column=a.columnRef;
                    a.expression->type=column.type; a.sql=ToLower(column.name)+" ";
                    q.aggregateClauses.push_back(std::move(a));
                }
                continue;
            }
            auto a = aggregate();
            if (eat("as"))
                a.alias = take().text;
            q.aggregateClauses.push_back(std::move(a));
        } while (eat(","));
        q.outputColumnCount=q.aggregateClauses.size();
        need("from");
        take();
        if (eat("where"))
            do
            {
                FilterClause f;
                f.columnRef = resolve(take().text);
                auto op = ToLower(take().text);
                if (op=="not") op += " " + ToLower(take().text);
                if (op == "=")
                    f.op = FilterClause::FILTER_EQ;
                else if (op == "!=" || op == "<>")
                    f.op = FilterClause::FILTER_NE;
                else if (op == "<")
                    f.op = FilterClause::FILTER_LT;
                else if (op == "<=")
                    f.op = FilterClause::FILTER_LTE;
                else if (op == ">")
                    f.op = FilterClause::FILTER_GT;
                else if (op == ">=")
                    f.op = FilterClause::FILTER_GTE;
                else if (op=="like") f.op=FilterClause::FILTER_LIKE;
                else if (op=="not like") f.op=FilterClause::FILTER_NOT_LIKE;
                else if (op=="in") f.op=FilterClause::FILTER_IN;
                else if (op=="not in") f.op=FilterClause::FILTER_NOT_IN;
                else
                    throw std::invalid_argument("Unsupported filter operator " + op);
                if (f.op==FilterClause::FILTER_IN || f.op==FilterClause::FILTER_NOT_IN)
                {
                    need("(");
                    do { f.values.push_back(filterValue(f.columnRef)); } while (eat(","));
                    need(")");
                }
                else f.value=filterValue(f.columnRef);
                if ((f.op==FilterClause::FILTER_LIKE || f.op==FilterClause::FILTER_NOT_LIKE) &&
                    f.columnRef.Type()->type_num()!=STRING_TYPE)
                    throw std::invalid_argument("LIKE requires string operands");
                q.filterClauses.push_back(std::move(f));
            } while (eat("and"));
        if (eat("group"))
        {
            need("by");
            q.grouped = true;
            do
            {
                std::shared_ptr<Expression> e;
                if (at<tokens.size() && !tokens[at].quoted && !q.tables[0]->ColumnIndex(tokens[at].text))
                    for (const auto &a:q.aggregateClauses)
                        if (!a.alias.empty() && ToLower(a.alias)==ToLower(tokens[at].text))
                        {
                            if (a.type!=AggregateClause::AGGREGATE_PROJECT)
                                throw std::invalid_argument("Cannot group by an aggregate alias");
                            e=a.expression; ++at; break;
                        }
                if (!e) e=expr();
                if (e->kind!=Expression::CONSTANT)
                {
                    q.groupExpressions.push_back(e);
                    if (e->kind==Expression::COLUMN) q.groupBy.push_back(*e->column);
                }
            } while (eat(","));
        }
        // Legacy dense paths apply only when every varying key is one column.
        if (q.groupExpressions.size()!=1 || q.groupExpressions[0]->kind!=Expression::COLUMN)
            q.groupBy.clear();
        if (eat("having"))
        {
            do
            {
                auto a=aggregate();
                if (a.type!=AggregateClause::AGGREGATE_COUNT)
                    throw std::invalid_argument("HAVING currently requires COUNT");
                size_t index=0;
                while (index<q.aggregateClauses.size() && q.aggregateClauses[index].sql!=a.sql) ++index;
                if (index==q.aggregateClauses.size()) q.aggregateClauses.push_back(std::move(a));
                auto op=take().text; FilterClause::Op comparison;
                if(op==">")comparison=FilterClause::FILTER_GT;
                else if(op==">=")comparison=FilterClause::FILTER_GTE;
                else if(op=="<")comparison=FilterClause::FILTER_LT;
                else if(op=="<=")comparison=FilterClause::FILTER_LTE;
                else if(op=="=")comparison=FilterClause::FILTER_EQ;
                else if(op=="<>" || op=="!=")comparison=FilterClause::FILTER_NE;
                else throw std::invalid_argument("Unsupported HAVING comparison");
                std::string sign=eat("-")?"-":""; auto literal=take(); size_t used;
                if(literal.quoted)throw std::invalid_argument("HAVING requires an integer bound");
                int64_t value=std::stoll(sign+literal.text,&used);
                if(used!=sign.size()+literal.text.size())throw std::invalid_argument("Invalid HAVING bound");
                q.having.push_back({index,comparison,value});
            } while(eat("and"));
        }
        if (eat("order"))
        {
            need("by");
            do
            {
                size_t index = q.aggregateClauses.size();
                for (size_t i = 0; i < q.aggregateClauses.size(); ++i)
                    if (!q.aggregateClauses[i].alias.empty() && at < tokens.size() &&
                        ToLower(tokens[at].text) == ToLower(q.aggregateClauses[i].alias))
                    {
                        index = i;
                        ++at;
                        break;
                    }
                if (index == q.aggregateClauses.size())
                {
                    auto a = aggregate();
                    for (size_t i = 0; i < q.aggregateClauses.size(); ++i)
                        if (a.sql == q.aggregateClauses[i].sql) { index=i; break; }
                    if (index == q.aggregateClauses.size())
                        q.aggregateClauses.push_back(std::move(a));
                }
                bool desc = eat("desc");
                if (!desc)
                    eat("asc");
                q.orderBy.push_back({index, desc});
            } while (eat(","));
        }
        if (eat("limit"))
        {
            auto t = take().text;
            size_t n;
            if (t.empty() || !std::isdigit((unsigned char)t[0]))
                throw std::invalid_argument("Invalid LIMIT");
            auto value = std::stoull(t, &n);
            if (n != t.size())
                throw std::invalid_argument("Invalid LIMIT");
            q.limit = value;
        }
        if (eat("offset"))
        {
            auto t = take().text;
            size_t n;
            if (t.empty() || !std::isdigit((unsigned char)t[0]))
                throw std::invalid_argument("Invalid OFFSET");
            q.offset = std::stoull(t, &n);
            if (n != t.size())
                throw std::invalid_argument("Invalid OFFSET");
        }
        if (q.limit && q.offset > UINT64_MAX - *q.limit)
            throw std::invalid_argument("LIMIT plus OFFSET overflows");
        eat(";");
        if (at != tokens.size())
            throw std::invalid_argument("Unsupported SQL near " + tokens[at].text);
        bool hasAgg = false;
        for (const auto &a : q.aggregateClauses)
            hasAgg |= a.type != AggregateClause::AGGREGATE_PROJECT;
        for (const auto &a : q.aggregateClauses)
            if (a.type == AggregateClause::AGGREGATE_PROJECT && (hasAgg || q.grouped))
            {
                if (a.expression->kind != Expression::CONSTANT &&
                    !std::any_of(q.groupExpressions.begin(),q.groupExpressions.end(),
                        [&](const auto &e){return same(*a.expression,*e);}))
                    throw std::invalid_argument(
                        "Selected expression must be grouped or aggregated");
            }
        return std::move(q);
    }
};
} // namespace
Result<QueryDesc> ParseSelect(const std::string &sql, const TableRegistry &registry)
{
    try
    {
        return Parser{Lex(sql), 0, {}, registry}.parse();
    }
    catch (const std::exception &e)
    {
        return Status::Invalid(e.what());
    }
}
std::string ColumnRef::ToString() const { return Name(); }
std::string FilterClause::ToString() const
{
    return columnRef.Name() + " " + std::to_string(op) + " " + value;
}
std::string AggregateClause::ToString() const
{
    return !alias.empty() ? alias : !sql.empty() ? sql : columnRef ? columnRef->Name() : "count";
}
std::string QueryDesc::ToString() const
{
    std::string s;
    for (auto &a : aggregateClauses)
        s += a.ToString() + "\n";
    return s;
}
} // namespace pgaccel
