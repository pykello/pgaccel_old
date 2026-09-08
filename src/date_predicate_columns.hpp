#pragma once
#include "parser.h"
#include <map>
#include <set>
namespace pgaccel {
struct DatePredicatePair {
    std::string first,second;
    Expression::Kind kind;
    bool operator==(const DatePredicatePair &) const = default;
};
using DatePredicateMap=std::map<const ColumnarTable *,std::vector<DatePredicatePair>>;
// Preserve only comparison outcomes when neither date's value is observable.
// Cross-alias comparisons are not same-row predicates, even for one table.
inline DatePredicateMap DatePredicateOnlyColumns(const QueryDesc &root) {
    struct Use {bool value=false;std::optional<DatePredicatePair> predicate;};
    std::map<const ColumnarTable *,std::map<std::string,Use>> uses;
    std::set<const QueryDesc *> visited;
    auto visit=[&](auto &&self,const QueryDesc &q)->void {
        if(!visited.insert(&q).second)return;
        auto use=[&](const ColumnRef &c,const DatePredicatePair *p=nullptr) {
            if(c.tableIdx<0 || c.Type()->type_num()!=DATE_TYPE)return;
            auto &u=uses[q.tables.at(c.tableIdx)][c.Name()];
            if(!p){u.value=true;return;}
            if(u.predicate && *u.predicate!=*p)u.value=true;else u.predicate=*p;
        };
        auto expression=[&](auto &&walk,const std::shared_ptr<Expression> &e,bool predicate)->void {
            if(!e)return;
            if(predicate && e->left && e->right && e->left->kind==Expression::COLUMN &&
               e->right->kind==Expression::COLUMN && e->left->column && e->right->column) {
                auto a=*e->left->column,b=*e->right->column;auto kind=e->kind;
                bool compare=kind==Expression::LT || kind==Expression::LTE || kind==Expression::GT ||
                    kind==Expression::GTE || kind==Expression::EQ || kind==Expression::NE;
                if(compare && a.tableIdx>=0 && a.tableIdx==b.tableIdx && a.columnIdx!=b.columnIdx &&
                   a.Type()->type_num()==DATE_TYPE && b.Type()->type_num()==DATE_TYPE) {
                    if(a.Name()>b.Name()) {
                        std::swap(a,b);
                        if(kind==Expression::LT)kind=Expression::GT;else if(kind==Expression::GT)kind=Expression::LT;
                        else if(kind==Expression::LTE)kind=Expression::GTE;else if(kind==Expression::GTE)kind=Expression::LTE;
                    }
                    DatePredicatePair p{a.Name(),b.Name(),kind};use(a,&p);use(b,&p);return;
                }
            }
            if(e->column)use(*e->column);
            bool boolean=predicate && (e->kind==Expression::AND || e->kind==Expression::OR || e->kind==Expression::NOT);
            walk(walk,e->left,boolean);walk(walk,e->right,boolean);walk(walk,e->condition,false);
        };
        for(const auto &f:q.filterClauses)use(f.columnRef);
        for(const auto &e:q.predicates)expression(expression,e,true);
        for(const auto &j:q.leftJoins)for(const auto &e:j.predicates)expression(expression,e,true);
        for(const auto &j:q.existenceJoins)for(const auto &e:j.predicates)expression(expression,e,true);
        for(const auto &c:q.groupBy)use(c);
        for(const auto &e:q.groupExpressions)expression(expression,e,false);
        for(const auto &a:q.aggregateClauses){if(a.columnRef)use(*a.columnRef);expression(expression,a.expression,false);}
        for(const auto &h:q.having)expression(expression,h.bound,false);
        for(const auto &m:q.membershipQueries){use(m.column);self(self,*m.query);}
        for(const auto &s:q.scalarQueries) {
            for(const auto &c:s.outerKeys)use(c);self(self,*s.query);
            for(const auto &c:s.innerKeys)if(c.tableIdx>=0 && c.Type()->type_num()==DATE_TYPE)
                uses[s.query->tables.at(c.tableIdx)][c.Name()].value=true;
        }
        for(const auto &m:q.materializedSources)self(self,*m.query);
    };
    visit(visit,root);DatePredicateMap result;
    for(const auto &[table,columns]:uses)for(const auto &[name,u]:columns) {
        if(u.value || !u.predicate || name!=u.predicate->first)continue;
        auto other=columns.find(u.predicate->second);
        if(other!=columns.end() && !other->second.value && other->second.predicate==u.predicate)
            result[table].push_back(*u.predicate);
    }
    return result;
}
inline Result<std::map<std::string,ColumnDataP>> DatePredicateDictionaries(
    const std::vector<ColumnDesc> &schema,const RowGroup &batch,const std::vector<DatePredicatePair> &pairs) {
    std::map<std::string,ColumnDataP> result;
    for(const auto &pair:pairs) {
        auto column=[&](const std::string &name)->const DictColumnData<DateType>& {
            auto it=std::find_if(schema.begin(),schema.end(),[&](const auto &c){return c.name==name;});
            if(it==schema.end() || it->type->type_num()!=DATE_TYPE)throw std::invalid_argument("Missing date predicate column");
            const auto &p=batch.columns[it-schema.begin()];
            if(p->type!=ColumnDataBase::DICT_COLUMN_DATA)throw std::invalid_argument("Invalid date predicate layout");
            return static_cast<const DictColumnData<DateType>&>(*p);
        };
        const auto &a=column(pair.first),&b=column(pair.second);
        auto value=[](const auto &d,int row){return d.dict[d.bytesPerValue()==1?d.values[row]:reinterpret_cast<const uint16_t *>(d.values)[row]];};
        std::vector<uint8_t> matches(batch.size);int representative[2]{-1,-1};
        for(int row=0;row<batch.size;++row) {
            auto x=value(a,row),y=value(b,row);bool match;
            switch(pair.kind) {
            case Expression::LT:match=x<y;break;case Expression::LTE:match=x<=y;break;
            case Expression::GT:match=x>y;break;case Expression::GTE:match=x>=y;break;
            case Expression::EQ:match=x==y;break;case Expression::NE:match=x!=y;break;
            default:return Status::Invalid("Invalid date predicate comparison");
            }
            matches[row]=match;if(representative[match]<0)representative[match]=row;
        }
        auto compress=[&](const auto &input)->ColumnDataP {
            auto out=std::make_shared<DictColumnData<DateType>>();out->type=ColumnDataBase::DICT_COLUMN_DATA;
            out->size=batch.size;out->valueType=input.valueType;
            for(int row:representative)if(row>=0)out->dict.push_back(value(input,row));
            std::sort(out->dict.begin(),out->dict.end());out->dict.erase(std::unique(out->dict.begin(),out->dict.end()),out->dict.end());
            uint8_t codes[2]{};for(int i=0;i<2;++i)if(representative[i]>=0)
                codes[i]=std::lower_bound(out->dict.begin(),out->dict.end(),value(input,representative[i]))-out->dict.begin();
            out->values=static_cast<uint8_t *>(AllocateAligned(512,batch.size));
            for(int row=0;row<batch.size;++row)out->values[row]=codes[matches[row]];
            return out;
        };
        result.emplace(pair.first,compress(a));result.emplace(pair.second,compress(b));
    }
    return result;
}
}
