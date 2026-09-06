#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteJoinCount(const QueryDesc &,bool,bool); }
using namespace pgaccel;
TEST(JoinCount, SparseKeysDatePredicatesConditionalCountsAndDuplicateFallback) {
    for(int64_t keyOffset:{int64_t(0),int64_t(INT64_MIN)})
    for(bool duplicate:{false,true}) {
        std::vector<std::vector<ColumnDataP>> groups;
        for(int g=0;g<3;++g) {
            std::vector<int64_t> keys;std::vector<std::string> priority;
            for(int i=0;i<23;++i){keys.push_back(keyOffset+(g*23+i)*3);priority.push_back(i%3==0?"urgent":i%3==1?"high":"other");}
            if(g==2 && duplicate)keys[22]=keyOffset+6;
            if(g%2){std::reverse(keys.begin(),keys.end());std::reverse(priority.begin(),priority.end());}
            groups.push_back({test::Raw(keys,keyOffset?8:4),test::Dict<StringType>(priority)});
        }
        auto registry=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"priority",std::make_shared<StringType>()}});
        groups.clear();
        for(int g=0;g<3;++g) {
            std::vector<int64_t> keys;std::vector<int32_t> a,b;std::vector<std::string> mode;
            for(int i=0;i<259;++i){keys.push_back(keyOffset+i);a.push_back(i%5);b.push_back(i%7);mode.push_back(i%3==0?"mail":i%3==1?"ship":"air");}
            keys[0]=INT64_MIN;keys[1]=INT64_MAX;
            groups.push_back({test::Raw(keys),test::Dict<DateType>(a),test::Dict<DateType>(b),test::Dict<StringType>(mode)});
        }
        auto fact=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"a",std::make_shared<DateType>()},{"b",std::make_shared<DateType>()},{"mode",std::make_shared<StringType>()}});
        registry.emplace("f",std::move(fact.at("t")));
        for(const std::string suffix:{"", " AND f.mode IN ('mail','ship')", " AND f.a<CAST('1960-01-01' AS date)"})
        for(const std::string tail:{"", " LIMIT 1 OFFSET 1"}) {
            auto q=ParseSelect("SELECT f.mode,sum(CASE WHEN priority='urgent' OR priority='high' THEN 1 ELSE 0 END),"
                "sum(CASE WHEN priority<>'urgent' AND priority<>'high' THEN 1 ELSE 0 END),"
                "sum(CASE WHEN priority='never' THEN 1 ELSE 0 END),count(*) FROM t,f "
                "WHERE t.k=f.k AND f.a<f.b"+suffix+" GROUP BY f.mode ORDER BY f.mode"+tail,registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();
            auto direct=ExecuteJoinCount(*q,true,true);EXPECT_EQ(direct.has_value(),!duplicate);
            QueryDesc reference=*q;
            for(const auto &p:q->predicates)if(p->kind==Expression::EQ){reference.predicates.push_back(p);break;}
            auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
                EXPECT_EQ(actual->values,expected->values);
            }
        }
    }
}
