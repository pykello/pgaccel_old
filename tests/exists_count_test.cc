#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteExistsCount(const QueryDesc &,bool,bool); }
using namespace pgaccel;
TEST(ExistsCount, DuplicateOuterRowsInnerMultiplicityAntiAndFilters) {
    for(int width:{4,8}) {
        auto registry=test::Table({{test::Raw({1,1,2,3,4,7,9,10},width),test::Dict<StringType>({"a","b","a","c","b","a","b","c"}),test::Dict<DateType>({1,1,2,3,4,5,6,7})}},
            {{"k",std::make_shared<Int64Type>()},{"label",std::make_shared<StringType>()},{"day",std::make_shared<DateType>()}});
        std::vector<int64_t> keys;std::vector<int32_t> a,b;
        for(int i=0;i<67;++i){keys.push_back(i%13);a.push_back(i%7);b.push_back(i%5);}
        keys[0]=width==4?INT32_MIN:INT64_MIN;keys[1]=width==4?INT32_MAX:INT64_MAX;
        std::vector<std::vector<ColumnDataP>> groups;
        for(int g=0;g<3;++g)groups.push_back({test::Raw(keys,width),test::Dict<DateType>(a),test::Dict<DateType>(b)});
        auto fact=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"a",std::make_shared<DateType>()},{"b",std::make_shared<DateType>()}});
        registry.emplace("f",std::move(fact.at("t")));
        for(const std::string anti:{"", "NOT "})for(const std::string extra:{"", " AND o.day<CAST('1970-01-05' AS date)", " AND o.day<CAST('1960-01-01' AS date)"}) {
            auto q=ParseSelect("SELECT o.label,count(*) FROM t o WHERE "+anti+
                "EXISTS(SELECT * FROM f WHERE f.k=o.k AND f.a<f.b)"+extra+" GROUP BY o.label ORDER BY o.label",registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();
            auto direct=ExecuteExistsCount(*q,true,true);ASSERT_TRUE(direct.has_value());ASSERT_TRUE(direct->ok())<<direct->status().Message();
            QueryDesc reference=*q;auto &predicates=reference.existenceJoins[0].predicates;
            for(const auto &p:predicates)if(p->kind==Expression::EQ){predicates.push_back(p);break;}
            auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
                EXPECT_EQ(actual->values,expected->values);
            }
        }
    }
}
