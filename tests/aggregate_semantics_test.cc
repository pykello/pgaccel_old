#include "test_table.hpp"
#include <numeric>
#include <gtest/gtest.h>
using namespace pgaccel;
static void Check(const TableRegistry &t,const std::string &sql,const Rows &expected) {
    auto q=ParseSelect(sql,t); ASSERT_TRUE(q.ok())<<q.status().Message();
    for(bool avx:{false,true}) for(bool parallel:{false,true}) {
        auto r=ExecuteQuery(*q,avx,parallel); ASSERT_TRUE(r.ok())<<r.status().Message(); EXPECT_EQ(r->values,expected)<<sql;
    }
}
TEST(Aggregation, WideMixedProjectionAndOrdering) {
    auto t=test::Table({{test::Raw({INT64_MAX,INT64_MAX,7}),test::Dict<StringType>({"b","b","a"})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT sum(v), count(*), min(v), max(v) FROM t",{{"18446744073709551621","3","7","9223372036854775807"}});
    Check(t,"SELECT v FROM t WHERE v = 9223372036854775807",{{"9223372036854775807"},{"9223372036854775807"}});
    Check(t,"SELECT v FROM t WHERE v = 42",{});
    Check(t,"SELECT 1,s,count(*) AS c FROM t GROUP BY 1,s ORDER BY c DESC LIMIT 1",{{"1","b","2"}});
    Check(t,"SELECT s, sum(v),avg(v),min(v),max(v) FROM t WHERE v = 7 GROUP BY s",{{"a","7","7","7","7"}});
    Check(t,"SELECT sum(v),avg(v),min(v),max(v),count(*) FROM t WHERE v = 42",{{"NULL","NULL","NULL","NULL","0"}});
    Check(t,"SELECT sum(v+1),sum((v+2)*3),sum(v/2) FROM t WHERE v = 7",{{"8","27","3.500000"}});
}
TEST(Aggregation, DistinctUsesValuesAcrossDictionaries) {
    auto t=test::Table({{test::Raw({1,1,2}),test::Dict<StringType>({"a","c","a"})},
                        {test::Raw({2,3,3}),test::Dict<StringType>({"c","a","b"})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT count(distinct v),count(distinct s) FROM t",{{"3","3"}});
    Check(t,"SELECT count(distinct s) FROM t",{{"3"}});
    Check(t,"SELECT count(distinct s) FROM t WHERE v = 1",{{"2"}});
    Check(t,"SELECT s,count(distinct v) AS n,count(*) FROM t GROUP BY s ORDER BY n DESC,s",{{"a","3","3"},{"c","2","2"},{"b","1","1"}});
    auto q=ParseSelect("SELECT count(distinct v) FROM t",t); ASSERT_TRUE(q.ok());
    auto plan=std::move(q).ValueUnsafe(); plan.memoryBudgetBytes=1;
    auto r=ExecuteQuery(plan,true,true); ASSERT_FALSE(r.ok()); EXPECT_NE(r.status().Message().find("budget"),std::string::npos);
}
TEST(Aggregation, MaximumDateDictionaryHasNoDiscardSentinel) {
    std::vector<int32_t> dates(65536); std::iota(dates.begin(),dates.end(),0);
    auto t=test::Table({{test::Dict<DateType>(dates)}},{{"d",std::make_shared<DateType>()}});
    Check(t,"SELECT d,count(*) FROM t WHERE d < '1970-01-03' GROUP BY d",{{"1970-01-01","1"},{"1970-01-02","1"}});
}
TEST(Parser, UnsupportedAndMalformedQueriesFail) {
    auto t=test::Table({{test::Raw({1}),test::Raw({2})}},{{"v",std::make_shared<Int64Type>()},{"w",std::make_shared<Int64Type>()}});
    for(auto sql:{"SELECT count(*) FROM t WHERE v=1 AND", "SELECT count(*) FROM t WHERE", "SELECT v,count(*) FROM t", "SELECT count(*) FROM t WHERE v=1 garbage", "SELECT sum(v) FROM t LIMIT -1"}) EXPECT_FALSE(ParseSelect(sql,t).ok())<<sql;
}

TEST(Aggregation, OffsetBoundaries) {
    auto t=test::Table({{test::Raw({3,1,2,2})}},{{"v",std::make_shared<Int64Type>()}});
    Check(t,"SELECT v FROM t ORDER BY v LIMIT 2 OFFSET 1",{{"2"},{"2"}});
    Check(t,"SELECT v,count(*) AS n FROM t GROUP BY v ORDER BY n DESC,v LIMIT 1 OFFSET 1",{{"1","1"}});
    Check(t,"SELECT v FROM t LIMIT 1 OFFSET 2",{{"2"}});
    Check(t,"SELECT v FROM t ORDER BY v OFFSET 3",{{"3"}});
    Check(t,"SELECT v FROM t ORDER BY v LIMIT 0 OFFSET 1",{});
    Check(t,"SELECT count(*) FROM t OFFSET 1",{});
    Check(t,"SELECT sum(v) FROM t OFFSET 1",{});
    Check(t,"SELECT v FROM t LIMIT 2 OFFSET 10",{});
    EXPECT_FALSE(ParseSelect("SELECT v FROM t LIMIT 18446744073709551615 OFFSET 1",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT v FROM t OFFSET -1",t).ok());
}

TEST(Aggregation, CompositeLogicalKeys) {
    auto t=test::Table({{test::Raw({1,1,2}),test::Dict<StringType>({"a","c","a"})},
                        {test::Raw({2,3,3}),test::Dict<StringType>({"c","a","b"})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT s,v,count(*) FROM t GROUP BY v,s ORDER BY s,v",{{"a","1","1"},{"a","2","1"},{"a","3","1"},{"b","3","1"},{"c","1","1"},{"c","2","1"}});
    Check(t,"SELECT v,s,sum(v),avg(v),count(*) FROM t GROUP BY v,s ORDER BY v,s LIMIT 2 OFFSET 2",{{"2","a","2","2","1"},{"2","c","2","2","1"}});
    Check(t,"SELECT v,s,count(distinct v),min(v) FROM t GROUP BY v,s ORDER BY v,s LIMIT 1",{{"1","a","1","1"}});
    EXPECT_FALSE(ParseSelect("SELECT s,count(*) FROM t GROUP BY v",t).ok());
}

TEST(Aggregation, DerivedKeysAndAliases) {
    auto t=test::Table({{test::Raw({1,1,2,3})}},{{"v",std::make_shared<Int64Type>()}});
    Check(t,"SELECT v,v-1,v-2,count(*) FROM t GROUP BY v,v-1,v-2 ORDER BY v",{{"1","0","-1","2"},{"2","1","0","1"},{"3","2","1","1"}});
    Check(t,"SELECT v*0 AS k,count(*) FROM t GROUP BY k",{{"0","4"}});
    Check(t,"SELECT v-1 AS k,sum(v) FROM t GROUP BY k ORDER BY k LIMIT 1 OFFSET 1",{{"1","2"}});
    EXPECT_FALSE(ParseSelect("SELECT v,count(*) FROM t GROUP BY v-1",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT sum(v) AS s FROM t GROUP BY s",t).ok());
}
TEST(Aggregation, CompositeMergesAcrossDictionaryCodeSpaces) {
    auto t=test::Table({{test::Raw({1,1,2}),test::Dict<StringType>({"a","c","a"}),test::Raw({1,2,3})},
                        {test::Raw({1,3,1}),test::Dict<StringType>({"c","b","c"}),test::Raw({4,5,2})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()},{"w",std::make_shared<Int64Type>()}});
    Check(t,"SELECT v,s,count(*) AS n FROM t GROUP BY v,s ORDER BY n DESC LIMIT 1",{{"1","c","3"}});
    Check(t,"SELECT v,s,sum(w),count(*) AS n FROM t GROUP BY v,s ORDER BY n DESC LIMIT 1",{{"1","c","8","3"}});
    Check(t,"SELECT v,s,count(distinct w),min(w),max(w),count(*) AS n FROM t GROUP BY v,s ORDER BY n DESC LIMIT 1",{{"1","c","2","2","4","3"}});
}
TEST(Predicates, LikeAndMembership) {
    auto t=test::Table({{test::Raw({1,2,3,4,5}),test::Dict<StringType>({"google","Google","¥","a_b","a%b"})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT count(*) FROM t WHERE s LIKE '%google%'",{{"1"}});
    Check(t,"SELECT s FROM t WHERE s LIKE '_'",{{"¥"}});
    Check(t,"SELECT s FROM t WHERE s LIKE 'a\\_b'",{{"a_b"}});
    Check(t,"SELECT s FROM t WHERE s LIKE 'a\\%b'",{{"a%b"}});
    Check(t,"SELECT count(*) FROM t WHERE s NOT LIKE '%google%' AND v IN (1,2,2,3)",{{"2"}});
    Check(t,"SELECT count(*) FROM t WHERE s IN ('¥','google') AND v NOT IN (2,3)",{{"1"}});
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE v LIKE '%1%'",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE v IN ()",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE v IN (1,NULL)",t).ok());
}

TEST(Expressions, ByteLength) {
    auto t=test::Table({{test::Dict<StringType>({"", "¥", "abc"})}},{{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT length(s) AS n FROM t ORDER BY n",{{"0"},{"2"},{"3"}});
    Check(t,"SELECT sum(length(s)),min(length(s)),max(length(s)) FROM t",{{"5","0","3"}});
    EXPECT_FALSE(ParseSelect("SELECT length(1) FROM t",t).ok());
}

TEST(Expressions, TimestampUnitsAndNegativeEpoch) {
    auto t=test::Table({{test::Raw({-61,-1,0,59,60,3661})}},{{"ts",std::make_shared<TimestampType>()}});
    Check(t,"SELECT extract(minute FROM ts) AS m,count(*) FROM t GROUP BY m ORDER BY m",{{"0","2"},{"1","2"},{"58","1"},{"59","1"}});
    Check(t,"SELECT date_trunc('minute',ts) AS m,count(*) FROM t GROUP BY m ORDER BY m LIMIT 2",{{"1969-12-31 23:58:00","1"},{"1969-12-31 23:59:00","1"}});
    Check(t,"SELECT date_trunc('day',ts) AS d,count(*) FROM t GROUP BY d ORDER BY d",{{"1969-12-31 00:00:00","2"},{"1970-01-01 00:00:00","4"}});
    Check(t,"SELECT extract(year FROM ts) AS y,count(*) FROM t GROUP BY y ORDER BY y",{{"1969","2"},{"1970","4"}});
    EXPECT_FALSE(ParseSelect("SELECT extract(quarter FROM ts) FROM t",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT date_trunc('minute',1) FROM t",t).ok());
}

TEST(Projection, StarAndHiddenSortColumns) {
    auto t=test::Table({{test::Raw({3,1,2}),test::Dict<StringType>({"a","c","b"})}},
                      {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT count(*) FROM t ORDER BY sum(v)",{{"3"}});
    Check(t,"SELECT s FROM t ORDER BY v LIMIT 2",{{"c"},{"b"}});
    Check(t,"SELECT * FROM t ORDER BY v LIMIT 2",{{"1","c"},{"2","b"}});
    Check(t,"SELECT s FROM t GROUP BY s ORDER BY count(*) DESC,s LIMIT 2",{{"a"},{"b"}});
    EXPECT_FALSE(ParseSelect("SELECT s,count(*) FROM t GROUP BY s ORDER BY v",t).ok());
}

TEST(Aggregation, HavingBeforeOrderAndLimit) {
    auto t=test::Table({{test::Raw({1,1,2,3,3,3})}},{{"v",std::make_shared<Int64Type>()}});
    Check(t,"SELECT v,count(*) AS n FROM t GROUP BY v HAVING count(*) > 1 ORDER BY n DESC LIMIT 1",{{"3","3"}});
    Check(t,"SELECT v FROM t GROUP BY v HAVING count(*) >= 2 ORDER BY v",{{"1"},{"3"}});
    Check(t,"SELECT count(*) FROM t HAVING count(*) > 6",{});
    Check(t,"SELECT count(*) FROM t HAVING count(*) = 6",{{"6"}});
    Check(t,"SELECT v,sum(v) FROM t GROUP BY v HAVING count(*) > 10",{});
}

TEST(Expressions, RegexReplacementOwnership) {
    auto t=test::Table({{test::Dict<StringType>({"https://www.example.com/a","http://example.com/b","plain"})}},{{"s",std::make_shared<StringType>()}});
    Check(t,R"(SELECT regexp_replace(s,'^https?://(?:www\.)?([^/]+)/.*$','\1') AS host,count(*) FROM t GROUP BY host ORDER BY host)",{{"example.com","2"},{"plain","1"}});
    Check(t,"SELECT regexp_replace(s,'plain','a replacement longer than the original value') AS r FROM t ORDER BY r LIMIT 1",{{"a replacement longer than the original value"}});
    Check(t,"SELECT min(regexp_replace(s,'plain','a replacement longer than the original value')) FROM t",{{"a replacement longer than the original value"}});
    Check(t,"SELECT regexp_replace('aaa','a','xx') FROM t LIMIT 1",{{"xxaa"}});
    Check(t,"SELECT regexp_replace('a\nb','a.*b','x') FROM t LIMIT 1",{{"x"}});
    EXPECT_FALSE(ParseSelect("SELECT regexp_replace(s,'[','x') FROM t",t).ok());
    EXPECT_FALSE(ParseSelect(R"(SELECT regexp_replace(s,'(a)','\9') FROM t)",t).ok());
}

TEST(Expressions, LazyCaseAndPredicatePrecedence) {
    auto t=test::Table({{test::Raw({0,1,2}),test::Dict<StringType>({"zero","one","two"})}},
                      {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    Check(t,"SELECT CASE WHEN (v=0 AND v+1=1) THEN s ELSE '' END AS k,count(*) FROM t GROUP BY k ORDER BY k",{{"","2"},{"zero","1"}});
    Check(t,"SELECT CASE WHEN v=0 THEN 0 ELSE 1/v END AS n FROM t ORDER BY n",{{"0.000000"},{"0.500000"},{"1.000000"}});
    Check(t,"SELECT CASE WHEN v=0 THEN 1.2 ELSE 3 END AS n FROM t ORDER BY n",{{"1.2"},{"3.0"},{"3.0"}});
    Check(t,"SELECT CASE WHEN s='one' THEN 1 ELSE 0 END AS n FROM t ORDER BY n",{{"0"},{"0"},{"1"}});
    EXPECT_FALSE(ParseSelect("SELECT CASE WHEN v=0 THEN s ELSE 1 END FROM t",t).ok());
    EXPECT_FALSE(ParseSelect("SELECT CASE WHEN v=0 THEN s END FROM t",t).ok());
}

TEST(Expressions, TimestampTruncationOverflowIsExplicit) {
    auto t=test::Table({{test::Raw({INT64_MIN})}},{{"ts",std::make_shared<TimestampType>()}});
    auto q=ParseSelect("SELECT date_trunc('minute',ts) FROM t",t);ASSERT_TRUE(q.ok());
    auto result=ExecuteQuery(*q,true,true);ASSERT_FALSE(result.ok());
    EXPECT_NE(result.status().Message().find("Truncated timestamp"),std::string::npos);
}

TEST(Aggregation, PartitionCandidatesPreserveGlobalOffset) {
    std::vector<std::vector<ColumnDataP>> groups;
    std::map<std::pair<int,int>,int> counts;
    for(int g=0;g<8;++g) {
        std::vector<int64_t> v,w;
        for(int k=0;k<100;++k) for(int r=0;r<(k+g)%5+1;++r) {
            v.push_back(k/3);w.push_back(k%3);++counts[{k/3,k%3}];
        }
        groups.push_back({test::Raw(v),test::Raw(w)});
    }
    auto t=test::Table(groups,{{"v",std::make_shared<Int64Type>()},{"w",std::make_shared<Int64Type>()}});
    std::vector<std::tuple<int,int,int>> ordered;
    for(const auto &[key,count]:counts)if(count>20)ordered.push_back({-count,key.first,key.second});
    std::sort(ordered.begin(),ordered.end());Rows expected;
    for(int i=11;i<18;++i) {auto [negative,v,w]=ordered[i];expected.push_back({std::to_string(v),std::to_string(w),std::to_string(-negative)});}
    Check(t,"SELECT v,w,count(*) AS n FROM t GROUP BY v,w HAVING count(*) > 20 ORDER BY n DESC,v,w LIMIT 7 OFFSET 11",expected);
    std::map<int,std::pair<int,int>> byV;
    for(const auto &[key,count]:counts) {byV[key.first].first+=count;byV[key.first].second+=key.second*count;}
    ordered.clear();expected.clear();Rows sums;
    for(const auto &[v,state]:byV)if(state.first>60)ordered.push_back({-state.first,v,state.second});
    std::sort(ordered.begin(),ordered.end());
    for(int i=4;i<11;++i) {auto [negative,v,sum]=ordered[i];expected.push_back({std::to_string(v),std::to_string(-negative)});sums.push_back({std::to_string(v),std::to_string(sum),std::to_string(-negative)});}
    Check(t,"SELECT v,count(*) AS n FROM t GROUP BY v HAVING count(*) > 60 ORDER BY n DESC,v LIMIT 7 OFFSET 4",expected);
    Check(t,"SELECT v,sum(w),count(*) AS n FROM t GROUP BY v HAVING count(*) > 60 ORDER BY n DESC,v LIMIT 7 OFFSET 4",sums);
}
