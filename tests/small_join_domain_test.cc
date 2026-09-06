#include "test_table.hpp"
#include "../src/executor_small_join_domain.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel { Result<QueryOutput> ExecuteRelational(const QueryDesc &, bool, bool); }

TEST(SmallJoinDomain, SignedExtremesDuplicatesAndBoundedDiscovery) {
    auto integer=std::make_shared<Int64Type>();
    for(int scenario=0;scenario<5;++scenario) for(int width:{1,2,4,8}) {
        if(scenario>=2 && width!=8)continue;
        int64_t first=scenario==0?0:scenario==1?-20:scenario==4?INT64_MAX-20:INT64_MIN;
        int64_t last=scenario==3?INT64_MAX:first+20;
        std::vector<int64_t> values;
        for(int i=0;i<513;++i)values.push_back(i%3==0?first:i%3==1?last:first+1);
        if(scenario==4)values[0]=INT64_MIN;
        std::vector<int64_t> firstStripe(values.begin(),values.begin()+257),secondStripe(values.begin()+257,values.end());
        auto registry=test::Table({{test::Raw(firstStripe,width)},{test::Raw(secondStripe,width)}},{{"k",integer}});
        auto dimension=test::Table({{test::Raw({first,first,last},width)}},{{"k",integer}});
        auto rowId=[](size_t i){return i<257?uint64_t(i):(uint64_t(1)<<32)|(i-257);};
        registry.emplace("d",std::move(dimension.at("t")));
        auto q=ParseSelect("SELECT count(*) FROM t,d WHERE t.k=d.k",registry);ASSERT_TRUE(q.ok());
        std::vector<std::pmr::vector<uint64_t>> rows(2);
        for(size_t i=0;i<values.size();++i)rows[0].push_back(rowId(i));
        rows[1]={0,1,2};auto original=rows;
        Budget small(512);FilterSmallJoinDomains(*q,rows,small);EXPECT_EQ(rows,original);
        Budget budget(1<<20);FilterSmallJoinDomains(*q,rows,budget);
        std::vector<uint64_t> expected;
        for(size_t i=0;i<values.size();++i)if(values[i]==first||values[i]==last)expected.push_back(rowId(i));
        EXPECT_EQ(std::vector<uint64_t>(rows[0].begin(),rows[0].end()),expected);
        EXPECT_EQ(rows[1],original[1]);EXPECT_EQ(budget.used.load(),0);
        auto actual=ExecuteRelational(*q,true,true);ASSERT_TRUE(actual.ok())<<actual.status().Message();
        int count=0;for(auto value:values)count+=(value==first)*2+(value==last);
        EXPECT_EQ(actual->values,Rows({{std::to_string(count)}}));
    }
}

TEST(SmallJoinDomain, FilteredDimensionsPropagateAlongRequiredEqualities) {
    auto integer=std::make_shared<Int64Type>();
    std::vector<int64_t> keys;for(int i=0;i<1027;++i)keys.push_back(i%25);
    auto registry=test::Table({{test::Raw(keys,4)}},{{"k",integer}});
    std::vector<int64_t> nation,region;for(int i=0;i<25;++i){nation.push_back(i);region.push_back(i%5);}
    auto d=test::Table({{test::Raw(nation,4),test::Raw(region,4)}},{{"k",integer},{"region",integer}});
    registry.emplace("d",std::move(d.at("t")));
    auto r=test::Table({{test::Raw({0,1,2,3,4},4)}},{{"k",integer}});registry.emplace("r",std::move(r.at("t")));
    auto q=ParseSelect("SELECT count(*) FROM t,d,r WHERE t.k=d.k AND d.region=r.k AND r.k=2",registry);
    ASSERT_TRUE(q.ok());auto actual=ExecuteRelational(*q,true,true);ASSERT_TRUE(actual.ok())<<actual.status().Message();
    int expected=0;for(auto key:keys)expected+=key%5==2;
    EXPECT_EQ(actual->values,Rows({{std::to_string(expected)}}));
    auto disjunction=ParseSelect("SELECT count(*) FROM t,d WHERE t.k=d.k OR d.k=0",registry);
    ASSERT_TRUE(disjunction.ok());
    std::vector<std::pmr::vector<uint64_t>> rows(2);
    for(size_t i=0;i<keys.size();++i)rows[0].push_back(i);
    for(size_t i=0;i<nation.size();++i)rows[1].push_back(i);
    auto original=rows;Budget budget(1<<20);
    FilterSmallJoinDomains(*disjunction,rows,budget);EXPECT_EQ(rows,original);
}

TEST(SmallJoinDomain, EmptyBuildsFullDomainsAndLeftJoinsPreserveSemantics) {
    auto integer=std::make_shared<Int64Type>();
    std::vector<int64_t> keys;for(int i=0;i<129;++i)keys.push_back(i%4);
    auto registry=test::Table({{test::Raw(keys,4)}},{{"k",integer}});
    auto d=test::Table({{test::Raw({0,1,2,3},4)}},{{"k",integer}});registry.emplace("d",std::move(d.at("t")));
    auto full=ParseSelect("SELECT count(*) FROM t,d WHERE t.k=d.k",registry);ASSERT_TRUE(full.ok());
    auto actual=ExecuteRelational(*full,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,Rows({{"129"}}));
    auto empty=ParseSelect("SELECT count(*) FROM t,d WHERE t.k=d.k AND d.k<0",registry);ASSERT_TRUE(empty.ok());
    actual=ExecuteRelational(*empty,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,Rows({{"0"}}));
    auto left=ParseSelect("SELECT count(*) FROM t LEFT JOIN d ON t.k=d.k AND d.k<0",registry);ASSERT_TRUE(left.ok());
    actual=ExecuteRelational(*left,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,Rows({{"129"}}));
}
