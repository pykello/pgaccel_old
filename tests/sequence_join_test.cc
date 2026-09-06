#include "test_table.hpp"
#include "executor_sequence_join.hpp"
#include "executor_state.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(SequenceJoin, VerifyAllWidthsTailsAndSignedExtremes) {
    for(int width:{1,2,4,8}) for(bool avx:{false,true}) {
        int64_t start=width==8?INT64_MAX-36:-16;
        std::vector<std::vector<ColumnDataP>> stripes;
        for(int begin=0;begin<37;begin+=16) {
            std::vector<int64_t> values;
            for(int i=begin;i<std::min(37,begin+16);++i) values.push_back(start+i);
            stripes.push_back({test::Raw(values,width)});
        }
        auto registry=test::Table(std::move(stripes),{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());
        auto column=*q->aggregateClauses[0].columnRef;
        SequenceJoinIndex index;
        EXPECT_FALSE(index.prepare(*q->tables[0],column,36,avx));
        ASSERT_TRUE(index.prepare(*q->tables[0],column,37,avx));
        for(int i=0;i<37;++i) EXPECT_EQ(index.lookup(start+i),std::optional<uint64_t>((uint64_t(i/16)<<32)|(i%16)));
        EXPECT_FALSE(index.lookup(start-1));
        if(width!=8) EXPECT_FALSE(index.lookup(start+37));
    }
}
TEST(SequenceJoin, RejectDuplicateReorderedSparseAndIrregularInputs) {
    for(const std::vector<int64_t> values:std::vector<std::vector<int64_t>>{
        {1,2,3,4,5,6,7,9,9},{1,2,3,4,5,6,8,7,9},{1,2,3,4,5,6,7,8,10},
        {INT64_MAX,INT64_MIN}}) {
        auto registry=test::Table({{test::Raw(values)}},{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());
        for(bool avx:{false,true}) {
            SequenceJoinIndex index;
            EXPECT_FALSE(index.prepare(*q->tables[0],*q->aggregateClauses[0].columnRef,values.size(),avx));
            EXPECT_FALSE(index.active());
        }
    }
    auto registry=test::Table({{test::Raw({1,2})},{test::Raw({3,4,5})}},{{"k",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());
    SequenceJoinIndex index;
    EXPECT_FALSE(index.prepare(*q->tables[0],*q->aggregateClauses[0].columnRef,5,true));
}
TEST(SequenceJoin, JoinKeepsDuplicateProbeRowsAndAdditionalPredicates) {
    auto registry=test::Table({{test::Raw({-1,0,1,2,3,4,5,6}),test::Raw({1,2,3,4,5,6,7,8})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto other=test::Table({{test::Raw({-2,-1,-1,0,1,2,3,4,5,6,7,8})}},{{"k",std::make_shared<Int64Type>()}});
    registry.emplace("u",std::move(other.at("t")));
    for(const auto &[sql,expected]:std::vector<std::pair<std::string,Rows>>{
        {"SELECT count(*),sum(t.v) FROM t,u WHERE t.k=u.k",{{"9","37"}}},
        {"SELECT count(*) FROM t,u WHERE t.k=u.k AND t.v>3",{{"5"}}},
        {"SELECT count(*) FROM u WHERE NOT EXISTS (SELECT * FROM t WHERE t.k=u.k)",{{"3"}}}}) {
        auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok())<<q.status().Message();
        for(bool avx:{false,true}) {
            auto result=ExecuteQuery(*q,avx,true);ASSERT_TRUE(result.ok())<<result.status().Message();
            EXPECT_EQ(result->values,expected);
        }
    }
}
TEST(SequenceJoin, DirectoryMapsStripesStoredOutOfKeyOrder) {
    auto registry=test::Table({{test::Raw({5})},{test::Raw({1,2})},{test::Raw({3,4})}},
        {{"k",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());
    for(bool avx:{false,true}) {
        Budget budget(4096);SequenceJoinIndex index(&budget);
        ASSERT_TRUE(index.prepare(*q->tables[0],*q->aggregateClauses[0].columnRef,5,avx));
        EXPECT_EQ(index.lookup(1),std::optional<uint64_t>(uint64_t(1)<<32));
        EXPECT_EQ(index.lookup(4),std::optional<uint64_t>((uint64_t(2)<<32)|1));
        EXPECT_EQ(index.lookup(5),std::optional<uint64_t>(0));
        EXPECT_GT(budget.peak.load(),0);
    }
}
