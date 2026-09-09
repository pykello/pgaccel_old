#include "test_table.hpp"
#include "executor_arithmetic_program.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(ArithmeticGather, SignedExtremesOffsetsRepeatedIndicesAndTails) {
    for(int width:{4,8}) {
        std::vector<int64_t> values;
        for(int i=0;i<65;++i)values.push_back(i%2?(width==4?INT32_MAX:INT64_MAX):(width==4?INT32_MIN:INT64_MIN));
        auto data=test::Raw(values,width);auto raw=static_cast<const RawColumnDataBase *>(data.get());
        uint32_t selected[25];for(int i=0;i<25;++i)selected[i]=(i*11)%7;
        for(bool avx:{false,true})for(int n:{0,1,7,8,9,15,16,25}) {
            std::array<int64_t,27> output;output.fill(17);
            if(width==4)DecodeNumbers<int32_t,true,true>(raw,3,n,output.data()+1,avx,selected);
            else DecodeNumbers<int64_t,true,true>(raw,3,n,output.data()+1,avx,selected);
            EXPECT_EQ(output.front(),17);
            for(int i=0;i<n;++i)EXPECT_EQ(output[i+1],values[3+selected[i]]);
            for(int i=n+1;i<27;++i)EXPECT_EQ(output[i],17);
        }
    }
}
TEST(DenseArithmetic, DecimalExpressionsAcrossStripesAndSelections) {
    auto decimal=std::make_shared<DecimalType>(); decimal->scale=2;
    std::vector<std::vector<ColumnDataP>> groups;
    for(int width:{1,2,4,8}) {
        std::vector<std::string> a,b; std::vector<int64_t> x,y;
        for(int i=0;i<269;++i) {a.push_back(i%2?"z":"a");b.push_back(i%3?"q":"p");x.push_back(i%17-8);y.push_back(i%5);}
        groups.push_back({test::Dict<StringType>(a),test::Dict<StringType>(b),test::Raw(x,width),test::Raw(y,width)});
    }
    auto registry=test::Table(std::move(groups),{{"a",std::make_shared<StringType>()},
        {"b",std::make_shared<StringType>()},{"x",decimal},{"y",decimal}});
    for(const std::string where:{"", " WHERE x<0.00", " WHERE x>99.00"}) {
        auto q=ParseSelect("SELECT a,b,sum(x),avg(x),sum(x*(1-y)),sum(x*(1-y)*(1+y)),count(*) FROM t"+where+" GROUP BY a,b ORDER BY a,b",registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();
        // A mathematically neutral division keeps the independent interpreter.
        auto reference=ParseSelect("SELECT a,b,sum(x),avg(x),sum((x*(1-y))/1),sum((x*(1-y)*(1+y))/1),count(*) FROM t"+where+" GROUP BY a,b ORDER BY a,b",registry);
        ASSERT_TRUE(reference.ok())<<reference.status().Message();
        auto expected=ExecuteQuery(*reference,false,false); ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true}) for(bool parallel:{false,true}) {
            auto result=ExecuteQuery(*q,avx,parallel); ASSERT_TRUE(result.ok())<<result.status().Message();
            ASSERT_EQ(result->values.size(),expected->values.size());
            for(size_t r=0;r<result->values.size();++r) for(size_t c=0;c<7;++c) {
                if(c==4 || c==5) EXPECT_EQ(std::stold(result->values[r][c]),std::stold(expected->values[r][c]));
                else EXPECT_EQ(result->values[r][c],expected->values[r][c]);
            }
        }
    }
}
TEST(DenseArithmetic, WideFallbackAndBudget) {
    auto registry=test::Table({{test::Dict<StringType>({"a","a"}),test::Raw({INT64_MAX,INT64_MAX})}},
        {{"k",std::make_shared<StringType>()},{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(v),sum(v*v),count(*) FROM t GROUP BY k",registry);
    ASSERT_TRUE(q.ok()); auto result=ExecuteQuery(*q,true,true);
    ASSERT_TRUE(result.ok())<<result.status().Message();
    EXPECT_EQ(result->values,(Rows{{"a","18446744073709551614","170141183460469231694793815568465002498","2"}}));
    QueryDesc limited=*q; limited.memoryBudgetBytes=1; limited.allowSpill=false;
    EXPECT_FALSE(ExecuteQuery(limited,true,true).ok());
}
TEST(DenseArithmetic, SparseBlocksAndDifferentLocalDictionaryIds) {
    std::vector<std::vector<ColumnDataP>> groups;
    for(const std::string other:{"z","a"}) {
        std::vector<std::string> keys(269,"m");
        std::fill(keys.begin()+256,keys.end(),other);
        groups.push_back({test::Dict<StringType>(keys),test::Raw(std::vector<int64_t>(269,3),1)});
    }
    auto registry=test::Table(std::move(groups),{{"k",std::make_shared<StringType>()},
        {"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(v),avg(v),count(*) FROM t GROUP BY k ORDER BY k",registry);
    ASSERT_TRUE(q.ok());
    for(bool avx:{false,true}) for(bool parallel:{false,true}) {
        auto result=ExecuteQuery(*q,avx,parallel); ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values,(Rows{{"a","39","3","13"},{"m","1536","3","512"},{"z","39","3","13"}}));
    }
}
TEST(DenseArithmetic, SignedMultiplyWidthBoundary) {
    std::vector<std::vector<ColumnDataP>> groups;
    for(bool wide:{false,true}) {
        std::vector<std::string> k; std::vector<int64_t> x,y;
        for(int i=0;i<8;++i) {
            k.insert(k.end(),{"a","b"});
            x.insert(x.end(),{wide?2147483648LL:INT32_MIN,wide?4294967296LL:INT32_MAX});
            y.insert(y.end(),{wide?3:-1,wide?-2:2});
        }
        groups.push_back({test::Dict<StringType>(k),test::Raw(x),test::Raw(y)});
    }
    auto registry=test::Table(std::move(groups),
        {{"k",std::make_shared<StringType>()},{"x",std::make_shared<Int64Type>()},{"y",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(x*y) FROM t GROUP BY k ORDER BY k",registry);
    ASSERT_TRUE(q.ok());
    for(bool avx:{false,true}) for(bool parallel:{false,true}) {
        auto result=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values,(Rows{{"a","68719476736"},{"b","-34359738384"}}));
    }
}
