#include "test_table.hpp"
#include "executor_boolean_bitmap.hpp"
#include "executor_buffer.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(BooleanBitmap, SharedPredicatesDecimalBoundsAndPartialWords) {
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    for(int n:{1,17,65,513}) {
        std::vector<int64_t> numbers;std::vector<std::string> labels;
        for(int i=0;i<n;++i){numbers.push_back((i%40)*100);labels.push_back(i%3==0?"x":i%3==1?"y":"z");}
        auto registry=test::Table({{test::Raw(numbers,2),test::Dict<StringType>(labels)}},{{"v",decimal},{"s",std::make_shared<StringType>()}});
        auto q=ParseSelect("SELECT count(*) FROM t WHERE (v>=1 AND v<=1+10 AND (s='x' OR s='y')) OR (v>=10 AND v<=20 AND s='x')",registry);
        ASSERT_TRUE(q.ok());ASSERT_EQ(q->predicates.size(),1);
        for(bool avx:{false,true}) {
            Budget budget(1<<20);BooleanBitmapProgram program(budget,avx);
            int root=program.compile(*q->predicates.front(),0);auto bytes=program.bytes();
            int duplicate=program.compile(*q->predicates.front(),0);EXPECT_EQ(root,duplicate);EXPECT_EQ(bytes,program.bytes());
            OverwriteBuffer<uint64_t> scratch((bytes+7)/8,budget);std::array<int,64> counts{};
            std::memset(scratch.data(),0xff,scratch.size()*8);
            program.evaluate(registry.at("t")->GetRowGroup(0),reinterpret_cast<uint8_t *>(scratch.data()),counts);
            const auto bits=program.bitmap(reinterpret_cast<uint8_t *>(scratch.data()),root);int expected=0;
            for(int i=0;i<n;++i) {
                bool keep=(numbers[i]>=100 && numbers[i]<=1100 && (labels[i]=="x" || labels[i]=="y")) || (numbers[i]>=1000 && numbers[i]<=2000 && labels[i]=="x");
                EXPECT_EQ(bool(bits[i/8]>>(i%8)&1),keep);expected+=keep;
            }
            EXPECT_EQ(counts[root],expected);
            for(int i=n;i<((n+63)/64)*64;++i)EXPECT_EQ(bits[i/8]>>(i%8)&1,0);
        }
    }
}
TEST(BooleanBitmap, UnrepresentableIntegerBoundaryKeepsFallback) {
    auto registry=test::Table({{test::Raw({0,1,2})}},{{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT count(*) FROM t WHERE v<1.5 OR v>2",registry);ASSERT_TRUE(q.ok());
    Budget budget(1<<20);BooleanBitmapProgram program(budget,true);
    EXPECT_THROW(program.compile(*q->predicates.front(),0),NotDense);
}

TEST(BooleanBitmap, DeepConstantExpressionsKeepGeneralFallback) {
    auto registry=test::Table({{test::Raw({0,1,2})}},{{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT count(*) FROM t WHERE v<3 OR v>4",registry);ASSERT_TRUE(q.ok());
    auto predicate=q->predicates.front()->left;
    auto one=std::make_shared<Expression>();one->kind=Expression::CONSTANT;one->literal="1";one->type=std::make_shared<Int64Type>();
    auto value=one;
    for(int i=0;i<70;++i) {auto next=std::make_shared<Expression>();next->kind=Expression::ADD;next->left=value;next->right=one;next->type=one->type;value=next;}
    predicate->right=value;
    Budget budget(1<<20);BooleanBitmapProgram program(budget,true);
    EXPECT_THROW(program.compile(*predicate,0),NotDense);
}
TEST(BooleanBitmap, CompactEqualityDisjunctionUsesOneExactBitmap) {
    std::vector<int64_t> values;for(int i=0;i<259;++i)values.push_back(-50+i%100);
    auto registry=test::Table({{test::Raw(values,1)}},{{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT count(*) FROM t WHERE v=-17 OR v=-3 OR v=0 OR v=3 OR v=17 OR v=3",registry);ASSERT_TRUE(q.ok());
    for(bool avx:{false,true}) {
        Budget budget(1<<20);BooleanBitmapProgram program(budget,avx);int root=program.compile(*q->predicates[0],0);
        EXPECT_EQ(program.bytes(),RowGroupSize/8);
        OverwriteBuffer<uint64_t> scratch((program.bytes()+7)/8,budget);std::array<int,64> counts;
        auto bytes=reinterpret_cast<uint8_t *>(scratch.data());program.evaluate(registry.at("t")->GetRowGroup(0),bytes,counts);
        auto bits=program.bitmap(bytes,root);int expected=0;
        for(size_t i=0;i<values.size();++i) {
            bool keep=values[i]==-17 || values[i]==-3 || values[i]==0 || values[i]==3 || values[i]==17;
            EXPECT_EQ(bool(bits[i/8]>>(i%8)&1),keep);expected+=keep;
        }
        EXPECT_EQ(counts[root],expected);
    }
}
