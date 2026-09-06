#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(CompactIn, RawWidthsSignedEndpointsTailsAndIntersection) {
    for(auto [first,width]:std::vector<std::pair<int64_t,int>>{{-128,1},{-32768,2},{INT32_MIN,4},{INT64_MIN,8},{INT64_MAX-100,8}}) {
        std::vector<int64_t> values;for(int i=0;i<259;++i)values.push_back(first+i%100);
        if(width==8)values.push_back(first<0?INT64_MAX:INT64_MIN);
        auto registry=test::Table({{test::Raw(values,width)}},{{"v",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT v FROM t",registry);ASSERT_TRUE(q.ok());const auto &rg=q->tables[0]->GetRowGroup(0);
        for(bool negate:{false,true})for(bool avx:{false,true})for(bool wide:{false,true})for(bool empty:{false,true}) {
            std::vector<int64_t> members=empty?std::vector<int64_t>{}:std::vector<int64_t>{first+3,first+5,first+(wide?90:63),first+5};
            FilterClause f;f.columnRef=*q->aggregateClauses[0].columnRef;f.op=negate?FilterClause::FILTER_NOT_IN:FilterClause::FILTER_IN;
            for(auto value:members)f.values.push_back(std::to_string(value));auto filter=CreateFilterNode({f},avx);
            for(bool intersect:{false,true}) {
                uint8_t bitmap[RowGroupSize/8];std::memset(bitmap,0xa5,sizeof(bitmap));int expectedCount=0;
                std::vector<bool> expected;
                for(size_t i=0;i<values.size();++i) {
                    bool hit=std::find(members.begin(),members.end(),values[i])!=members.end();
                    bool keep=(!intersect || (bitmap[i/8]>>(i%8)&1)) && (negate?!hit:hit);
                    expected.push_back(keep);expectedCount+=keep;
                }
                int count=intersect?filter->ExecuteAnd(rg,bitmap):filter->ExecuteSet(rg,bitmap);
                EXPECT_EQ(count,expectedCount);
                for(size_t i=0;i<values.size();++i)EXPECT_EQ(bool(bitmap[i/8]>>(i%8)&1),expected[i]);
                if(!intersect)EXPECT_EQ(filter->ExecuteCount(rg),count);
            }
        }
    }
}
