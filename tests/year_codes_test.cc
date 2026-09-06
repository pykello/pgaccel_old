#include "test_table.hpp"
#include "executor_year_codes.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(YearCodes, StripeLocalDictionariesAndSignedRawDatesAgree) {
    std::vector<int32_t> dates;for(int i=-400;i<400;++i)dates.push_back(i);
    std::vector<int32_t> reverse=dates;std::reverse(reverse.begin(),reverse.end());
    auto raw=std::make_shared<RawColumnData<DateType>>();
    raw->type=ColumnDataBase::RAW_COLUMN_DATA;raw->size=5;raw->bytesPerValue=4;
    raw->minValue=-366;raw->maxValue=365;raw->values=(uint8_t*)AllocateAligned(512,20);
    int32_t rawDays[5]={-366,-1,0,364,365};std::memcpy(raw->values,rawDays,20);
    auto tables=test::Table({{test::Dict<DateType>(dates)}, {test::Dict<DateType>(reverse)},
                             {test::Dict<DateType>({-366,-1,0,364,365})}},{{"day",std::make_shared<DateType>()}});
    auto q=ParseSelect("SELECT EXTRACT(year FROM day) FROM t",tables);ASSERT_TRUE(q.ok());
    auto column=*q->aggregateClauses[0].expression->left->column;
    Budget budget(1<<20);YearCodeDomain domain(*tables.at("t"),column);
    EXPECT_EQ(domain.first,1968);EXPECT_EQ(domain.last,1971);
    for(int g=0;g<3;++g) {
        const auto &rg=tables.at("t")->GetRowGroup(g);YearCodeStripe codes(domain,rg,column,budget);
        for(int row=0;row<rg.size;++row)
            EXPECT_EQ(codes.get(row),Evaluate(*q->aggregateClauses[0].expression,rg,row).number-domain.first+1);
    }
    RowGroup rawGroup;rawGroup.size=5;rawGroup.columns={raw};
    YearCodeStripe rawCodes(domain,rawGroup,column,budget);
    for(int row=0;row<5;++row)EXPECT_EQ(rawCodes.get(row),domain.encode(rawDays[row]));
}
TEST(YearCodes, RejectsWideDomainsAndOutOfRangeDays) {
    auto tables=test::Table({{test::Dict<DateType>({0,365*65})}},{{"day",std::make_shared<DateType>()}});
    auto q=ParseSelect("SELECT day FROM t",tables);ASSERT_TRUE(q.ok());
    auto column=*q->aggregateClauses[0].expression->column;
    EXPECT_THROW(YearCodeDomain(*tables.at("t"),column),NotDense);
    EXPECT_THROW(YearCodeDomain::year(INT64_MAX),NotDense);
    EXPECT_EQ(YearCodeDomain::year(-719528),0);
    EXPECT_EQ(YearCodeDomain::year(2932896),9999);
}
