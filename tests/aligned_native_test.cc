#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(AlignedNative, RoundTripWidthsDictionariesAndTail) {
    for (int width : {1,2,4,8}) {
        auto t=test::Table({{test::Raw({-7,2,11},width),test::Dict<StringType>({"abc","z","abc"}),
                            test::Dict<DateType>({1,2,3})}},
            {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()},
             {"d",std::make_shared<DateType>()}});
        std::stringstream metadata,data;
        ASSERT_TRUE(t.at("t")->Save(metadata,data,true).ok());
        EXPECT_EQ(metadata.str().find("PGACCEL_SNAPSHOT 2 "),0);
        EXPECT_EQ(data.str().size()%512,0);
        auto loaded=ColumnarTable::Load("t",metadata,data);ASSERT_TRUE(loaded.ok());
        std::stringstream metadata2,data2;
        ASSERT_TRUE((*loaded)->Save(metadata2,data2).ok());
        auto copy=ColumnarTable::Load("t",metadata2,data2);ASSERT_TRUE(copy.ok());
        auto raw=static_cast<const RawColumnDataBase *>((*copy)->GetRowGroup(0).columns[0].get());
        EXPECT_EQ(raw->bytesPerValue,width);
        EXPECT_EQ((*copy)->GetRowGroup(0).columns[1]->size,3);
        auto truncated=data.str();truncated.pop_back();
        std::stringstream bad(truncated),meta(metadata.str());
        EXPECT_FALSE(ColumnarTable::Load("t",meta,bad).ok());
    }
}
