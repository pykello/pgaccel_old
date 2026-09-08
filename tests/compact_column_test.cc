#include "compact_column.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(CompactColumn, RawBoundsWidthsAndInvalidSelections) {
    Int64Type logical;
    for (int width:{1,2,4,8}) {
        auto input=test::Raw({-100,-7,0,3,99},width);
        auto compact=CompactNativeColumn(*input,logical,{1,3}); ASSERT_TRUE(compact.ok());
        auto raw=static_cast<RawColumnData<Int64Type>*>((*compact).get());
        EXPECT_EQ(raw->minValue,-7); EXPECT_EQ(raw->maxValue,3); EXPECT_EQ(raw->size,2);
        std::stringstream saved; ASSERT_TRUE(raw->Save(saved).ok());
        ASSERT_TRUE(ColumnDataBase::Load(saved,&logical).ok());
        EXPECT_FALSE(CompactNativeColumn(*input,logical,{1,1}).ok());
        EXPECT_FALSE(CompactNativeColumn(*input,logical,{3,1}).ok());
        EXPECT_FALSE(CompactNativeColumn(*input,logical,{5}).ok());
        auto empty=CompactNativeColumn(*input,logical,{}); ASSERT_TRUE(empty.ok()); EXPECT_EQ((*empty)->size,0);
    }
}
TEST(CompactColumn, DictionaryPruningAndCodeWidthTransition) {
    std::vector<std::string> labels;
    for(int i=0;i<300;++i)labels.push_back(std::to_string(1000+i));
    auto input=test::Dict<StringType>(labels); StringType logical;
    auto compact=CompactNativeColumn(*input,logical,{1,256,299}); ASSERT_TRUE(compact.ok());
    auto dict=static_cast<DictColumnData<StringType>*>((*compact).get());
    EXPECT_EQ(dict->dict,(std::vector<std::string>{"1001","1256","1299"}));
    EXPECT_EQ(dict->bytesPerValue(),1); EXPECT_EQ(dict->values[2],2);
    EXPECT_EQ(static_cast<DictColumnDataBase*>(input.get())->dictSize(),300);
    EXPECT_EQ(dict->globalDictionary,nullptr); EXPECT_TRUE(dict->globalIds.empty());
    std::stringstream saved; ASSERT_TRUE(dict->Save(saved).ok());
    ASSERT_TRUE(ColumnDataBase::Load(saved,&logical).ok());
    DateType date; auto days=test::Dict<DateType>({2,1,2,3});
    auto dates=CompactNativeColumn(*days,date,{0,2});ASSERT_TRUE(dates.ok());
    EXPECT_EQ(static_cast<DictColumnData<DateType>*>((*dates).get())->dict,(std::vector<DateType::c_type>{2}));
}
TEST(CompactColumn, DirectSelectionPreservesCrossColumnRowsAndEmptySchema) {
    char directory[]="/tmp/pgaccel-compact-XXXXXX"; ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup { const char *p; ~Cleanup(){std::filesystem::remove_all(p);} } cleanup{directory};
    auto table=test::Table({{test::Raw({1,2,3}),test::Dict<StringType>({"a","b","a"})},
                            {test::Raw({4,5}),test::Dict<StringType>({"c","b"})}},
                           {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    std::string path=std::string(directory)+"/t.pgaccel";
    ASSERT_TRUE(table.at("t")->Save(path).ok());
    for (bool empty:{false,true}) {
        DirectScanStats io;
        auto loaded=ColumnarTable::LoadDirectSelected("t",path,{"k","s"},
            [&](const auto &,const RowGroup &g)->Result<std::vector<uint16_t>> {
                if(empty)return std::vector<uint16_t>{};
                return std::vector<uint16_t>{uint16_t(g.size-1)};
            },&io);
        ASSERT_TRUE(loaded.ok())<<loaded.status().Message();
        EXPECT_EQ(io.rows,5);EXPECT_GT(io.bytesRead,0);
        TableRegistry registry;registry.emplace("t",std::move(loaded).ValueUnsafe());
        auto query=ParseSelect("SELECT k,s FROM t ORDER BY k",registry);ASSERT_TRUE(query.ok());
        auto result=ExecuteQuery(*query,true,true);ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values,empty?Rows{}:Rows({{"3","a"},{"5","b"}}));
    }
}
