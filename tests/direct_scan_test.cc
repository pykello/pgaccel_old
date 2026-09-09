#include "test_table.hpp"
#include "direct_input.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>
using namespace pgaccel;
TEST(DirectScan, ProjectionOwnedBatchesAndEarlyStop) {
    char path[]="/tmp/pgaccel-scan-XXXXXX";int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);std::filesystem::remove(std::string(p)+".metadata");}} cleanup{path};
    auto t=test::Table({{test::Raw({1,2,3}),test::Dict<StringType>({"a","b","a"})},
                        {test::Raw({4,5}),test::Dict<StringType>({"c","d"})}},
                       {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    for(bool aligned:{false,true}) {
        ASSERT_TRUE(t.at("t")->Save(path,aligned).ok());
        int calls=0;ColumnDataP retained;
        auto result=ColumnarTable::ScanDirect(path,{"V"},[&](const auto &schema,const RowGroup &batch)->Result<bool>{
            EXPECT_EQ(schema.size(),1);EXPECT_EQ(schema[0].name,"v");EXPECT_EQ(batch.columns.size(),1);
            if(!calls)retained=batch.columns[0];++calls;return true;
        });
        ASSERT_TRUE(result.ok())<<result.status().Message();EXPECT_EQ(result->rows,5);EXPECT_EQ(calls,2);
        EXPECT_EQ(result->ioBufferBytes,DirectInput::BufferBytes+DirectInput::HeaderBytes);EXPECT_GT(result->bytesRead,0);
        EXPECT_EQ(reinterpret_cast<RawColumnDataBase *>(retained.get())->values[0],1);
        auto early=ColumnarTable::ScanDirect(path,{"s"},[](const auto &,const auto &)->Result<bool>{return false;});
        ASSERT_TRUE(early.ok());EXPECT_EQ(early->groups,1);EXPECT_EQ(early->rows,3);
        EXPECT_FALSE(ColumnarTable::ScanDirect(path,{"missing"},[](const auto &,const auto &)->Result<bool>{return true;}).ok());
        EXPECT_FALSE(ColumnarTable::ScanDirect(path,{"v"},[](const auto &,const auto &)->Result<bool>{return Status::Invalid("consumer failed");}).ok());
    }
}

TEST(DirectScan, HeaderModeRejectsCorruptAndTruncatedIdentity) {
    char path[]="/tmp/pgaccel-header-invalid-XXXXXX";
    int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);std::filesystem::remove(std::string(p)+".metadata");}} cleanup{path};
    auto table=test::Table({{test::Raw({1,2,3})}},{{"v",std::make_shared<Int64Type>()}});
    ASSERT_TRUE(table.at("t")->Save(path,true).ok());
    ASSERT_TRUE(ColumnarTable::LoadSchema("t",path).ok());
    {std::fstream data(path,std::ios::binary|std::ios::in|std::ios::out);data.put('X');}
    EXPECT_FALSE(ColumnarTable::LoadSchema("t",path).ok());
    ASSERT_TRUE(table.at("t")->Save(path,true).ok());
    std::filesystem::resize_file(path,12);
    EXPECT_FALSE(ColumnarTable::LoadSchema("t",path).ok());
    EXPECT_FALSE(ColumnarTable::ScanDirect(path,{"v"},
        [](const auto &,const auto &)->Result<bool>{return true;}).ok());
}
