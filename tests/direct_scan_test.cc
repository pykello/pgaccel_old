#include "test_table.hpp"
#include "direct_input.hpp"
#include <gtest/gtest.h>
#include <chrono>
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

TEST(DirectScan, RecycledStringsDoNotChangeRetainedColumns) {
    char path[]="/tmp/pgaccel-string-reuse-XXXXXX";
    int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);std::filesystem::remove(std::string(p)+".metadata");}} cleanup{path};
    std::vector<std::vector<std::string>> labels{
        {std::string(90,'a'),std::string(90,'b')},
        {std::string(80,'c'),std::string(80,'d'),std::string(80,'e')},
        {std::string(70,'f'),std::string(70,'g')},
        {std::string(150,'h')}};
    std::vector<std::vector<ColumnDataP>> groups;
    for(const auto &values:labels)groups.push_back({test::Dict<StringType>(values)});
    auto table=test::Table(std::move(groups),{{"s",std::make_shared<StringType>()}});
    for(bool aligned:{false,true}) {
        ASSERT_TRUE(table.at("t")->Save(path,aligned).ok());
        ColumnDataP retained;int group=0;
        auto result=ColumnarTable::ScanDirect(path,{"s"},
            [&](const auto &,const RowGroup &batch)->Result<bool>{
                auto &column=static_cast<const DictColumnData<StringType>&>(*batch.columns[0]);
                EXPECT_EQ(column.dict,labels[group]);
                if(group==0)retained=batch.columns[0];
                ++group;return true;
            });
        ASSERT_TRUE(result.ok());EXPECT_EQ(group,labels.size());
        ASSERT_TRUE(retained);
        EXPECT_EQ(static_cast<const DictColumnData<StringType>&>(*retained).dict,labels[0]);
    }
}

TEST(DirectScan, CacheIdentityRequiresValidatedVersionedDirectInput) {
    char path[]="/tmp/pgaccel-cache-identity-XXXXXX";
    int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);std::filesystem::remove(std::string(p)+".metadata");}} cleanup{path};
    auto table=test::Table({{test::Raw({1,2,3})}},{{"v",std::make_shared<Int64Type>()}});
    ASSERT_TRUE(table.at("t")->Save(path,true).ok());
    auto first=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(first.ok());
    ASSERT_TRUE((*first)->SnapshotIdentity());auto identity=*(*first)->SnapshotIdentity();
    auto again=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(again.ok());
    EXPECT_EQ((*again)->SnapshotIdentity(),identity);
    auto time=std::filesystem::last_write_time(path);
    std::filesystem::last_write_time(path,time+std::chrono::seconds(1));
    auto touched=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(touched.ok());
    ASSERT_TRUE((*touched)->SnapshotIdentity());
    EXPECT_EQ((*touched)->SnapshotIdentity()->uuid,identity.uuid);
    EXPECT_NE((*touched)->SnapshotIdentity()->file,identity.file);
    ASSERT_TRUE(table.at("t")->Save(path,true).ok());
    auto saved=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(saved.ok());
    EXPECT_NE((*saved)->SnapshotIdentity()->uuid,identity.uuid);
    {std::fstream data(path,std::ios::binary|std::ios::in|std::ios::out);data.put('X');}
    EXPECT_FALSE(ColumnarTable::LoadSchema("t",path).ok());
    {std::ofstream data(path,std::ios::binary),meta(std::string(path)+".metadata");
     meta<<"1\n0 1 v "<<INT64_TYPE<<'\n';ASSERT_TRUE(test::Raw({4})->Save(data).ok());}
    auto legacy=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(legacy.ok());
    EXPECT_FALSE((*legacy)->SnapshotIdentity());
}
