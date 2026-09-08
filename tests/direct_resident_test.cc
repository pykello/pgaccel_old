#include "test_table.hpp"
#include "direct_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(DirectResident, ExecutesProjectedColumnsWithExistingKernels) {
    char path[]="/tmp/pgaccel-direct-resident-XXXXXX";int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove(p);std::filesystem::remove(std::string(p)+".metadata");}}cleanup{path};
    auto t=test::Table({{test::Raw({-2,9,4}),test::Dict<StringType>({"unused","values","here"})}},
        {{"v",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    ASSERT_TRUE(t.at("t")->Save(path).ok());
    DirectScanStats io;auto loaded=LoadDirectTable("t",path,{"V"},&io);ASSERT_TRUE(loaded.ok());
    TableRegistry selected;selected.emplace("t",std::move(loaded).ValueUnsafe());
    auto q=ParseSelect("SELECT sum(v) FROM t WHERE v > 0",selected);ASSERT_TRUE(q.ok());
    auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"13"}}));
    EXPECT_EQ(selected.at("t")->ColumnCount(),1);EXPECT_EQ(io.rows,3);EXPECT_GT(io.bytesRead,0);
    EXPECT_FALSE(LoadDirectTable("t",path,{"v","missing"}).ok());
}
