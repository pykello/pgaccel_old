#include <cstring>
#include "../benchmarks/json_rows.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(JsonRows, EscapesControlsQuotesBackslashesAndPreservesUtf8) {
    EXPECT_EQ(benchmark::JsonRows({}),"[]");
    EXPECT_EQ(benchmark::JsonRows({{}, {"", "quote\"slash\\", "caf\xc3\xa9", std::string("a\0b",3), "\n\r\t"}}),
        "[[],[\"\",\"quote\\\"slash\\\\\",\"caf\xc3\xa9\",\"a\\u0000b\",\"\\u000a\\u000d\\u0009\"]]");
    std::string bytes,expected="\"";
    for(unsigned i=0;i<32;++i)bytes+=char(i);
    expected+="\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007";
    expected+="\\u0008\\u0009\\u000a\\u000b\\u000c\\u000d\\u000e\\u000f";
    expected+="\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017";
    expected+="\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f\"";
    std::string actual;benchmark::AppendJsonString(actual,bytes);EXPECT_EQ(actual,expected);
    std::string plain(100000,'x');EXPECT_EQ(benchmark::JsonRows({{plain}}),"[[\""+plain+"\"]]");
}
#include <sys/mman.h>
#include <unistd.h>
TEST(JsonRows, VectorEscapeProbesStopAtProtectedPageBoundaries) {
    size_t page=sysconf(_SC_PAGESIZE);auto memory=static_cast<char *>(mmap(nullptr,page*2,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0));
    ASSERT_NE(memory,MAP_FAILED);ASSERT_EQ(mprotect(memory+page,page,PROT_NONE),0);
    for(size_t n=1;n<=96;++n)for(char special:{char(0),char(31),'"','\\',char(0xc3)}) {
        auto begin=memory+page-n;std::memset(begin,'a',n);begin[n-1]=special;
        std::string actual;benchmark::AppendJsonString(actual,std::string_view(begin,n));
        std::string expected="\""+std::string(n-1,'a');
        if(special==0)expected+="\\u0000";else if(special==31)expected+="\\u001f";
        else if(special=='"')expected+="\\\"";else if(special=='\\')expected+="\\\\";else expected+=special;
        expected+='"';EXPECT_EQ(actual,expected);
    }
    EXPECT_EQ(munmap(memory,page*2),0);
}
