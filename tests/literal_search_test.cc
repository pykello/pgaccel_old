#include "../src/executor_literal_search.hpp"
#include <gtest/gtest.h>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
using namespace pgaccel;
TEST(LiteralSearch, BinaryInputsAlignmentsTailsAndEarliestMatches) {
    uint32_t random=0x128fe7;
    for(size_t length:{0,1,2,3,7,16,32,33,70})
    for(size_t size:{0,1,15,31,63,64,65,79,127,128,129,259}) {
        std::string needle(length,'a');for(char &c:needle){random=random*1664525+1013904223;c=char(random>>24);}
        for(size_t alignment:{0,1,17,63}) {
            std::string storage(size+alignment,'a');
            for(char &c:storage){random=random*1664525+1013904223;c=char(random>>24);}
            if(length<=size && length) {
                storage.replace(alignment+size-length,length,needle);
                if(size>length+63)storage.replace(alignment+63,length,needle);
            }
            std::string_view text(storage.data()+alignment,size);
            for(size_t position:{size_t(0),size_t(1),size/2,size,size+1})
            for(bool avx:{false,true})EXPECT_EQ(FindLiteralBytes(text,needle,position,avx),text.find(needle,position))
                <<"length="<<length<<" size="<<size<<" position="<<position;
        }
    }
    for(const std::string text:{"abababa","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
    for(const std::string needle:{"aba","aaa","aab","ba"})
    for(size_t position=0;position<=text.size()+1;++position)
        EXPECT_EQ(FindLiteralBytes(text,needle,position,true),std::string_view(text).find(needle,position));
}
TEST(LiteralSearch, MaskedLoadsDoNotCrossProtectedPage) {
    size_t page=sysconf(_SC_PAGESIZE);
    auto region=static_cast<char *>(mmap(nullptr,page*2,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0));
    ASSERT_NE(region,MAP_FAILED);ASSERT_EQ(mprotect(region+page,page,PROT_NONE),0);
    for(size_t size:{1,15,31,63,64,65,79,127,129}) {
        auto begin=region+page-size;std::memset(begin,'a',size);
        for(const std::string needle:{"aa","aaa","aab","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"}) {
            std::string_view text(begin,size);
            for(size_t position:{size_t(0),size/2,size})
                EXPECT_EQ(FindLiteralBytes(text,needle,position,true),text.find(needle,position));
        }
    }
    EXPECT_EQ(munmap(region,page*2),0);
}
