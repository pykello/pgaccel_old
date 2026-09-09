#include "direct_input.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>
using namespace pgaccel;
TEST(DirectInput, UnalignedSeeksTailsAndBounds) {
    char path[]="/tmp/pgaccel-direct-XXXXXX";
    int fd=mkstemp(path); ASSERT_GE(fd,0);close(fd);
    struct Cleanup { const char *p; ~Cleanup(){std::filesystem::remove(p);} } cleanup{path};
    std::string input(2*DirectInput::BufferBytes+117,'\0');
    for(size_t i=0;i<input.size();++i)input[i]=char(i*17%251);
    {std::ofstream out(path,std::ios::binary);out.write(input.data(),input.size());}
    DirectInput in(path);
    for(size_t offset:{size_t(0),DirectInput::BufferBytes-5,input.size()-13,size_t(777)}) {
        in.clear();in.seekg(offset);ASSERT_TRUE(in);
        std::string data(13,'\0');in.read(data.data(),data.size());
        ASSERT_TRUE(in);EXPECT_EQ(data,input.substr(offset,13));
        EXPECT_EQ(uint64_t(in.tellg()),offset+13);
    }
    EXPECT_GT(in.bytesRead(),0);EXPECT_GT(in.readCalls(),0);
    in.seekg(-4,std::ios::end);std::string tail(4,'\0');in.read(tail.data(),4);
    EXPECT_EQ(tail,input.substr(input.size()-4));EXPECT_EQ(in.peek(),EOF);
    in.clear();in.seekg(-1);EXPECT_FALSE(in);
}
TEST(DirectInput, MissingFileFailsWithoutBufferedFallback) {
    EXPECT_THROW(DirectInput("/nonexistent/pgaccel-direct-test"),std::runtime_error);
}
