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
    std::string input(2*DirectInput::ReadBytes+117,'\0');
    for(size_t i=0;i<input.size();++i)input[i]=char(i*17%251);
    {std::ofstream out(path,std::ios::binary);out.write(input.data(),input.size());}
    DirectInput in(path);
    for(size_t offset:{size_t(0),DirectInput::ReadBytes-5,input.size()-13,size_t(777)}) {
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

TEST(DirectInput, SequentialReadAheadAndEarlyDestruction) {
    char path[]="/tmp/pgaccel-prefetch-XXXXXX";
    int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);}} cleanup{path};
    std::string input(2*DirectInput::ReadBytes+117,'\0');
    for(size_t i=0;i<input.size();++i)input[i]=char(i*31%251);
    {std::ofstream out(path,std::ios::binary);out.write(input.data(),input.size());}
    {
        DirectInput in(path);
        std::string output(input.size(),'\0');
        in.read(output.data(),output.size());
        ASSERT_TRUE(in);EXPECT_EQ(output,input);
        EXPECT_EQ(in.bytesRead(),input.size());EXPECT_EQ(in.readCalls(),3);
        EXPECT_EQ(in.readCalls(),3); // Draining metrics must not count twice.
    }
    for(int i=0;i<8;++i) {
        DirectInput in(path);
        EXPECT_EQ(in.get(),int(static_cast<unsigned char>(input[0])));
        // A queued write must finish before the stream frees its buffers.
    }
}

TEST(DirectInput, HeaderReadsStaySmallAndSeekAcrossBlocks) {
    char path[]="/tmp/pgaccel-header-XXXXXX";
    int fd=mkstemp(path);ASSERT_GE(fd,0);close(fd);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove(p);}} cleanup{path};
    std::string input(3*DirectInput::HeaderBytes+17,'\0');
    for(size_t i=0;i<input.size();++i)input[i]=char(i*17%251);
    {std::ofstream out(path,std::ios::binary);out.write(input.data(),input.size());}
    DirectInput in(path,DirectInput::Mode::Header);
    char header[24];in.read(header,sizeof(header));
    ASSERT_TRUE(in);EXPECT_EQ(std::string(header,sizeof(header)),input.substr(0,sizeof(header)));
    EXPECT_EQ(in.bufferBytes(),DirectInput::HeaderBytes);
    EXPECT_EQ(in.bytesRead(),DirectInput::HeaderBytes);EXPECT_EQ(in.readCalls(),1);
    in.seekg(DirectInput::HeaderBytes-5);in.read(header,sizeof(header));
    ASSERT_TRUE(in);EXPECT_EQ(std::string(header,sizeof(header)),input.substr(DirectInput::HeaderBytes-5,sizeof(header)));
    EXPECT_EQ(in.bytesRead(),2*DirectInput::HeaderBytes);EXPECT_EQ(in.readCalls(),2);
    in.seekg(-17,std::ios::end);in.read(header,17);
    ASSERT_TRUE(in);EXPECT_EQ(std::string(header,17),input.substr(input.size()-17));
    EXPECT_EQ(in.peek(),EOF);
}
