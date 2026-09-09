#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(NativeValidation, EmptyTableAndMalformedInput) {
    auto t=test::Table({},{{"v",std::make_shared<Int64Type>()}});
    for(auto sql:{"SELECT count(*) FROM t","SELECT count(distinct v) FROM t"}) {
        auto q=ParseSelect(sql,t);ASSERT_TRUE(q.ok());auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"0"}}));
    }
    auto q=ParseSelect("SELECT min(v),sum(v),avg(v) FROM t",t);ASSERT_TRUE(q.ok());
    auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"NULL","NULL","NULL"}}));
    for(auto metadata:{"", "-1\n", "1\n0 -1 v 2\n", "1\n0 1 v 999\n", "1\n0 1 v 2\n"}) {
        std::stringstream meta(metadata),data;EXPECT_FALSE(ColumnarTable::Load("t",meta,data).ok());
    }
}
TEST(NativeValidation, RejectCorruptDictionaryCodes) {
    auto d=test::Dict<StringType>({"a","b"});
    static_cast<DictColumnDataBase*>(d.get())->values[0]=255;
    EXPECT_THROW(test::Table({{d}},{{"s",std::make_shared<StringType>()}}),std::runtime_error);
}
TEST(DateTypes, TimestampUTCAndNativeRoundTrip) {
    TimestampType timestamp;
    EXPECT_EQ(ParseTimestamp("1969-12-31 23:59:59"),-1);
    EXPECT_EQ(ToString(&timestamp,-1),"1969-12-31 23:59:59");
    EXPECT_THROW(ParseTimestamp("2020-01-01 24:00:00"),std::invalid_argument);
    auto t=test::Table({{test::Raw({0,86400})}},{{"ts",std::make_shared<TimestampType>()}});
    auto q=ParseSelect("SELECT min(ts),max(ts) FROM t WHERE ts >= '1970-01-01 00:00:00'",t);ASSERT_TRUE(q.ok());
    auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"1970-01-01 00:00:00","1970-01-02 00:00:00"}}));
}
TEST(NativeValidation, VersionAndPairIdentity) {
    auto t=test::Table({{test::Raw({7,9})}},{{"v",std::make_shared<Int64Type>()}});
    std::stringstream data1,meta1,data2,meta2;
    ASSERT_TRUE(t.at("t")->Save(meta1,data1).ok());
    ASSERT_TRUE(t.at("t")->Save(meta2,data2).ok());
    EXPECT_EQ(meta1.str().find("PGACCEL_SNAPSHOT 1 "),0);
    data1.seekg(0);meta1.seekg(0);
    EXPECT_TRUE(ColumnarTable::Load("t",meta1,data1).ok());
    data1.clear();data1.seekg(0);meta2.seekg(0);
    EXPECT_FALSE(ColumnarTable::Load("t",meta2,data1).ok());
    auto bad=meta1.str();bad.replace(bad.find(" 1 "),3," 99 ");
    std::stringstream unsupported(bad);data1.clear();data1.seekg(0);
    EXPECT_FALSE(ColumnarTable::Load("t",unsupported,data1).ok());
}
TEST(NativeValidation, RawBoundsAllWidthsAndVectorTails) {
    Int64Type logical;
    for (int width : {1,2,4,8}) for (int count : {0,1,63,64,65,65535,65536}) {
        RawColumnData<Int64Type> column;
        column.type = ColumnDataBase::RAW_COLUMN_DATA;
        column.size = count; column.bytesPerValue = width;
        column.values = static_cast<uint8_t *>(AllocateAligned(512, count*width));
        int64_t low = width == 8 ? INT64_MIN : -(int64_t(1) << (width*8-1));
        int64_t high = width == 8 ? INT64_MAX : (int64_t(1) << (width*8-1))-1;
        column.minValue = count ? low : 0;
        column.maxValue = count > 1 ? high : column.minValue;
        for (int i=0; i<count; ++i) {
            int64_t value = i ? high : low;
            switch (width) {
            case 1: reinterpret_cast<int8_t *>(column.values)[i] = value; break;
            case 2: reinterpret_cast<int16_t *>(column.values)[i] = value; break;
            case 4: reinterpret_cast<int32_t *>(column.values)[i] = value; break;
            case 8: reinterpret_cast<int64_t *>(column.values)[i] = value; break;
            }
        }
        auto load = [&] {
            std::stringstream stream;
            EXPECT_TRUE(column.Save(stream).ok());
            return ColumnDataBase::Load(stream, &logical);
        };
        ASSERT_TRUE(load().ok()) << width << ':' << count;
        if (count > 1) {
            --column.maxValue; EXPECT_FALSE(load().ok()); ++column.maxValue;
            ++column.minValue; EXPECT_FALSE(load().ok()); --column.minValue;
        }
        if (count == 1) { ++column.maxValue; EXPECT_FALSE(load().ok()); }
    }
}

TEST(NativeValidation, DictionaryCoverageAtCodeWidthBoundaries) {
    DateType logical;
    for(int count:{0,1,2,255,256,65536}) {
        std::vector<DateType::c_type> labels;
        for(int i=0;i<count;++i)labels.push_back(i);
        auto input=test::Dict<DateType>(labels);
        auto *dictionary=static_cast<DictColumnData<DateType>*>(input.get());
        auto load=[&] {
            std::stringstream stream;EXPECT_TRUE(dictionary->Save(stream).ok());
            return ColumnDataBase::Load(stream,&logical);
        };
        ASSERT_TRUE(load().ok());
        if(count>1) {
            if(dictionary->bytesPerValue()==1)dictionary->values[count-1]=0;
            else reinterpret_cast<uint16_t*>(dictionary->values)[count-1]=0;
            EXPECT_FALSE(load().ok()); // A declared entry has no code.
        }
        if(count>0 && count<65536) {
            if(dictionary->bytesPerValue()==1)dictionary->values[0]=count;
            else reinterpret_cast<uint16_t*>(dictionary->values)[0]=count;
            EXPECT_FALSE(load().ok()); // First invalid representable code.
        }
    }
}

TEST(NativeValidation, ReusedStringSlotsStillValidateDictionary) {
    StringType logical;
    auto input=test::Dict<StringType>({std::string(100,'a'),std::string(100,'b')});
    auto *dictionary=static_cast<DictColumnData<StringType>*>(input.get());
    std::vector<std::string> scratch{std::string(200,'z'),std::string(200,'y')};
    auto load=[&] {
        std::stringstream stream;EXPECT_TRUE(dictionary->Save(stream).ok());
        return ColumnDataBase::LoadReusingStrings(stream,&logical,scratch);
    };
    auto valid=load();ASSERT_TRUE(valid.ok());
    auto *loaded=static_cast<DictColumnData<StringType>*>((*valid).get());
    EXPECT_EQ(loaded->dict,dictionary->dict);
    loaded->dict.swap(scratch);
    dictionary->dict[1]=dictionary->dict[0];
    EXPECT_FALSE(load().ok());
    dictionary->dict[1]=std::string(100,'b');
    dictionary->values[1]=0;EXPECT_FALSE(load().ok());
    dictionary->values[1]=2;EXPECT_FALSE(load().ok());
}

TEST(NativeValidation, DictionaryStringReadErrorsPreserveStreamState) {
    StringType logical;
    auto column=test::Dict<StringType>({"abcdefgh"});
    std::stringstream saved;ASSERT_TRUE(column->Save(saved).ok());
    auto bytes=saved.str();
    for(bool exceptions:{false,true}) {
        std::istringstream truncated(bytes.substr(0,12));
        if(exceptions)truncated.exceptions(std::ios::failbit|std::ios::badbit);
        EXPECT_ANY_THROW(ColumnDataBase::Load(truncated,&logical));
        EXPECT_TRUE(truncated.fail());EXPECT_TRUE(truncated.eof());
        struct BrokenBuffer:std::stringbuf {
            explicit BrokenBuffer(const std::string &bytes):std::stringbuf(bytes){}
            std::streamsize xsgetn(char *out,std::streamsize n) override {
                if(gptr()-eback()>=12)throw std::runtime_error("injected read failure");
                return std::stringbuf::xsgetn(out,n);
            }
        } buffer(bytes);
        std::istream broken(&buffer);
        if(exceptions)broken.exceptions(std::ios::badbit);
        EXPECT_ANY_THROW(ColumnDataBase::Load(broken,&logical));
        EXPECT_TRUE(broken.bad());
    }
}
