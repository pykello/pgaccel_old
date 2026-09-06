#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(SubstringLike, MalformedUtf8RetainsRegexSemantics) {
    std::vector<std::string> labels{"", "special requests", "specialrequests", "requests special",
        "special special requests", "special request", "special\nrequests", "abaspecialrequests",
        std::string("special\0requests",16), std::string("special\xffrequests",16),
        "specialérequests", "érequests", "ababa", "aba", "baaba", "abababa", "a_b", "a\\b",
        std::string("\xc0\xafspecialrequests",17), std::string("specialrequests\xc2",16)};
    std::vector<std::string> values;std::vector<int64_t> keys;
    for(int i=0;i<513;++i){values.push_back(labels[i%labels.size()]);keys.push_back(i);}
    auto registry=test::Table({{test::Dict<StringType>(values),test::Raw(keys,4)}},
        {{"s",std::make_shared<StringType>()},{"k",std::make_shared<Int64Type>()}});
    for(bool avx:{false,true})for(bool anti:{false,true})
    for(const std::string pattern:{"%special%", "%%", "%é%", "%absent%"})
    for(const std::string restriction:{"", "k<5 AND "}) {
        std::string predicate="s "+std::string(anti?"NOT LIKE":"LIKE")+" '"+pattern+"'";
        auto native=ParseSelect("SELECT count(*) FROM t WHERE "+restriction+predicate,registry);
        auto reference=ParseSelect("SELECT count(*) FROM t WHERE "+restriction+"("+predicate+" OR 1=0)",registry);
        ASSERT_TRUE(native.ok());ASSERT_TRUE(reference.ok());
        auto actual=ExecuteQuery(*native,avx,true),expected=ExecuteQuery(*reference,false,false);
        ASSERT_TRUE(actual.ok());ASSERT_TRUE(expected.ok());EXPECT_EQ(actual->values,expected->values)<<predicate;
    }
}
