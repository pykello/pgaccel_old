#include "regex_delimited_capture.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <re2/re2.h>
#include <random>
using namespace pgaccel;

TEST(RegexDelimitedCapture, StrictUtf8IsSufficientForTheRe2Suffix)
{
    re2::RE2::Options options; options.set_dot_nl(true);
    re2::RE2 all(".*", options);
    auto check = [&](std::string_view text) {
        // RE2 accepts some non-standard three/four-byte encodings. Strict
        // UTF-8 is sufficient for this fast path; other bytes must fall back.
        if (IsValidUtf8(text))
            EXPECT_TRUE(re2::RE2::FullMatch(re2::StringPiece(text.data(), text.size()), all));
    };
    for (unsigned a = 0; a < 256; ++a) {
        char single = a; check(std::string_view(&single, 1));
        EXPECT_EQ(IsValidUtf8(std::string_view(&single, 1)), a < 128);
        for (unsigned b = 0; b < 256; ++b) {
            char pair[]{char(a), char(b)}; check(std::string_view(pair, 2));
            EXPECT_EQ(IsValidUtf8(std::string_view(pair, 2)),
                (a < 128 && b < 128) || (a >= 0xc2 && a <= 0xdf && b >= 0x80 && b <= 0xbf));
        }
    }
    for (const std::string text : {"", "é", "😀", "\xed\xa0\x80", "\xe0\x80\x80",
            "\xf4\x90\x80\x80", "\xf0\x80\x80\x80", "\xf4\x8f\xbf\xbf", "\xef\xbf\xbf"}) check(text);
    for (const std::string text : {"\xed\xa0\x80", "\xe0\x80\x80", "\xf4\x90\x80\x80", "\xf0\x80\x80\x80", "\xe2\x82", "\xf0\x90\x80"})
        EXPECT_FALSE(IsValidUtf8(text));
    for (const std::string text : {"\xe0\xa0\x80", "\xed\x9f\xbf", "\xf0\x90\x80\x80", "\xf4\x8f\xbf\xbf", "\xef\xbf\xbf"})
        EXPECT_TRUE(IsValidUtf8(text));
    std::mt19937 random(612);
    for (int i = 0; i < 4000; ++i) {
        std::string text(i % 80, 'a');
        for (char &c : text) c = random() % 256;
        check(text);
    }
}

TEST(RegexDelimitedCapture, OrderedOptionalPrefixesAndUnicodeAgreeWithRe2)
{
    std::vector<std::string> values{"", "http://www./rest", "https://www.example/a/b",
        "http://example", "http://example/", "http://é/x", "http://example/é",
        "http://example/😀", "http://example/\xff", "http://example/\xed\xa0\x80",
        "http://example/\nline", std::string("http://example/\0tail", 20),
        "aaa/x", "aa/x", "a/x", "prehello#rest", "pre#rest", "pabc]rest"};
    std::mt19937 random(617);
    for (int i = 0; i < 600; ++i) {
        std::string value;
        for (int j = 0; j < i % 90; ++j) value.push_back("a:/#] "[random() % 6]);
        if (i % 3 == 0) value = "http://www.example/" + value;
        values.push_back(value);
    }
    std::vector<int64_t> ids(values.size());
    for (size_t i = 0; i < ids.size(); ++i) ids[i] = i;
    auto registry = test::Table({{test::Dict<StringType>(values), test::Raw(ids, 4)}},
        {{"s", std::make_shared<StringType>()}, {"id", std::make_shared<Int64Type>()}});
    for (const std::string pattern : {R"(^https?://(?:www\.)?([^/]+)/.*$)",
            R"(^a?([^:]+):.*$)", R"(^a?(?:aa)?([^/]+)/.*$)", R"(^([^:]+):.*$)",
            R"(^(?:pre)?([^#]+)#.*$)", R"(^p([^\]]+)\].*$)"}) {
        ASSERT_TRUE(DelimitedCapture::Compile(pattern, "\\1")) << pattern;
        re2::RE2::Options options; options.set_dot_nl(true);
        re2::RE2 regex(pattern, options); ASSERT_TRUE(regex.ok());
        Rows expected;
        for (auto value : values) { re2::RE2::Replace(&value, regex, "\\1"); expected.push_back({value}); }
        auto query = ParseSelect("SELECT REGEXP_REPLACE(s,'" + pattern + "','\\1') FROM t ORDER BY id", registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*query, true, parallel); ASSERT_TRUE(result.ok()) << result.status().Message();
            EXPECT_EQ(result->values, expected) << pattern;
        }
    }
}

TEST(RegexDelimitedCapture, UnsupportedRegexesAndLargeBranchProductsFallBack)
{
    for (const std::string pattern : {R"(^x([^/]+):.*$)", R"(^x([^/]+)/.+$)",
            R"(^x([a-z]+)/.*$)", R"(^x(?:a|b)?([^/]+)/.*$)", R"(^x\d([^/]+)/.*$)",
            R"(^a??([^/]+)/.*$)", R"(^a?b?c?d?e?([^/]+)/.*$)", R"(^é([^/]+)/.*$)"})
        EXPECT_FALSE(DelimitedCapture::Compile(pattern, "\\1")) << pattern;
    EXPECT_FALSE(DelimitedCapture::Compile(R"(^x([^/]+)/.*$)", "\\0"));
    EXPECT_FALSE(DelimitedCapture::Compile(R"(^x([^/]+)/.*$)", "prefix\\1"));
    EXPECT_TRUE(DelimitedCapture::Compile(R"(^a?b?c?d?([^/]+)/.*$)", "\\1"));
}
