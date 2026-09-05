#include "regex_capture_prefix.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <random>
using namespace pgaccel;

TEST(RegexCapturePrefix, EligibilityRejectsAmbiguousSyntaxAndOtherRewrites)
{
    for (const std::string pattern : {"(a).*$", "^(a)$", "^a|(b).*$", "^(a)(?-s).*$",
            "^(?i:a).*$", "^([[:alpha:]]).*$", "^(a)\\Q.*$", "^([]a]).*$", "^(a)\\.*$"})
        EXPECT_FALSE(CompileCapturePrefix(pattern, "\\1")) << pattern;
    for (const std::string rewrite : {"x", "\\0", "\\1x", "\\1\\1"})
        EXPECT_FALSE(CompileCapturePrefix("^(a).*$", rewrite));
    EXPECT_TRUE(CompileCapturePrefix("^https?://(?:www\\.)?([^/]+)/.*$", "\\1"));
}

TEST(RegexCapturePrefix, ReplacementMatchesRe2AcrossGreedinessAndByteInputs)
{
    std::vector<std::string> values{"", "a", "ab", "aaababa", "http://www.example/x/y",
        "https://example/", "http://www./a", "http://www.example", "http://é/x",
        "http://example/é", "http://example/\nnext", std::string("http://example/\xff", 16),
        std::string("http://example/\0a", 17)};
    std::mt19937 random(7381);
    for (int i = 0; i < 400; ++i) {
        std::string value;
        for (int j = 0; j < i % 150; ++j) value.push_back("ab/\n "[random() % 5]);
        if (i % 3 == 0) value = "http://www.example/" + value;
        values.push_back(value);
    }
    std::vector<int64_t> ids(values.size());
    for (size_t i = 0; i < ids.size(); ++i) ids[i] = i;
    auto registry = test::Table({{test::Dict<StringType>(values), test::Raw(ids, 4)}},
        {{"s", std::make_shared<StringType>()}, {"id", std::make_shared<Int64Type>()}});
    for (const std::string pattern : {"^https?://(?:www\\.)?([^/]+)/.*$", "^((a|ab)).*$",
            "^(a*).*$", "^(a*?).*$", "^(a)?b.*$", "^([^/]+)/.*$", "^(.*)b.*$",
            "^((?:ab|a)+).*$", "^([a-z]*).*$"}) {
        const std::string &expression = pattern;
        re2::RE2::Options options; options.set_dot_nl(true);
        re2::RE2 regex(expression, options); ASSERT_TRUE(regex.ok());
        ASSERT_TRUE(CompileCapturePrefix(expression, "\\1")) << expression;
        Rows expected;
        for (auto value : values) { re2::RE2::Replace(&value, regex, "\\1"); expected.push_back({value}); }
        auto q = ParseSelect("SELECT REGEXP_REPLACE(s,'" + expression + "','\\1') FROM t ORDER BY id", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok()) << result.status().Message();
            EXPECT_EQ(result->values, expected) << expression;
        }
    }
}

TEST(RegexCapturePrefix, LaterCapturesRetainOwnedIntermediateStrings)
{
    auto registry = test::Table({{test::Dict<StringType>({"xhello/rest", "xworld/last", "xé/trailing"}),
        test::Raw({0, 1, 2}, 4)}},
        {{"s", std::make_shared<StringType>()}, {"id", std::make_shared<Int64Type>()}});
    auto q = ParseSelect("SELECT REGEXP_REPLACE(REGEXP_REPLACE(s,'x','long-owned-prefix/'),'^([^/]+)/([^/]+).*$', '\\2') FROM t ORDER BY id", registry);
    ASSERT_TRUE(q.ok());
    for (bool parallel : {false, true}) {
        auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"hello"}, {"world"}, {"é"}}));
    }
}
