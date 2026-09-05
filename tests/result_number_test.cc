#include "executor_result_number.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(ResultNumber, WideLimitsExponentsAndScaleRounding) {
    EXPECT_EQ(ResultNumber("170141183460469231731687303715884105727", 0), std::numeric_limits<Wide>::max());
    EXPECT_EQ(ResultNumber("-170141183460469231731687303715884105728", 0), std::numeric_limits<Wide>::min());
    EXPECT_EQ(ResultNumber("1.2345e+2", 2), Wide(12345));
    EXPECT_EQ(ResultNumber("-1.235", 2), Wide(-124));
    EXPECT_EQ(ResultNumber("1e-100", 12), Wide(0));
    EXPECT_EQ(ResultNumber("2.9999999999999999", 12), Wide(3000000000000LL));
    EXPECT_EQ(ResultNumber("0e100", 0), Wide(0));
    for (std::string text : {"170141183460469231731687303715884105728", "1e100"})
        EXPECT_THROW(ResultNumber(text, 0), std::overflow_error);
    for (std::string text : {"", "1e", "1e-", "1.2.3", "NULL", "e2", "+"})
        EXPECT_THROW(ResultNumber(text, 0), std::invalid_argument);
}
