#include "types.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(DateTypes, CalendarValidationAndEpoch) {
    DateType date;
    EXPECT_EQ(ParseDate("1970-01-01"),0);
    EXPECT_EQ(ParseDate("1969-12-31"),-1);
    EXPECT_EQ(ToString(&date,ParseDate("2000-02-29")),"2000-02-29");
    EXPECT_THROW(ParseDate("1900-02-29"),std::invalid_argument);
    EXPECT_THROW(ParseDate("2024-13-01"),std::invalid_argument);
    EXPECT_THROW(ParseDate("2024-01-01junk"),std::invalid_argument);
}
TEST(DecimalTypes, SignedAndChecked) {
    DecimalType decimal; decimal.scale = 2;
    EXPECT_EQ(ParseDecimal(2,"-0.25"),-25);
    EXPECT_EQ(ToString(&decimal,-25),"-0.25");
    EXPECT_EQ(ParseDecimal(0,"-9223372036854775808"),INT64_MIN);
    EXPECT_THROW(ParseDecimal(2,"1.001"),std::invalid_argument);
    EXPECT_THROW(ParseDecimal(2,"9223372036854775807"),std::out_of_range);
}
